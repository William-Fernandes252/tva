{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT, ask, asks, runReaderT)
import Data.Aeson qualified as Aeson
import Data.ByteString.Char8 qualified as B8
import Data.ByteString.Lazy qualified as BL
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Data.UUID (toText)
import Data.UUID.V4 (nextRandom)
import Domain.Core (EntityId (..), Resolution (..), Video, VideoJob (..), Worker, finishJob, resolutionToTag)
import Domain.Database
  ( AnyVideoJob (MkAnyVideoJob),
    MonadDatabase (..),
    findVideoJobById',
    insertPendingJob',
    updateJobToCompleted',
    updateJobToPending',
    updateJobToProcessing',
  )
import Domain.Event (SystemEvent (..))
import Domain.Queue (MonadQueue (..))
import Hasql.Connection qualified as Hasql (Connection, acquire, settings)
import MinIO (MinioConfig (..), downloadObject, initMinIO, uploadObject)
import Network.AMQP
import Network.AMQP.Types (FieldTable (FieldTable), FieldValue (FVString))
import System.Directory (listDirectory, removeDirectoryRecursive)
import System.Environment (lookupEnv)
import System.FilePath (takeExtension, (</>))
import System.IO.Temp (createTempDirectory)
import System.Process.Typed (proc, runProcess_)

-- | Configuration for the video worker, bundling all infrastructure handles.
data WorkerConfig = WorkerConfig
  { wDbConn :: Hasql.Connection,
    wRabbitConn :: Connection,
    wRabbitChan :: Channel,
    wWorkerId :: EntityId Worker,
    wMinio :: MinioConfig,
    wRetry :: RetryConfig
  }

-- | Retry and backoff settings, configurable via environment variables.
data RetryConfig = RetryConfig
  { retryMaxAttempts :: Int,
    retryBaseDelaySeconds :: Int,
    retryMaxDelaySeconds :: Int
  }

-- | The concrete monad stack for the video worker.
newtype WorkerM a = WorkerM {runWorkerM :: ReaderT WorkerConfig IO a}
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader WorkerConfig)

retryHeaderName :: Text
retryHeaderName = "x-retry-count"

-- | Compute exponential backoff delay in microseconds.
backoffDelay :: RetryConfig -> Int -> Int
backoffDelay cfg retryCount =
  let seconds = min (2 ^ retryCount * retryBaseDelaySeconds cfg) (retryMaxDelaySeconds cfg)
   in seconds * 1_000_000

-- | Parse the retry count from a message's headers.
getRetryCount :: Message -> Int
getRetryCount msg = fromMaybe 0 $ do
  FieldTable headers <- msgHeaders msg
  FVString val <- Map.lookup retryHeaderName headers
  readMaybe (B8.unpack val)

-- | Set the retry count header on a message.
setRetryHeader :: Int -> Message -> Message
setRetryHeader n msg =
  msg {msgHeaders = Just (FieldTable newHeaders)}
  where
    FieldTable existing = fromMaybe (FieldTable Map.empty) (msgHeaders msg)
    cleaned = Map.delete retryHeaderName existing
    newHeaders = Map.insert retryHeaderName (FVString (B8.pack (show n))) cleaned

dlxName :: Text
dlxName = "video_dlx"

dlqName :: Text
dlqName = "video_dlq"

-- | Declare the dead-letter exchange and queue, binding them together.
setupDeadLetterInfrastructure :: Channel -> IO ()
setupDeadLetterInfrastructure chan = do
  declareExchange
    chan
    newExchange
      { exchangeName = dlxName,
        exchangeType = "topic",
        exchangeDurable = True
      }

  _ <-
    declareQueue
      chan
      newQueue
        { queueName = dlqName,
          queueDurable = True,
          queueExclusive = False,
          queueAutoDelete = False
        }

  bindQueue chan dlqName dlxName "#"

  putStrLn "Dead-letter infrastructure initialized."
  putStrLn "  DLX: video_dlx"
  putStrLn "  DLQ: video_dlq"

-- | Route a message to the dead-letter queue (after max retries).
sendToDLQ :: Channel -> Message -> IO ()
sendToDLQ chan msg = do
  _ <- publishMsg chan dlxName "#" msg
  putStrLn "[WARN] Message sent to dead-letter queue (max retries exhausted)."

-- | Action to set up the queue and start consuming. Uses manual ack/nack
--   with exponential backoff and a dead-letter queue for poison messages.
setupConsumer :: WorkerConfig -> (SystemEvent -> WorkerM (Either Text ())) -> IO ()
setupConsumer cfg handler = do
  let chan = wRabbitChan cfg
      retryCfg = wRetry cfg
      maxRetries = retryMaxAttempts retryCfg

  setupDeadLetterInfrastructure chan

  -- Declare main queue
  _ <-
    declareQueue
      chan
      newQueue
        { queueName = "video_upload_queue",
          queueDurable = True,
          queueExclusive = False,
          queueAutoDelete = False
        }

  bindQueue chan "video_upload_queue" "video_exchange" "video.uploaded"

  putStrLn "Video worker: waiting for VideoUploadedEvent messages..."
  putStrLn "  queue: video_upload_queue"
  putStrLn "  exchange: video_exchange"
  putStrLn "  routing key: video.uploaded"

  -- Manual ack mode: we decide when to ack/nack each message
  _consumerTag <- consumeMsgs chan "video_upload_queue" Ack $ \(msg, env) -> do
    case Aeson.decode (msgBody msg) of
      Nothing -> do
        putStrLn "[WARN] Could not decode message body. Sending to DLQ."
        sendToDLQ chan msg
        ackEnv env
      Just event -> do
        let retryCount = getRetryCount msg
        result <- runReaderT (runWorkerM (handler event)) cfg
        case result of
          Right () -> do
            putStrLn "[INFO] Message processed successfully. Acking."
            ackEnv env
          Left errMsg -> do
            putStrLn $ "[WARN] Processing failed (retry " <> show retryCount <> "/" <> show maxRetries <> "): " <> T.unpack errMsg
            if retryCount < maxRetries
              then do
                let delay = backoffDelay retryCfg retryCount
                    delaySecs = delay `div` 1_000_000
                putStrLn $ "[INFO] Retrying in " <> show delaySecs <> "s (exponential backoff)..."
                threadDelay delay
                -- Publish retry message with incremented count
                let retryMsg =
                      (setRetryHeader (retryCount + 1) msg)
                        { msgDeliveryMode = Just Persistent
                        }
                _ <- publishMsg chan "video_exchange" "video.uploaded" retryMsg
                ackEnv env
              else do
                putStrLn "[ERROR] Max retries exhausted. Routing to DLQ."
                sendToDLQ chan msg
                ackEnv env
  return ()

instance MonadQueue SystemEvent WorkerM where
  publish :: SystemEvent -> WorkerM ()
  publish event = do
    chan <- asks wRabbitChan
    let payload = Aeson.encode event
        rKey = case event of
          VideoUploadedEvent _ _ -> "video.uploaded"
          TranscodeFinishedEvent _ _ -> "video.finished"
          TranscodeFailedEvent _ _ -> "video.failed"
        message =
          newMsg
            { msgBody = payload,
              msgDeliveryMode = Just Persistent
            }
    liftIO $ do
      _ <- publishMsg chan "video_exchange" rKey message
      return ()

  consume :: (SystemEvent -> WorkerM ()) -> WorkerM ()
  consume handler = do
    cfg <- ask
    let handler' e = Right () <$ handler e
    liftIO $ setupConsumer cfg handler'

instance MonadDatabase WorkerM where
  insertPendingJob vid source = do
    conn <- asks wDbConn
    liftIO $ Domain.Database.insertPendingJob' conn vid source

  findVideoJobById vid = do
    conn <- asks wDbConn
    liftIO $ Domain.Database.findVideoJobById' conn vid

  updateJobToProcessing vid worker = do
    conn <- asks wDbConn
    liftIO $ Domain.Database.updateJobToProcessing' conn vid worker

  updateJobToPending vid = do
    conn <- asks wDbConn
    liftIO $ Domain.Database.updateJobToPending' conn vid

  updateJobToCompleted vid chunks = do
    conn <- asks wDbConn
    liftIO $ Domain.Database.updateJobToCompleted' conn vid chunks

-- | Handle an incoming system event.
--   Returns 'Right ()' on success (message should be acked).
--   Returns 'Left errorMsg' on transient failure (consumer will retry/DLQ).
handleEvent :: SystemEvent -> WorkerM (Either Text ())
handleEvent (VideoUploadedEvent vid res) = do
  workerId <- asks wWorkerId
  liftIO $
    putStrLn $
      "[INFO] Received VideoUploadedEvent { videoId: "
        <> T.unpack (toText' vid)
        <> ", resolution: "
        <> T.unpack (resolutionToTag res)
        <> " }"

  mJob <- findVideoJobById vid
  case mJob of
    Just (MkAnyVideoJob (QueuedJob _ sourceKey)) -> do
      liftIO $ putStrLn "[INFO] Found Pending job. Transitioning to Processing..."
      updateJobToProcessing vid workerId
      liftIO $ putStrLn "[INFO] Starting transcode..."
      result <- transcodeVideo vid res sourceKey
      case result of
        Right outputPaths -> do
          liftIO $ putStrLn $ "[INFO] Transcode succeeded. Output paths: " ++ show outputPaths
          updateJobToCompleted vid outputPaths
          let validated = finishJob outputPaths (RunningJob vid workerId 0)
          liftIO $ putStrLn $ "[INFO] Validated FinishedJob: " ++ show validated
          publish $ TranscodeFinishedEvent vid outputPaths
          return $ Right ()
        Left err -> do
          liftIO $ putStrLn $ "[ERROR] Transcode failed: " ++ T.unpack err
          updateJobToPending vid
          return $ Left err
    Just (MkAnyVideoJob _) -> do
      liftIO $ putStrLn "[INFO] Job already in Processing/Completed state. Idempotent — acking."
      return $ Right ()
    Nothing -> do
      liftIO $ putStrLn "[WARN] No job found for this video ID. Acking (skip)."
      return $ Right ()
handleEvent (TranscodeFinishedEvent _vid _chunks) = do
  liftIO $ putStrLn "[INFO] Received TranscodeFinishedEvent (not yet handled)."
  return $ Right ()
handleEvent (TranscodeFailedEvent _vid _err) = do
  liftIO $ putStrLn "[INFO] Received TranscodeFailedEvent (not yet handled)."
  return $ Right ()

-- | Map a resolution to an FFmpeg scale filter.
resolutionToScale :: Resolution -> Text
resolutionToScale R1080p = "scale=1920:1080"
resolutionToScale R720p = "scale=1280:720"
resolutionToScale R480p = "scale=854:480"

-- | Transcode a video: download from MinIO, run FFmpeg to produce HLS chunks,
--   upload segments organized by resolution, return output paths.
transcodeVideo ::
  EntityId Video ->
  Resolution ->
  Text ->
  WorkerM (Either Text [Text])
transcodeVideo vid res sourceKey = do
  minioCfg <- asks wMinio
  let tag = resolutionToTag res
      uuidText = toText' vid
      -- Output directory: "{uuid}/{resolution}/"
      outputDir = uuidText <> "/" <> tag
      scale = resolutionToScale res
      inputBucket = minioBucket minioCfg -- "raw-videos"
      outputBucket = minioOutputBucket minioCfg -- "processed-videos"
  result <- liftIO $ try @SomeException $ do
    tmpDir <- createTempDirectory "/tmp" "tva-transcode"

    -- Download source video from raw-videos bucket
    let inputFile = tmpDir </> "input.mkv"
    videoBytes <- downloadObject minioCfg inputBucket (encodeUtf8 sourceKey)
    BL.writeFile inputFile videoBytes
    putStrLn $ "[INFO] Downloaded source video to " <> inputFile

    -- FFmpeg: transcode to HLS with proper chunking
    --   -force_key_frames ensures clean segment boundaries every 2 seconds
    --   -hls_time 6: 6-second segments
    --   -hls_segment_type mpegts for broad compatibility
    let segmentPattern = tmpDir </> "segment_%03d.ts"
        outputPlaylist = tmpDir </> "output.m3u8"
    runProcess_ $
      proc
        "ffmpeg"
        [ "-i",
          inputFile,
          "-c:v",
          "libx264",
          "-preset",
          "fast",
          "-crf",
          "23",
          "-force_key_frames",
          "expr:gte(t,n_forced*2)",
          "-c:a",
          "aac",
          "-b:a",
          "128k",
          "-vf",
          T.unpack scale,
          "-f",
          "hls",
          "-hls_time",
          "6",
          "-hls_list_size",
          "0",
          "-hls_segment_type",
          "mpegts",
          "-hls_segment_filename",
          segmentPattern,
          outputPlaylist
        ]
    putStrLn "[INFO] FFmpeg HLS transcode complete."

    -- Collect and upload all HLS output files to processed-videos bucket
    files <- listDirectory tmpDir
    let outputFiles = filter (\f -> takeExtension f `elem` [".m3u8", ".ts"]) files
    mapM_
      ( \f -> do
          let localPath = tmpDir </> f
              objectKey = encodeUtf8 outputDir <> "/" <> encodeUtf8 (T.pack f)
          uploadObject minioCfg outputBucket objectKey localPath
          putStrLn $ "[INFO] Uploaded " <> f <> " to MinIO (" <> T.unpack outputDir <> ")"
      )
      outputFiles

    -- Build the list of output paths for the 'FinishedJob' constructor
    let outputPaths = map (\f -> outputDir <> "/" <> T.pack f) outputFiles

    removeDirectoryRecursive tmpDir
    return outputPaths

  case result of
    Left (e :: SomeException) ->
      return $ Left $ T.pack (show e)
    Right paths ->
      return $ Right paths

-- | Extract the UUID text from an EntityId.
toText' :: EntityId a -> Text
toText' (EntityId u) = toText u

main :: IO ()
main = do
  putStrLn "=== TVA Video Worker ==="

  -- Initialize PostgreSQL connection pool.
  pgHost <- fromMaybe "127.0.0.1" <$> lookupEnv "PG_HOST"
  mPgPort <- lookupEnv "PG_PORT"
  let pgPort = fromMaybe 5432 (mPgPort >>= readMaybe)
  pgUser <- fromMaybe "video_user" <$> lookupEnv "PG_USER"
  pgPass <- fromMaybe "video_password" <$> lookupEnv "PG_PASS"
  pgDb <- fromMaybe "video_db" <$> lookupEnv "PG_DB"
  let connSettings =
        Hasql.settings
          (fromString pgHost)
          (fromIntegral pgPort)
          (fromString pgUser)
          (fromString pgPass)
          (fromString pgDb)
  result <- Hasql.acquire connSettings
  dbConn <- case result of
    Left err -> error $ "Failed to connect to PostgreSQL: " ++ show err
    Right c -> return c
  putStrLn $ "Connected to PostgreSQL at " ++ pgHost ++ ":" ++ show (pgPort :: Int)

  -- Initialize RabbitMQ connection.
  mqHost <- fromMaybe "127.0.0.1" <$> lookupEnv "RABBITMQ_HOST"
  mqUser <- fromMaybe "mq_user" <$> lookupEnv "RABBITMQ_USER"
  mqPass <- fromMaybe "mq_password" <$> lookupEnv "RABBITMQ_PASS"
  mqVhost <- fromMaybe "/" <$> lookupEnv "RABBITMQ_VHOST"
  conn <- openConnection mqHost (T.pack mqVhost) (T.pack mqUser) (T.pack mqPass)
  chan <- openChannel conn

  -- Declare the topic exchange (idempotent — fine if api-server already did it).
  declareExchange
    chan
    newExchange
      { exchangeName = "video_exchange",
        exchangeType = "topic",
        exchangeDurable = True
      }
  putStrLn $ "Connected to RabbitMQ at " ++ mqHost

  -- Generate a random worker ID.
  workerUuid <- nextRandom
  let workerId = EntityId workerUuid :: EntityId Worker
  putStrLn $ "Worker ID: " <> show workerUuid

  -- Initialize MinIO connection.
  putStrLn "Initializing MinIO connection..."
  minioCfg <- initMinIO
  putStrLn "MinIO connection initialized."

  -- Initialize retry / backoff configuration from environment.
  mMaxAttempts <- lookupEnv "RETRY_MAX_ATTEMPTS"
  let retryMaxAttempts = fromMaybe 3 (mMaxAttempts >>= readMaybe)
  mBaseDelay <- lookupEnv "RETRY_BASE_DELAY_SECONDS"
  let retryBaseDelaySeconds = fromMaybe 5 (mBaseDelay >>= readMaybe)
  mMaxDelay <- lookupEnv "RETRY_MAX_DELAY_SECONDS"
  let retryMaxDelaySeconds = fromMaybe 60 (mMaxDelay >>= readMaybe)
  let retryCfg = RetryConfig {retryMaxAttempts, retryBaseDelaySeconds, retryMaxDelaySeconds}
  putStrLn $
    "Retry config: maxAttempts="
      <> show retryMaxAttempts
      <> " baseDelay="
      <> show retryBaseDelaySeconds
      <> "s"
      <> " maxDelay="
      <> show retryMaxDelaySeconds
      <> "s"

  let cfg =
        WorkerConfig
          { wDbConn = dbConn,
            wRabbitConn = conn,
            wRabbitChan = chan,
            wWorkerId = workerId,
            wMinio = minioCfg,
            wRetry = retryCfg
          }

  -- Run the consumer loop (blocks until interrupted).
  setupConsumer cfg handleEvent

-- | Parse an integer from a string, returning Nothing on failure.
readMaybe :: (Read a) => String -> Maybe a
readMaybe s = case reads s of
  [(x, "")] -> Just x
  _ -> Nothing
