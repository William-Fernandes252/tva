{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT, ask, asks, local, runReaderT)
import Data.Aeson qualified as Aeson
import Data.ByteString.Char8 qualified as B8
import Data.ByteString.Lazy qualified as BL
import Data.Int (Int32)
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
import Domain.Logger
import Domain.Event (SystemEvent (..))
import Domain.Queue (MonadQueue (..))
import Hasql.Connection qualified as Hasql (Connection, acquire, settings)
import MinIO (MinioConfig (..), downloadObject, initMinIO, uploadObject)
import Network.AMQP
import Network.AMQP.Types (FieldTable (FieldTable), FieldValue (FVString))
import System.Directory (listDirectory, removeDirectoryRecursive)
import System.Environment (lookupEnv)
import System.FilePath (takeExtension, (</>))
import System.IO (hGetLine, hIsEOF)
import System.IO.Temp (createTempDirectory)
import System.Process.Typed (proc, runProcess_, setStdout, createPipe, withProcessWait, readProcessStdout_, getStdout, checkExitCode)

-- | Configuration for the video worker, bundling all infrastructure handles.
data WorkerConfig = WorkerConfig
  { wDbConn :: Hasql.Connection,
    wRabbitConn :: Connection,
    wRabbitChan :: Channel,
    wWorkerId :: EntityId Worker,
    wMinio :: MinioConfig,
    wRetry :: RetryConfig,
    wLogEnv :: LogEnv,
    wKatipContext :: LogContexts,
    wKatipNamespace :: Namespace
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

instance Katip WorkerM where
  getLogEnv = asks wLogEnv
  localLogEnv f (WorkerM m) = WorkerM (local (\c -> c {wLogEnv = f (wLogEnv c)}) m)

instance KatipContext WorkerM where
  getKatipContext = asks wKatipContext
  localKatipContext f (WorkerM m) = WorkerM (local (\c -> c {wKatipContext = f (wKatipContext c)}) m)
  getKatipNamespace = asks wKatipNamespace
  localKatipNamespace f (WorkerM m) = WorkerM (local (\c -> c {wKatipNamespace = f (wKatipNamespace c)}) m)

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
setupDeadLetterInfrastructure :: WorkerConfig -> IO ()
setupDeadLetterInfrastructure cfg = runKatipContextT (wLogEnv cfg) (mempty :: LogContexts) "video-worker" $ do
  let chan = wRabbitChan cfg
  liftIO $ declareExchange
    chan
    newExchange
      { exchangeName = dlxName,
        exchangeType = "topic",
        exchangeDurable = True
      }

  _ <- liftIO $ 
    declareQueue
      chan
      newQueue
        { queueName = dlqName,
          queueDurable = True,
          queueExclusive = False,
          queueAutoDelete = False
        }

  liftIO $ bindQueue chan dlqName dlxName "#"

  $(logTM) InfoS "Dead-letter infrastructure initialized. DLX: video_dlx, DLQ: video_dlq"

-- | Route a message to the dead-letter queue (after max retries).
sendToDLQ :: WorkerConfig -> Message -> IO ()
sendToDLQ cfg msg = runKatipContextT (wLogEnv cfg) (mempty :: LogContexts) "video-worker" $ do
  let chan = wRabbitChan cfg
  _ <- liftIO $ publishMsg chan dlxName "#" msg
  $(logTM) WarningS "Message sent to dead-letter queue (max retries exhausted)."

-- | Action to set up the queue and start consuming. Uses manual ack/nack
--   with exponential backoff and a dead-letter queue for poison messages.
setupConsumer :: WorkerConfig -> (SystemEvent -> WorkerM (Either Text ())) -> IO ()
setupConsumer cfg handler = runKatipContextT (wLogEnv cfg) (mempty :: LogContexts) "video-worker" $ do
  let chan = wRabbitChan cfg
      retryCfg = wRetry cfg
      maxRetries = retryMaxAttempts retryCfg

  liftIO $ setupDeadLetterInfrastructure cfg

  -- Declare main queue
  _ <- liftIO $ 
    declareQueue
      chan
      newQueue
        { queueName = "video_upload_queue",
          queueDurable = True,
          queueExclusive = False,
          queueAutoDelete = False
        }

  liftIO $ bindQueue chan "video_upload_queue" "video_exchange" "video.uploaded"

  $(logTM) InfoS "Video worker: waiting for VideoUploadedEvent messages on video_upload_queue"

  -- Manual ack mode: we decide when to ack/nack each message
  _consumerTag <- liftIO $ consumeMsgs chan "video_upload_queue" Ack $ \(msg, env) -> runKatipContextT (wLogEnv cfg) (mempty :: LogContexts) "video-worker" $ do
    case Aeson.decode (msgBody msg) of
      Nothing -> do
        $(logTM) WarningS "Could not decode message body. Sending to DLQ."
        liftIO $ sendToDLQ cfg msg
        liftIO $ ackEnv env
      Just event -> do
        let retryCount = getRetryCount msg
        result <- liftIO $ runReaderT (runWorkerM (handler event)) cfg
        case result of
          Right () -> do
            $(logTM) InfoS "Message processed successfully. Acking."
            liftIO $ ackEnv env
          Left errMsg -> katipAddContext (sl "retryCount" retryCount <> sl "maxRetries" maxRetries <> sl "error" errMsg) $ do
            $(logTM) WarningS "Processing failed"
            if retryCount < maxRetries
              then do
                let delay = backoffDelay retryCfg retryCount
                    delaySecs = delay `div` 1_000_000
                $(logTM) InfoS "Retrying (exponential backoff)"
                liftIO $ threadDelay delay
                -- Publish retry message with incremented count
                let retryMsg =
                      (setRetryHeader (retryCount + 1) msg)
                        { msgDeliveryMode = Just Persistent
                        }
                _ <- liftIO $ publishMsg chan "video_exchange" "video.uploaded" retryMsg
                liftIO $ ackEnv env
              else do
                $(logTM) ErrorS "Max retries exhausted. Routing to DLQ."
                liftIO $ sendToDLQ cfg msg
                case event of
                  VideoUploadedEvent vid _ -> liftIO $ runReaderT (runWorkerM (updateJobToFailed vid "Max retries exhausted")) cfg
                  _ -> return ()
                liftIO $ ackEnv env
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

  updateJobToFailed vid err = do
    conn <- asks wDbConn
    liftIO $ Domain.Database.updateJobToFailed' conn vid err

  updateJobProgress vid prog = do
    conn <- asks wDbConn
    liftIO $ Domain.Database.updateJobProgress' conn vid prog

-- | Handle an incoming system event.
--   Returns 'Right ()' on success (message should be acked).
--   Returns 'Left errorMsg' on transient failure (consumer will retry/DLQ).
handleEvent :: SystemEvent -> WorkerM (Either Text ())
handleEvent (VideoUploadedEvent vid res) = katipAddContext (sl "videoId" (show vid) <> sl "resolution" (resolutionToTag res)) $ do
  workerId <- asks wWorkerId
  $(logTM) InfoS "Received VideoUploadedEvent"

  mJob <- findVideoJobById vid
  case mJob of
    Just (MkAnyVideoJob (QueuedJob _ sourceKey)) -> do
      $(logTM) InfoS "Found Pending job. Transitioning to Processing..."
      updateJobToProcessing vid workerId
      $(logTM) InfoS "Starting transcode..."
      result <- transcodeVideo vid res sourceKey
      case result of
        Right outputPaths -> katipAddContext (sl "outputPaths" outputPaths) $ do
          $(logTM) InfoS "Transcode succeeded"
          updateJobToCompleted vid outputPaths
          let validated = finishJob outputPaths (RunningJob vid workerId 0)
          katipAddContext (sl "validatedJob" (show validated)) $ $(logTM) InfoS "Validated FinishedJob"
          publish $ TranscodeFinishedEvent vid outputPaths
          return $ Right ()
        Left err -> katipAddContext (sl "error" err) $ do
          $(logTM) ErrorS "Transcode failed"
          updateJobToPending vid
          return $ Left err
    Just (MkAnyVideoJob _) -> do
      $(logTM) InfoS "Job already in Processing/Completed state. Idempotent — acking."
      return $ Right ()
    Nothing -> do
      $(logTM) WarningS "No job found for this video ID. Acking (skip)."
      return $ Right ()
handleEvent (TranscodeFinishedEvent _vid _chunks) = do
  $(logTM) InfoS "Received TranscodeFinishedEvent (not yet handled)."
  return $ Right ()
handleEvent (TranscodeFailedEvent _vid _err) = do
  $(logTM) InfoS "Received TranscodeFailedEvent (not yet handled)."
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
  cfg <- ask
  let dbConn = wDbConn cfg
  result <- liftIO $ try @SomeException $ do
    tmpDir <- createTempDirectory "/tmp" "tva-transcode"

    -- Download source video from raw-videos bucket
    let inputFile = tmpDir </> "input.mkv"
    videoBytes <- downloadObject minioCfg inputBucket (encodeUtf8 sourceKey)
    BL.writeFile inputFile videoBytes

    -- Get the exact duration in seconds using ffprobe
    let probeCmd = proc "ffprobe" ["-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", inputFile]
    probeOut <- readProcessStdout_ probeCmd
    let durationStr = T.strip $ T.pack $ B8.unpack $ BL.toStrict probeOut
        totalDurationSeconds = fromMaybe 0.0 (readMaybe (T.unpack durationStr) :: Maybe Double)
        totalDurationMs = round (totalDurationSeconds * 1000000) :: Integer

    -- FFmpeg: transcode to HLS with proper chunking
    --   -force_key_frames ensures clean segment boundaries every 2 seconds
    --   -hls_time 6: 6-second segments
    --   -hls_segment_type mpegts for broad compatibility
    --   -progress pipe:1 outputs machine-readable progress to stdout
    let segmentPattern = tmpDir </> "segment_%03d.ts"
        outputPlaylist = tmpDir </> "output.m3u8"
        ffmpegProc =
          setStdout createPipe $
            proc
              "ffmpeg"
              [ "-i", inputFile,
                "-c:v", "libx264",
                "-preset", "fast",
                "-crf", "23",
                "-force_key_frames", "expr:gte(t,n_forced*2)",
                "-c:a", "aac",
                "-b:a", "128k",
                "-vf", T.unpack scale,
                "-f", "hls",
                "-hls_time", "6",
                "-hls_list_size", "0",
                "-hls_segment_type", "mpegts",
                "-hls_segment_filename", segmentPattern,
                "-progress", "pipe:1",
                "-nostats",
                outputPlaylist
              ]

    withProcessWait ffmpegProc $ \p -> do
      let outH = getStdout p
          loop lastPct = do
            eof <- hIsEOF outH
            if eof then return () else do
              line <- hGetLine outH
              let tline = T.pack line
              case T.stripPrefix "out_time_us=" tline of
                Just usStr -> do
                  let outTimeUs = fromMaybe 0 (readMaybe (T.unpack usStr) :: Maybe Integer)
                      pct :: Int32
                      pct = if totalDurationMs > 0
                              then fromIntegral (min 100 ((outTimeUs * 100) `div` totalDurationMs))
                              else 0
                  if pct > lastPct
                    then do
                      Domain.Database.updateJobProgress' dbConn vid pct
                      loop pct
                    else loop lastPct
                Nothing -> loop lastPct
          
      loop 0
      checkExitCode p

    -- Collect and upload all HLS output files to processed-videos bucket
    files <- listDirectory tmpDir
    let outputFiles = filter (\f -> takeExtension f `elem` [".m3u8", ".ts"]) files
    mapM_
      ( \f -> do
          let localPath = tmpDir </> f
              objectKey = encodeUtf8 outputDir <> "/" <> encodeUtf8 (T.pack f)
          uploadObject minioCfg outputBucket objectKey localPath
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
  logEnv <- setupLogEnv "video-worker" "development"
  runKatipContextT logEnv (mempty :: LogContexts) "startup" $ do
    $(logTM) InfoS "=== TVA Video Worker ==="

    -- Initialize PostgreSQL connection pool.
    pgHost <- liftIO $ fromMaybe "127.0.0.1" <$> lookupEnv "PG_HOST"
    mPgPort <- liftIO $ lookupEnv "PG_PORT"
    let pgPort = fromMaybe 5432 (mPgPort >>= readMaybe)
    pgUser <- liftIO $ fromMaybe "video_user" <$> lookupEnv "PG_USER"
    pgPass <- liftIO $ fromMaybe "video_password" <$> lookupEnv "PG_PASS"
    pgDb <- liftIO $ fromMaybe "video_db" <$> lookupEnv "PG_DB"
    let connSettings =
          Hasql.settings
            (fromString pgHost)
            (fromIntegral pgPort)
            (fromString pgUser)
            (fromString pgPass)
            (fromString pgDb)
    result <- liftIO $ Hasql.acquire connSettings
    dbConn <- case result of
      Left err -> error $ "Failed to connect to PostgreSQL: " ++ show err
      Right c -> return c
    $(logTM) InfoS "Connected to PostgreSQL"

    -- Initialize RabbitMQ connection.
    mqHost <- liftIO $ fromMaybe "127.0.0.1" <$> lookupEnv "RABBITMQ_HOST"
    mqUser <- liftIO $ fromMaybe "mq_user" <$> lookupEnv "RABBITMQ_USER"
    mqPass <- liftIO $ fromMaybe "mq_password" <$> lookupEnv "RABBITMQ_PASS"
    mqVhost <- liftIO $ fromMaybe "/" <$> lookupEnv "RABBITMQ_VHOST"
    conn <- liftIO $ openConnection mqHost (T.pack mqVhost) (T.pack mqUser) (T.pack mqPass)
    chan <- liftIO $ openChannel conn

    -- Declare the topic exchange (idempotent — fine if api-server already did it).
    liftIO $ declareExchange
      chan
      newExchange
        { exchangeName = "video_exchange",
          exchangeType = "topic",
          exchangeDurable = True
        }
    $(logTM) InfoS "Connected to RabbitMQ"

    -- Generate a random worker ID.
    workerUuid <- liftIO nextRandom
    let workerId = EntityId workerUuid :: EntityId Worker
    katipAddContext (sl "workerId" (show workerUuid)) $ $(logTM) InfoS "Worker ID generated"

    -- Initialize MinIO connection.
    $(logTM) InfoS "Initializing MinIO connection..."
    minioCfg <- liftIO initMinIO
    $(logTM) InfoS "MinIO connection initialized."

    -- Initialize retry / backoff configuration from environment.
    mMaxAttempts <- liftIO $ lookupEnv "RETRY_MAX_ATTEMPTS"
    let retryMaxAttempts = fromMaybe 3 (mMaxAttempts >>= readMaybe)
    mBaseDelay <- liftIO $ lookupEnv "RETRY_BASE_DELAY_SECONDS"
    let retryBaseDelaySeconds = fromMaybe 5 (mBaseDelay >>= readMaybe)
    mMaxDelay <- liftIO $ lookupEnv "RETRY_MAX_DELAY_SECONDS"
    let retryMaxDelaySeconds = fromMaybe 60 (mMaxDelay >>= readMaybe)
    let retryCfg = RetryConfig {retryMaxAttempts, retryBaseDelaySeconds, retryMaxDelaySeconds}
    
    katipAddContext (sl "maxAttempts" retryMaxAttempts <> sl "baseDelay" retryBaseDelaySeconds <> sl "maxDelay" retryMaxDelaySeconds) $
      $(logTM) InfoS "Retry config loaded"

    let cfg =
          WorkerConfig
            { wDbConn = dbConn,
              wRabbitConn = conn,
              wRabbitChan = chan,
              wWorkerId = workerId,
              wMinio = minioCfg,
              wRetry = retryCfg,
              wLogEnv = logEnv,
              wKatipContext = mempty :: LogContexts,
              wKatipNamespace = "video-worker"
            }

    -- Run the consumer loop (blocks until interrupted).
    liftIO $ setupConsumer cfg handleEvent

-- | Parse an integer from a string, returning Nothing on failure.
readMaybe :: (Read a) => String -> Maybe a
readMaybe s = case reads s of
  [(x, "")] -> Just x
  _ -> Nothing
