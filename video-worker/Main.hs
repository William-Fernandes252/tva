{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT, ask, asks, runReaderT)
import Data.Aeson qualified as Aeson
import Data.Maybe (fromMaybe)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID (toText)
import Data.UUID.V4 (nextRandom)
import Network.AMQP
import System.Environment (lookupEnv)

import qualified Hasql.Connection as Hasql (Connection, acquire, settings)

import Domain.Core (EntityId (..), JobState (..), Video, VideoJob (..), Worker, resolutionToTag)
import Domain.Database
  ( MonadDatabase (..)
  , AnyVideoJob (MkAnyVideoJob)
  , insertPendingJob'
  , findVideoJobById'
  , updateJobToProcessing'
  )
import Domain.Event (SystemEvent (..))
import Domain.Queue (MonadQueue (..))

-- | Configuration for the video worker, bundling all infrastructure handles.
data WorkerConfig = WorkerConfig
  { wDbConn      :: Hasql.Connection
  , wRabbitConn  :: Connection
  , wRabbitChan  :: Channel
  , wWorkerId    :: EntityId Worker
  }

-- | The concrete monad stack for the video worker.
newtype WorkerM a = WorkerM { runWorkerM :: ReaderT WorkerConfig IO a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader WorkerConfig)

-- | Action to set up the queue and start consuming. This blocks until the
--   channel is closed (i.e., runs forever).
setupConsumer :: WorkerConfig -> (SystemEvent -> WorkerM ()) -> IO ()
setupConsumer cfg handler = do
  let chan = wRabbitChan cfg

  -- Declare a durable queue for video upload events.
  declareQueue chan newQueue
    { queueName       = "video_upload_queue"
    , queueDurable    = True
    , queueExclusive  = False
    , queueAutoDelete = False
    }

  -- Bind to the topic exchange with the "video.uploaded" routing key.
  bindQueue chan "video_upload_queue" "video_exchange" "video.uploaded"

  putStrLn "Video worker: waiting for VideoUploadedEvent messages..."
  putStrLn "  queue: video_upload_queue"
  putStrLn "  exchange: video_exchange"
  putStrLn "  routing key: video.uploaded"

  -- Start consuming. This call blocks.
  _consumerTag <- consumeMsgs chan "video_upload_queue" Ack $ \(msg, _meta) -> do
    case Aeson.decode (msgBody msg) of
      Just event -> do
        let action = handler event
        _result <- runReaderT (runWorkerM action) cfg
        return ()
      Nothing ->
        putStrLn $ "[WARN] Could not decode message body as SystemEvent"
  return ()

instance MonadQueue SystemEvent WorkerM where
  publish :: SystemEvent -> WorkerM ()
  publish event = do
    chan <- asks wRabbitChan
    let payload = Aeson.encode event
        rKey = case event of
          VideoUploadedEvent _ _   -> "video.uploaded"
          TranscodeFinishedEvent _ _ -> "video.finished"
          TranscodeFailedEvent _ _  -> "video.failed"
        message = newMsg
          { msgBody = payload
          , msgDeliveryMode = Just Persistent
          }
    liftIO $ do
      _ <- publishMsg chan "video_exchange" rKey message
      return ()

  consume :: (SystemEvent -> WorkerM ()) -> WorkerM ()
  consume handler = do
    cfg <- ask
    liftIO $ setupConsumer cfg handler

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

-- | Handle an incoming system event.
handleEvent :: SystemEvent -> WorkerM ()
handleEvent (VideoUploadedEvent vid res) = do
  workerId <- asks wWorkerId
  liftIO $ putStrLn $
    "[INFO] Received VideoUploadedEvent { videoId: " <> T.unpack (toText' vid)
    <> ", resolution: " <> T.unpack (resolutionToTag res) <> " }"

  mJob <- findVideoJobById vid
  case mJob of
    Just (MkAnyVideoJob (QueuedJob _ _)) -> do
      liftIO $ putStrLn "[INFO] Found Pending job. Transitioning to Processing..."
      updateJobToProcessing vid workerId
      liftIO $ putStrLn "[INFO] Job updated to Processing."
    Just (MkAnyVideoJob _) ->
      liftIO $ putStrLn "[WARN] Job is not in Pending state. Skipping."
    Nothing ->
      liftIO $ putStrLn "[WARN] No job found for this video ID. Skipping."

handleEvent (TranscodeFinishedEvent _vid _chunks) =
  liftIO $ putStrLn "[INFO] Received TranscodeFinishedEvent (not yet handled)."

handleEvent (TranscodeFailedEvent _vid _err) =
  liftIO $ putStrLn "[INFO] Received TranscodeFailedEvent (not yet handled)."

-- | Extract the UUID text from an EntityId for logging.
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
  pgDb   <- fromMaybe "video_db" <$> lookupEnv "PG_DB"
  let connSettings = Hasql.settings
        (fromString pgHost)
        (fromIntegral pgPort)
        (fromString pgUser)
        (fromString pgPass)
        (fromString pgDb)
  result <- Hasql.acquire connSettings
  dbConn <- case result of
    Left err -> error $ "Failed to connect to PostgreSQL: " ++ show err
    Right c  -> return c
  putStrLn $ "Connected to PostgreSQL at " <> pgHost <> ":" <> show pgPort

  -- Initialize RabbitMQ connection.
  mqHost  <- fromMaybe "127.0.0.1" <$> lookupEnv "RABBITMQ_HOST"
  mqUser  <- fromMaybe "mq_user"    <$> lookupEnv "RABBITMQ_USER"
  mqPass  <- fromMaybe "mq_password" <$> lookupEnv "RABBITMQ_PASS"
  mqVhost <- fromMaybe "/"           <$> lookupEnv "RABBITMQ_VHOST"
  conn <- openConnection mqHost (T.pack mqVhost) (T.pack mqUser) (T.pack mqPass)
  chan <- openChannel conn

  -- Declare the topic exchange (idempotent — fine if api-server already did it).
  declareExchange chan newExchange
    { exchangeName    = "video_exchange"
    , exchangeType    = "topic"
    , exchangeDurable = True
    }
  putStrLn $ "Connected to RabbitMQ at " <> mqHost

  -- Generate a random worker ID.
  workerUuid <- nextRandom
  let workerId = EntityId workerUuid :: EntityId Worker
  putStrLn $ "Worker ID: " <> show workerUuid

  let cfg = WorkerConfig
        { wDbConn      = dbConn
        , wRabbitConn  = conn
        , wRabbitChan  = chan
        , wWorkerId    = workerId
        }

  -- Run the consumer loop (blocks until interrupted).
  runReaderT (runWorkerM (consume handleEvent)) cfg

-- | Parse an integer from a string, returning Nothing on failure.
readMaybe :: Read a => String -> Maybe a
readMaybe s = case reads s of
  [(x, "")] -> Just x
  _         -> Nothing
