{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (withAsync)
import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVar, writeTVar)
import Control.Exception (SomeException, finally, try)
import Control.Monad (forever, unless, void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, eitherDecodeStrict)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as B8
import Data.ByteString.Lazy qualified as BL
import Data.Int (Int32)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, mapMaybe)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8)
import Database.PostgreSQL.LibPQ qualified as PQ
import Adapters.PostgreSQL (initJobStatusTrigger)
import Adapters.PostgreSQL qualified
import Domain.Logger
import GHC.Generics (Generic)
import Hasql.Connection qualified as Hasql
import Network.WebSockets qualified as WS
import System.Environment (lookupEnv)

-- | Payload received from the PostgreSQL trigger via NOTIFY.
data JobStatusNotification = JobStatusNotification
  { videoId :: Text,
    oldStatus :: Maybe Text,
    newStatus :: Text,
    outputChunks :: [Text],
    progress :: Maybe Int32,
    errorMessage :: Maybe Text
  }
  deriving (Show, Generic, FromJSON)

-- | Thread-safe map from video ID (text) to the set of WebSocket
--   connections currently watching that video.
newtype SubscriptionHub = SubscriptionHub (TVar (Int, Map Text [(Int, WS.Connection)]))

-- | Create an empty subscription hub.
newSubscriptionHub :: IO SubscriptionHub
newSubscriptionHub = SubscriptionHub <$> newTVarIO (0, Map.empty)

-- | Register a WebSocket connection as watching a specific video.
--   Returns a unique identifier for later un-subscription.
subscribe :: SubscriptionHub -> Text -> WS.Connection -> IO Int
subscribe (SubscriptionHub tv) vid conn = atomically $ do
  (nextId, subs) <- readTVar tv
  writeTVar tv (nextId + 1, Map.insertWith (<>) vid [(nextId, conn)] subs)
  return nextId

-- | Remove a WebSocket connection from a video's watch list by its identifier.
unsubscribe :: SubscriptionHub -> Text -> Int -> IO ()
unsubscribe (SubscriptionHub tv) vid connId = atomically $
  modifyTVar' tv $ \(n, subs) ->
    (n, Map.adjust (filter (\(i, _) -> i /= connId)) vid subs)

-- | Broadcast a payload to all WebSocket connections watching a video.
--   Dead connections are pruned from the hub.
broadcast :: SubscriptionHub -> Text -> BL.ByteString -> IO ()
broadcast (SubscriptionHub tv) vid payload = do
  pairs <- atomically $ do
    (_, subs) <- readTVar tv
    pure $ fromMaybe [] (Map.lookup vid subs)
  alive <- mapMaybe id <$> mapM trySend pairs
  atomically $ modifyTVar' tv $ \(n, subs) ->
    (n, Map.insert vid alive subs)
  where
    trySend (cid, conn) = do
      result <- try @SomeException (WS.sendTextData conn payload)
      pure $ case result of
        Right () -> Just (cid, conn)
        Left _ -> Nothing

-- | Open a dedicated libpq connection, LISTEN for job status changes,
--   and broadcast received notifications to the subscription hub.
listenLoop :: LogEnv -> SubscriptionHub -> PQ.Connection -> IO ()
listenLoop logEnv hub pgConn = runKatipContextT logEnv (mempty :: LogContexts) "notifier-worker" $ do
  $(logTM) InfoS "Starting PostgreSQL LISTEN loop on channel 'job_status_changed'..."
  _ <- liftIO $ PQ.exec pgConn "LISTEN job_status_changed"
  forever $ do
    _ <- liftIO $ PQ.consumeInput pgConn
    mNotif <- liftIO $ PQ.notifies pgConn
    case mNotif of
      Just notif -> do
        let rawPayload = PQ.notifyExtra notif
            decoded = decodeUtf8 rawPayload
        katipAddContext (sl "payload" decoded) $ $(logTM) InfoS "Received notification"
        case eitherDecodeStrict rawPayload of
          Right (n :: JobStatusNotification) ->
            liftIO $ broadcast hub (videoId n) (BL.fromStrict rawPayload)
          Left err ->
            katipAddContext (sl "error" err) $ $(logTM) WarningS "Failed to parse notification payload"
      Nothing ->
        liftIO $ threadDelay 100000

-- | WebSocket server application. Accepts connections on /ws/<videoId>
--   and streams job status updates for that video.
wsApp :: LogEnv -> SubscriptionHub -> WS.ServerApp
wsApp logEnv hub pendingConn = runKatipContextT logEnv (mempty :: LogContexts) "notifier-worker" $ do
  let path = decodeUtf8 (WS.requestPath (WS.pendingRequest pendingConn))
  case T.stripPrefix "/ws/" path of
    Just vid -> do
      conn <- liftIO $ WS.acceptRequest pendingConn
      katipAddContext (sl "videoId" vid) $ $(logTM) InfoS "WebSocket client connected"
      connId <- liftIO $ subscribe hub vid conn
      -- Keep the connection alive with a ping thread; clean up on disconnect.
      liftIO $ WS.withPingThread conn 30 (return ()) $
        forever (void (WS.receiveData conn :: IO ByteString))
          `finally` runKatipContextT logEnv (mempty :: LogContexts) "notifier-worker" (do
            liftIO $ unsubscribe hub vid connId
            katipAddContext (sl "videoId" vid) $ $(logTM) InfoS "WebSocket client disconnected")
    Nothing ->
      liftIO $ WS.rejectRequest pendingConn "Expected path /ws/<videoId>"

main :: IO ()
main = do
  logEnv <- setupLogEnv "notifier-worker" "development"
  runKatipContextT logEnv (mempty :: LogContexts) "startup" $ do
    $(logTM) InfoS "=== TVA Notifier Worker ==="

    -- Read PostgreSQL connection parameters from environment.
    pgHost <- liftIO $ fromMaybe "127.0.0.1" <$> lookupEnv "PG_HOST"
    mPgPort <- liftIO $ lookupEnv "PG_PORT"
    let pgPort = fromMaybe 5432 (mPgPort >>= readMaybe)
    pgUser <- liftIO $ fromMaybe "video_user" <$> lookupEnv "PG_USER"
    pgPass <- liftIO $ fromMaybe "video_password" <$> lookupEnv "PG_PASS"
    pgDb <- liftIO $ fromMaybe "video_db" <$> lookupEnv "PG_DB"

    hasqlConn <- liftIO Adapters.PostgreSQL.initPostgreSQL
    $(logTM) InfoS "Connected to PostgreSQL (Hasql)"

    -- Initialize the DB trigger (idempotent).
    liftIO $ initJobStatusTrigger hasqlConn

    let pgConnStr =
          "host="
            <> pgHost
            <> " port="
            <> show (pgPort :: Int)
            <> " dbname="
            <> pgDb
            <> " user="
            <> pgUser
            <> " password="
            <> pgPass
    pgConn <- liftIO $ PQ.connectdb (B8.pack pgConnStr)
    status <- liftIO $ PQ.status pgConn
    unless (status == PQ.ConnectionOk) $ do
      errMsg <- liftIO $ PQ.errorMessage pgConn
      liftIO $ PQ.finish pgConn
      error $ "Failed to connect to PostgreSQL (libpq): " <> show errMsg
    $(logTM) InfoS "Connected to PostgreSQL (libpq)."

    -- Read WebSocket server port.
    mWsPort <- liftIO $ lookupEnv "NOTIFIER_PORT"
    let wsPort = fromMaybe 8081 (mWsPort >>= readMaybe)

    -- Create the subscription hub.
    hub <- liftIO newSubscriptionHub

    -- Run the LISTEN loop in a background thread; block on the WS server.
    liftIO $ withAsync (listenLoop logEnv hub pgConn) $ \_listenAsync -> runKatipContextT logEnv (mempty :: LogContexts) "notifier-worker" $ do
      katipAddContext (sl "port" wsPort) $ $(logTM) InfoS "Starting WebSocket server"
      liftIO $ WS.runServer "0.0.0.0" wsPort (wsApp logEnv hub)

-- | Parse an integer from a string, returning 'Nothing' on failure.
readMaybe :: (Read a) => String -> Maybe a
readMaybe s = case reads s of
  [(x, "")] -> Just x
  _ -> Nothing
