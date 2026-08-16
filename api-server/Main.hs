{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import Api
import Config
import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (forever, forM_, guard)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (runReaderT)
import Control.Monad.Except (runExceptT, MonadError)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Data.UUID (UUID, fromText, toText)
import Data.UUID.V4 (nextRandom)
import Domain.Core (EntityId (..), JobState (..), Resolution, Video, resolutionFromTag, resolutionToTag, VideoJob(..))
import Domain.Database (MonadDatabase (..), AnyVideoJob(..))
import Adapters.PostgreSQL qualified
import Control.Monad.Except (MonadError)
import Domain.Event (SystemEvent (..))
import Domain.Queue (MonadQueue (..))
import Domain.Storage (MonadStorage (..))
import Domain.Logger
import Network.Wai.Handler.Warp (run)
import Servant

-- | Polymorphic server implementation for the VideoAPI.
--   Works in any monad that supports storage, queue, database, and IO operations.
server :: (MonadStorage m, MonadQueue SystemEvent m, MonadDatabase m, MonadIO m, MonadError ServerError m) => Text -> ServerT VideoAPI m
server webhookSecret = requestUpload :<|> checkStatus :<|> handleMinioWebhook :<|> checkHealth
  where
    -- \| Handler for POST /videos
    --   Generates a presigned upload URL and inserts a Pending job into the database.
    --   The actual VideoUploadedEvent is published later by 'handleMinioWebhook'
    --   when MinIO confirms the upload.
    requestUpload :: (MonadStorage m, MonadDatabase m, MonadIO m) => UploadRequest -> m UploadResponse
    requestUpload req = do
      uuid <- liftIO nextRandom
      let videoId = EntityId uuid
          ttl = 3600 :: NominalDiffTime -- 1 hour presigned URL TTL
      url <- generateUploadUrl videoId (resolution req) ttl
      let sourceObjKey = toText uuid <> "_" <> resolutionToTag (resolution req) <> ".mkv"
      insertPendingJob videoId sourceObjKey
      return $ UploadResponse videoId url

    -- \| Handler for GET /videos/:id/status
    checkStatus :: (MonadDatabase m, MonadIO m, MonadError ServerError m) => UUID -> m StatusResponse
    checkStatus uuid = do
      mJob <- findVideoJobById (EntityId uuid)
      case mJob of
        Just (MkAnyVideoJob job) -> case job of
          QueuedJob _ _ -> return $ StatusResponse Pending Nothing Nothing
          RunningJob _ _ prog -> return $ StatusResponse Processing (Just (fromIntegral prog)) Nothing
          FinishedJob _ _ -> return $ StatusResponse Completed Nothing Nothing
          FailedJob _ err -> return $ StatusResponse Failed Nothing (Just err)
        Nothing -> throwError err404 { errBody = "Video job not found" }

    -- | Handler for POST /webhooks/minio
    handleMinioWebhook :: (MonadQueue SystemEvent m, MonadIO m, MonadError ServerError m) => Maybe Text -> MinioWebhookEvent -> m NoContent
    handleMinioWebhook mAuthHeader event = do
      let expectedToken = "Bearer " <> webhookSecret
      if mAuthHeader /= Just expectedToken
        then throwError err401 { errBody = "Unauthorized webhook request" }
        else do
          -- Only process ObjectCreated:Put events. Ack everything else silently.
          if not (isObjectCreated event)
            then return NoContent
            else do
              case extractKey event >>= parseObjectKey of
                Nothing -> return NoContent
                Just (videoId, resolution) -> do
                  publish $ VideoUploadedEvent videoId resolution
                  return NoContent

    -- | Handler for GET /health
    checkHealth :: (Monad m) => m NoContent
    checkHealth = return NoContent

-- | Check whether the webhook event is an s3:ObjectCreated:Put notification.
isObjectCreated :: MinioWebhookEvent -> Bool
isObjectCreated (MinioWebhookEvent evtName _ recs) =
  evtName == "s3:ObjectCreated:Put"
    || any (\(MinioRecord recName _) -> recName == "s3:ObjectCreated:Put") recs

-- | Extract the object key from a MinIO webhook event.
extractKey :: MinioWebhookEvent -> Maybe Text
extractKey (MinioWebhookEvent _ mKey recs) =
  case recs of
    (MinioRecord _ (MinioS3Payload _ (MinioObject objKey _ _))) : _ -> Just objKey
    [] -> mKey

-- | Parse the object key to extract the Video ID and Resolution.
parseObjectKey :: Text -> Maybe (EntityId Video, Resolution)
parseObjectKey objKey = do
  base <- T.stripSuffix ".mkv" objKey
  let (uuidWithUnderscore, tag) = T.breakOnEnd "_" base
  let uuidText = T.dropEnd 1 uuidWithUnderscore
  guard (not (T.null uuidText) && not (T.null tag))
  uuid <- fromText uuidText
  res <- resolutionFromTag tag
  return (EntityId uuid, res)

-- | Background thread that periodically resets stalled jobs and republishes them.
sweeperThread :: AppConfig -> IO ()
sweeperThread cfg = forever $ do
  threadDelay (5 * 60 * 1000000) -- 5 minutes
  runKatipContextT (appLogEnv cfg) (mempty :: LogContexts) "api-server" $ do
    $(logTM) InfoS "Sweeping for zombie jobs..."
    rows <- liftIO $ Adapters.PostgreSQL.resetZombieJobs (appDbConn cfg)
    forM_ rows $ \(vid, sourceUrl) -> do
      katipAddContext (sl "videoId" (show vid)) $ do
        $(logTM) InfoS "Reset zombie job"
        case parseObjectKey sourceUrl of
          Just (vId, res) -> do
            _ <- liftIO $ runExceptT $ runReaderT (runAppM (publish (VideoUploadedEvent vId res))) cfg
            return ()
          Nothing -> $(logTM) WarningS "Could not parse resolution for zombie job"

-- | Application Boot
main :: IO ()
main = do
  cfg <- initAppConfig
  let secret = appWebhookSecret cfg
  _ <- forkIO (sweeperThread cfg)
  runKatipContextT (appLogEnv cfg) (mempty :: LogContexts) "api-server" $ do
    $(logTM) InfoS "Starting api-server on port 8080..."
  let application = serve videoApi (hoistServer videoApi (nt cfg) (server secret))
  run 8080 application
