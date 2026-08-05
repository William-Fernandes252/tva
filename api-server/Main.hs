{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Api
import Config
import Control.Monad (guard)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Data.UUID (UUID, fromText, toText)
import Data.UUID.V4 (nextRandom)
import Domain.Core (EntityId (..), JobState (..), Resolution, Video, resolutionFromTag, resolutionToTag)
import Domain.Database (MonadDatabase (..))
import Domain.Event (SystemEvent (..))
import Domain.Queue (MonadQueue (..))
import Domain.Storage (MonadStorage (..))
import Network.Wai.Handler.Warp (run)
import Servant

-- | Polymorphic server implementation for the VideoAPI.
--   Works in any monad that supports storage, queue, database, and IO operations.
server :: (MonadStorage m, MonadQueue SystemEvent m, MonadDatabase m, MonadIO m) => ServerT VideoAPI m
server = requestUpload :<|> checkStatus :<|> handleMinioWebhook
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
    checkStatus :: (MonadIO m) => UUID -> m StatusResponse
    checkStatus _uuid = do
      return $ StatusResponse Pending Nothing

    -- \| Handler for POST /webhooks/minio
    handleMinioWebhook :: (MonadQueue SystemEvent m, MonadIO m) => MinioWebhookEvent -> m NoContent
    handleMinioWebhook event = do
      -- Only process ObjectCreated:Put events. Ack everything else silently.
      if not (isObjectCreated event)
        then return NoContent
        else do
          case extractKey event >>= parseObjectKey of
            Nothing -> return NoContent
            Just (videoId, resolution) -> do
              publish $ VideoUploadedEvent videoId resolution
              return NoContent

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
  let (uuidText, tagPart) = T.breakOnEnd "_" base
  let tag = T.drop 1 tagPart
  guard (not (T.null uuidText) && not (T.null tag))
  uuid <- fromText uuidText
  res <- resolutionFromTag tag
  return (EntityId uuid, res)

-- | Application Boot
main :: IO ()
main = do
  cfg <- initAppConfig
  putStrLn "Starting api-server on port 8080..."
  let application = serve videoApi (hoistServer videoApi (nt cfg) server)
  run 8080 application
