{-# LANGUAGE OverloadedStrings #-}

module Main where

import Api
import Config
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Time (NominalDiffTime)
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import Domain.Core (EntityId (..), JobState (..), Resolution (..))
import Domain.Event (SystemEvent (..))
import Domain.Queue (MonadQueue (..))
import Domain.Storage (MonadStorage (..))
import Network.Wai.Handler.Warp (run)
import Servant

-- | Polymorphic server implementation for the VideoAPI.
--   Works in any monad that supports storage, queue, and IO operations.
server :: (MonadStorage m, MonadQueue SystemEvent m, MonadIO m) => ServerT VideoAPI m
server = requestUpload :<|> checkStatus :<|> handleMinioWebhook
  where
    -- | Handler for POST /videos
    requestUpload :: (MonadStorage m, MonadQueue SystemEvent m, MonadIO m) => UploadRequest -> m UploadResponse
    requestUpload _req = do
      uuid <- liftIO nextRandom
      let videoId = EntityId uuid
          ttl     = 3600 :: NominalDiffTime  -- 1 hour presigned URL TTL

      url <- generateUploadUrl videoId ttl

      -- Publish an event so downstream workers know a video was uploaded.
      -- Resolution is hardcoded to R1080p for now; a future enhancement
      -- could add a 'resolution' field to UploadRequest.
      publish $ VideoUploadedEvent videoId R1080p

      return $ UploadResponse videoId url

    -- | Handler for GET /videos/:id/status
    checkStatus :: MonadIO m => UUID -> m StatusResponse
    checkStatus _uuid = do
      return $ StatusResponse Pending Nothing

    handleMinioWebhook :: MonadIO m => MinioWebhookEvent -> m NoContent
    handleMinioWebhook _event = do
      return NoContent

-- | Application Boot
main :: IO ()
main = do
  cfg <- initAppConfig
  putStrLn "Starting api-server on port 8080..."
  let application = serve videoApi (hoistServer videoApi (nt cfg) server)
  run 8080 application
