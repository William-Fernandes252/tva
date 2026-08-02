{-# LANGUAGE OverloadedStrings #-}

module Main where

import Adapters.S3 (AppM (..), S3Config, initMinioEnv)
import Api
import Control.Exception (SomeException, try)
import Control.Monad.Except (ExceptT (..))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (runReaderT)
import Data.String (fromString)
import Data.Time (NominalDiffTime)
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import Domain.Core (EntityId (..), JobState (..))
import Domain.Storage (MonadStorage (..))
import Network.Wai.Handler.Warp (run)
import Servant

-- | Polymorphic server implementation for the VideoAPI.
--   Works in any monad that supports storage operations and IO.
server :: (MonadStorage m, MonadIO m) => ServerT VideoAPI m
server = requestUpload :<|> checkStatus :<|> handleMinioWebhook
  where
    -- | Handler for POST /videos
    requestUpload :: (MonadStorage m, MonadIO m) => UploadRequest -> m UploadResponse
    requestUpload _req = do
      uuid <- liftIO nextRandom
      let videoId = EntityId uuid
          ttl     = 3600 :: NominalDiffTime  -- 1 hour presigned URL TTL

      url <- generateUploadUrl videoId ttl
      return $ UploadResponse videoId url

    -- | Handler for GET /videos/:id/status
    checkStatus :: MonadIO m => UUID -> m StatusResponse
    checkStatus _uuid = do
      return $ StatusResponse Pending Nothing

    handleMinioWebhook :: MonadIO m => MinioWebhookEvent -> m NoContent
    handleMinioWebhook _event = do
      return NoContent

-- | Natural transformation from AppM to Handler.
--   Catches IO exceptions and wraps them as HTTP 500 errors.
nt :: S3Config -> AppM a -> Handler a
nt cfg action = Handler $ ExceptT $ do
  result <- try $ runReaderT (runAppM action) cfg
  case result of
    Left (e :: SomeException) ->
      pure $ Left $ err500 { errBody = fromString $ "Internal server error: " ++ show e }
    Right a ->
      pure $ Right a

-- | Application Boot
main :: IO ()
main = do
  putStrLn "Initializing S3/MinIO connection..."
  cfg <- initMinioEnv
  putStrLn "Starting api-server on port 8080..."
  let application = serve videoApi (hoistServer videoApi (nt cfg) server)
  run 8080 application
