{-# LANGUAGE OverloadedStrings #-}

module Main where

import Api
import Control.Monad.IO.Class (liftIO)
import Data.UUID (UUID)
import Data.UUID qualified
import Data.UUID.V4 (nextRandom)
import Domain.Core (EntityId (..), JobState (..))
import Network.Wai.Handler.Warp (run)
import Servant

-- | Servant server implementation for the VideoAPI.
server :: Server VideoAPI
server = requestUpload :<|> checkStatus :<|> handleMinioWebhook
  where
    -- | Handler for POST /videos
    requestUpload :: UploadRequest -> Handler UploadResponse
    requestUpload req = do
      uuid <- liftIO nextRandom
      let vId = EntityId uuid
          dummyUrl = "http://localhost:9000/videos/" <> (Data.UUID.toText uuid) <> "?X-Amz-Signature=..."

      return $ UploadResponse vId dummyUrl

    -- | Handler for GET /videos/:id/status
    checkStatus :: UUID -> Handler StatusResponse
    checkStatus uuid = do
      return $ StatusResponse Pending Nothing

    handleMinioWebhook :: MinioWebhookEvent -> Handler NoContent
    handleMinioWebhook _event = do
      return NoContent

-- | Application Boot
app :: Application
app = serve videoApi server

main :: IO ()
main = do
  putStrLn "Starting api-server on port 8080..."
  run 8080 app
