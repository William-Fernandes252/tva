{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Api where

import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Data.UUID (UUID)
import Domain.Core (EntityId (..), JobState (..), Video)
import GHC.Generics (Generic)
import Servant

data UploadRequest = UploadRequest
  { fileExtension :: Text,
    fileSize :: Int
  }
  deriving (Show, Generic)

instance FromJSON UploadRequest

instance ToJSON UploadRequest

data UploadResponse = UploadResponse
  { videoId :: EntityId Video,
    presignedUrl :: Text
  }
  deriving (Show, Generic)

instance FromJSON UploadResponse

instance ToJSON UploadResponse

data StatusResponse = StatusResponse
  { status :: JobState,
    progress :: Maybe Int
  }
  deriving (Show, Generic)

instance FromJSON StatusResponse

instance ToJSON StatusResponse

data MinioWebhookEvent = MinioWebhookEvent
  { eventName :: Text
  }
  deriving (Show, Generic)

instance FromJSON MinioWebhookEvent

type VideoAPI =
  -- POST /videos -> Request Upload URL
  "videos"
    :> ReqBody '[JSON] UploadRequest
    :> Post '[JSON] UploadResponse
    -- GET /videos/:id/status -> Poll video status (fallback for WebSockets)
    :<|> "videos"
      :> Capture "id" UUID
      :> "status"
      :> Get '[JSON] StatusResponse
    -- POST /webhooks/minio -> S3 ObjectCreated event
    :<|> "webhooks"
      :> "minio"
      :> ReqBody '[JSON] MinioWebhookEvent
      :> Post '[JSON] NoContent

-- | A Proxy object allows us to pass our type-level API as a runtime value to the server function.
videoApi :: Proxy VideoAPI
videoApi = Proxy