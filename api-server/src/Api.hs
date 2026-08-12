{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Api where

import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Data.UUID (UUID)
import Domain.Core (EntityId (..), JobState (..), Resolution, Video)
import GHC.Generics (Generic)
import Servant

data UploadRequest = UploadRequest
  { fileExtension :: Text,
    fileSize :: Int,
    resolution :: Resolution
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
    progress :: Maybe Int,
    errorMessage :: Maybe Text
  }
  deriving (Show, Generic)

instance FromJSON StatusResponse

instance ToJSON StatusResponse

-- | MinIO S3 event notification payload sent to the webhook endpoint.
--   Matches the AWS S3 Event Notification structure used by MinIO.
data MinioWebhookEvent = MinioWebhookEvent
  { eventName :: Text,
    key :: Maybe Text,
    records :: [MinioRecord]
  }
  deriving (Show, Generic)

instance FromJSON MinioWebhookEvent

-- | A single event record within an S3 notification.
data MinioRecord = MinioRecord
  { eventName :: Text,
    s3 :: MinioS3Payload
  }
  deriving (Show, Generic)

instance FromJSON MinioRecord

-- | The S3-specific payload within a notification record.
data MinioS3Payload = MinioS3Payload
  { bucket :: MinioBucket,
    object :: MinioObject
  }
  deriving (Show, Generic, FromJSON)

-- | S3 bucket information from a notification.
data MinioBucket = MinioBucket
  { name :: Text
  }
  deriving (Show, Generic)

instance FromJSON MinioBucket

-- | S3 object information from a notification.
data MinioObject = MinioObject
  { key :: Text,
    size :: Maybe Int,
    eTag :: Maybe Text
  }
  deriving (Show, Generic)

instance FromJSON MinioObject

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