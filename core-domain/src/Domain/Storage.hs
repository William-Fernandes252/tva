{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleContexts #-}

module Domain.Storage where

import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Domain.Core (EntityId, Resolution, Video)


-- | Monad to abstract storage operations, such as generating presigned URLs for video uploads.
class Monad m => MonadStorage m where
  -- | Generates a presigned URL for a client to upload a video directly.
  -- Takes the Video ID (for the object key), the desired resolution,
  -- and a time-to-live (TTL).
  generateUploadUrl :: EntityId Video -> Resolution -> NominalDiffTime -> m Text

  -- | Downloads a blob from a bucket to a local file path.
  --   Takes: Bucket name, Object Key, Destination FilePath
  downloadBlob :: Text -> Text -> FilePath -> m ()

  -- | Uploads a local file to a blob storage bucket.
  --   Takes: Bucket name, Object Key, Source FilePath
  uploadBlob :: Text -> Text -> FilePath -> m ()
