{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleContexts #-}

module Domain.Storage where

import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Domain.Core (EntityId, Video)


-- | Monad to abstract storage operations, such as generating presigned URLs for video uploads.
class Monad m => MonadStorage m where
  -- | Generates a presigned URL for a client to upload a video directly.
  -- Takes the Video ID (for the object key) and a time-to-live (TTL).
  generateUploadUrl :: EntityId Video -> NominalDiffTime -> m Text
