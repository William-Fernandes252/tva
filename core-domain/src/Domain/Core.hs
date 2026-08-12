{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Domain.Core where

import Data.Aeson (FromJSON, ToJSON)
import Data.Int (Int32)
import Data.Text (Text)
import Data.UUID (UUID)
import GHC.Generics (Generic)
import GHC.TypeLits (Symbol)

-- | A type-safe wrapper for entity identifiers, parameterized by a phantom type tag.
newtype EntityId (tag :: k) = EntityId UUID
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

-- | Core domain entities for the video processing service.
data Video

data Worker

data User

-- | Represents the state of a video processing job.
data JobState = Pending | Processing | Completed | Failed
  deriving (Show, Generic, FromJSON, ToJSON)

-- | A GADT representing a video processing job, parameterized by its state.
data VideoJob (s :: JobState) where
  -- A pending job only knows the source file location.
  QueuedJob :: EntityId Video -> Text -> VideoJob 'Pending
  -- A running job MUST have an assigned worker and a progress percentage.
  RunningJob :: EntityId Video -> EntityId Worker -> Int32 -> VideoJob 'Processing
  -- A finished job MUST have the output paths (e.g., HLS chunks).
  FinishedJob :: EntityId Video -> [Text] -> VideoJob 'Completed
  -- A failed job MUST have an error message.
  FailedJob :: EntityId Video -> Text -> VideoJob 'Failed

deriving instance Show (VideoJob s)

-- | Supported video resolutions for transcoding.
data Resolution = R1080p | R720p | R480p
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

-- | Map video resolutions to their corresponding routing keys for message queues.
type family RoutingKey (r :: Resolution) :: Symbol where
  RoutingKey 'R1080p = "transcode.high"
  RoutingKey 'R720p = "transcode.med"
  RoutingKey 'R480p = "transcode.low"

-- | Convert a 'Resolution' to a compact text tag for use in object keys.
resolutionToTag :: Resolution -> Text
resolutionToTag R1080p = "1080p"
resolutionToTag R720p = "720p"
resolutionToTag R480p = "480p"

-- | Parse a compact text tag back to a 'Resolution'.
resolutionFromTag :: Text -> Maybe Resolution
resolutionFromTag "1080p" = Just R1080p
resolutionFromTag "720p" = Just R720p
resolutionFromTag "480p" = Just R480p
resolutionFromTag _ = Nothing

-- | Start a video processing job.
startJob :: EntityId Worker -> VideoJob 'Pending -> VideoJob 'Processing
startJob workerId (QueuedJob vid _) = RunningJob vid workerId 0

-- | Finish a video processing job, transitioning it to the completed state.
finishJob :: [Text] -> VideoJob 'Processing -> VideoJob 'Completed
finishJob chunks (RunningJob vid _ _) = FinishedJob vid chunks