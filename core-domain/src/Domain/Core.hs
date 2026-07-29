{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Domain.Core where

import Data.Int (Int32)
import Data.Kind (Type)
import Data.Text (Text)
import Data.UUID (UUID)
import GHC.TypeLits (Symbol)

-- | A type-safe wrapper for entity identifiers, parameterized by a phantom type tag.
newtype EntityId (tag :: k) = EntityId UUID
  deriving (Show, Eq)

-- | Core domain entities for the video processing service.
data Video

data Worker

data User

-- | Represents the state of a video processing job.
data JobState = Pending | Processing | Completed
  deriving Show

-- | A GADT representing a video processing job, parameterized by its state.
data VideoJob (s :: JobState) where
  -- A pending job only knows the source file location.
  QueuedJob :: EntityId Video -> Text -> VideoJob 'Pending
  -- A running job MUST have an assigned worker and a progress percentage.
  RunningJob :: EntityId Video -> EntityId Worker -> Int32 -> VideoJob 'Processing
  -- A finished job MUST have the output paths (e.g., HLS chunks).
  FinishedJob :: EntityId Video -> [Text] -> VideoJob 'Completed

deriving instance Show (VideoJob s)

-- | Supported video resolutions for transcoding.
data Resolution = R1080p | R720p | R480p

-- | Map video resolutions to their corresponding routing keys for message queues.
type family RoutingKey (r :: Resolution) :: Symbol where
  RoutingKey 'R1080p = "transcode.high"
  RoutingKey 'R720p = "transcode.med"
  RoutingKey 'R480p = "transcode.low"

-- | Start a video processing job.
startJob :: EntityId Worker -> VideoJob 'Pending -> VideoJob 'Processing
startJob workerId (QueuedJob vid _) = RunningJob vid workerId 0

-- | Finish a video processing job, transitioning it to the completed state.
finishJob :: [Text] -> VideoJob 'Processing -> VideoJob 'Completed
finishJob chunks (RunningJob vid _ _) = FinishedJob vid chunks