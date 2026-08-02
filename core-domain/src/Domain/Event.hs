{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Domain.Event where

import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import GHC.Generics (Generic)
import Domain.Core (EntityId, Video, Resolution)

-- | Represents system events that can be published to the message broker.
data SystemEvent
  = VideoUploadedEvent (EntityId Video) Resolution
  | TranscodeFinishedEvent (EntityId Video) [Text]
  | TranscodeFailedEvent (EntityId Video) Text
  deriving (Show, Eq, Generic, FromJSON, ToJSON)