{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Domain.Database where

import Data.Int (Int32)
import Data.Text (Text)
import Data.UUID (UUID)
import GHC.Generics (Generic)
import Rel8
import Domain.Core 

instance DBType (EntityId a) where
  typeInformation = mapTypeInformation (EntityId) (\(EntityId u) -> u) typeInformation

instance DBType JobState where
  typeInformation = mapTypeInformation fromText toText typeInformation
    where
      fromText :: Text -> JobState
      fromText "PENDING"    = Pending
      fromText "PROCESSING" = Processing
      fromText "COMPLETED"  = Completed
      fromText _            = Pending

      toText :: JobState -> Text
      toText Pending    = "PENDING"
      toText Processing = "PROCESSING"
      toText Completed  = "COMPLETED"

-- | Represents a row in the video_jobs table, corresponding to a VideoJob in the domain model.
data VideoJobRow f = VideoJobRow
  { jobId        :: Column f (EntityId Video)
  , jobStatus    :: Column f JobState
  , sourceUrl    :: Column f Text
  , assignedTo   :: Column f (Maybe (EntityId Worker))
  , progress     :: Column f (Maybe Int32)
  , outputChunks :: Column f [Text]
  }
  deriving stock (Generic)
  deriving anyclass (Rel8able)

deriving stock instance Show (VideoJobRow Result)

-- | Defines the schema for the video_jobs table in the database.
videoJobTable :: TableSchema (VideoJobRow Name)
videoJobTable = TableSchema
  { name = QualifiedName
      { name = "video_jobs"
      , schema = Just "public"
      }
  , columns = VideoJobRow
      { jobId        = "id"
      , jobStatus    = "status"
      , sourceUrl    = "source_url"
      , assignedTo   = "assigned_worker_id"
      , progress     = "progress_percent"
      , outputChunks = "output_chunks"
      }
  }

-- | Represents any video job, regardless of its state, for easier handling in the application.
data AnyVideoJob where
  MkAnyVideoJob :: VideoJob s -> AnyVideoJob

-- | Parses a database row into an AnyVideoJob, handling the different job states appropriately.
parseDbRow :: VideoJobRow Result -> Either Text AnyVideoJob
parseDbRow row = case jobStatus row of
  Pending -> 
    Right $ MkAnyVideoJob (QueuedJob (jobId row) (sourceUrl row))
  
  Processing -> 
    case (assignedTo row, progress row) of
      (Just worker, Just prog) -> Right $ MkAnyVideoJob (RunningJob (jobId row) worker prog)
      _ -> Left "Corrupted DB state: Processing job missing worker or progress"
  
  Completed -> 
    Right $ MkAnyVideoJob (FinishedJob (jobId row) (outputChunks row))