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

module Domain.Database
  ( -- * Schema
    AnyVideoJob (..),

    -- * MonadDatabase
    MonadDatabase (..),
  )
where

import Data.Int (Int32)
import Data.Text (Text)
import Domain.Core

-- | Represents any video job, regardless of its state, for easier handling in the application.
data AnyVideoJob where
  MkAnyVideoJob :: VideoJob s -> AnyVideoJob

-- | Monad to abstract database operations for the video-processing service.
class (Monad m) => MonadDatabase m where
  -- | Insert a new video job with 'Pending' status into the database.
  insertPendingJob :: EntityId Video -> Text -> m ()

  -- | Look up a video job by its entity identifier.
  findVideoJobById :: EntityId Video -> m (Maybe AnyVideoJob)

  -- | Transition a 'Pending' job to 'Processing', assigning it to a worker.
  updateJobToProcessing :: EntityId Video -> EntityId Worker -> m ()

  -- | Reset a 'Processing' job back to 'Pending' on failure, clearing worker assignment.
  updateJobToPending :: EntityId Video -> m ()

  -- | Mark a job as 'Completed', storing the HLS output chunk paths.
  updateJobToCompleted :: EntityId Video -> [Text] -> m ()

  -- | Mark a job as 'Failed', storing the error message.
  updateJobToFailed :: EntityId Video -> Text -> m ()

  -- | Update the progress of a processing job.
  updateJobProgress :: EntityId Video -> Int32 -> m ()