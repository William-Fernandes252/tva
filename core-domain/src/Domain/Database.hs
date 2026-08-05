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
    VideoJobRow (..),
    videoJobTable,
    AnyVideoJob (..),
    parseDbRow,

    -- * MonadDatabase
    MonadDatabase (..),

    -- * Query functions
    insertPendingJob',
    findVideoJobById',
    updateJobToProcessing',
  )
where

import Data.Int (Int32)
import Data.Text (Text)
import Data.UUID (UUID)
import Domain.Core
import GHC.Generics (Generic)
import Hasql.Connection qualified as Hasql (Connection)
import Hasql.Session qualified as Hasql (run, statement)
import Rel8

instance DBType (EntityId a) where
  typeInformation = mapTypeInformation (EntityId) (\(EntityId u) -> u) typeInformation

instance DBEq (EntityId a)

instance DBType JobState where
  typeInformation = mapTypeInformation fromText toText typeInformation
    where
      fromText :: Text -> JobState
      fromText "PENDING" = Pending
      fromText "PROCESSING" = Processing
      fromText "COMPLETED" = Completed
      fromText _ = Pending

      toText :: JobState -> Text
      toText Pending = "PENDING"
      toText Processing = "PROCESSING"
      toText Completed = "COMPLETED"

-- | Represents a row in the video_jobs table, corresponding to a VideoJob in the domain model.
data VideoJobRow f = VideoJobRow
  { jobId :: Column f (EntityId Video),
    jobStatus :: Column f JobState,
    sourceUrl :: Column f Text,
    assignedTo :: Column f (Maybe (EntityId Worker)),
    progress :: Column f (Maybe Int32),
    outputChunks :: Column f [Text]
  }
  deriving stock (Generic)
  deriving anyclass (Rel8able)

deriving stock instance Show (VideoJobRow Result)

-- | Defines the schema for the video_jobs table in the database.
videoJobTable :: TableSchema (VideoJobRow Name)
videoJobTable =
  TableSchema
    { name =
        QualifiedName
          { name = "video_jobs",
            schema = Just "public"
          },
      columns =
        VideoJobRow
          { jobId = "id",
            jobStatus = "status",
            sourceUrl = "source_url",
            assignedTo = "assigned_worker_id",
            progress = "progress_percent",
            outputChunks = "output_chunks"
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

-- | Monad to abstract database operations for the video-processing service.
class (Monad m) => MonadDatabase m where
  -- | Insert a new video job with 'Pending' status into the database.
  insertPendingJob :: EntityId Video -> Text -> m ()

  -- | Look up a video job by its entity identifier.
  findVideoJobById :: EntityId Video -> m (Maybe AnyVideoJob)

  -- | Transition a 'Pending' job to 'Processing', assigning it to a worker.
  updateJobToProcessing :: EntityId Video -> EntityId Worker -> m ()

----------------------------------------------------------------------
-- Standalone query functions that work with a plain 'Hasql.Connection'.
-- These are the canonical Rel8-based implementations shared by all services.
----------------------------------------------------------------------

-- | Insert a new video job with 'Pending' status.
insertPendingJob' :: Hasql.Connection -> EntityId Video -> Text -> IO ()
insertPendingJob' conn vid source = do
  let row :: VideoJobRow Expr
      row =
        VideoJobRow
          { jobId = lit vid,
            jobStatus = lit Pending,
            sourceUrl = lit source,
            assignedTo = lit Nothing,
            progress = lit Nothing,
            outputChunks = lit []
          }
      stmt =
        insert
          Insert
            { into = videoJobTable,
              rows = values [row],
              onConflict = Abort,
              returning = NoReturning
            }
  _ <- Hasql.run (Hasql.statement () (run_ stmt)) conn
  return ()

-- | Look up a video job by its entity identifier.
findVideoJobById' :: Hasql.Connection -> EntityId Video -> IO (Maybe AnyVideoJob)
findVideoJobById' conn vid = do
  let stmt = select $ limit 1 $ do
        row <- each videoJobTable
        where_ $ jobId row ==. lit vid
        return row
  result <- Hasql.run (Hasql.statement () (run stmt)) conn
  case result of
    Left err -> putStrLn ("DB error: " ++ show err) >> return Nothing
    Right [row] -> case parseDbRow row of
      Right job -> return (Just job)
      Left _ -> return Nothing
    Right _ -> return Nothing

-- | Transition a 'Pending' job to 'Processing', assigning it to a worker.
updateJobToProcessing' :: Hasql.Connection -> EntityId Video -> EntityId Worker -> IO ()
updateJobToProcessing' conn vid worker = do
  let stmt =
        update
          Update
            { target = videoJobTable,
              from = pure (),
              set = \_ row ->
                row
                  { jobStatus = lit Processing,
                    assignedTo = lit (Just worker),
                    progress = lit (Just 0)
                  },
              updateWhere = \_ row -> jobId row ==. lit vid,
              returning = NoReturning
            }
  _ <- Hasql.run (Hasql.statement () (run_ stmt)) conn
  return ()