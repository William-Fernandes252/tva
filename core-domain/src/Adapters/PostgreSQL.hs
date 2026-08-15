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

module Adapters.PostgreSQL
  ( -- * Initialization
    initPostgreSQL,
    
    -- * Schema
    VideoJobRow (..),
    videoJobTable,
    parseDbRow,

    -- * Query functions
    insertPendingJob,
    findVideoJobById,
    updateJobToProcessing,
    updateJobToPending,
    updateJobToCompleted,
    updateJobToFailed,
    updateJobProgress,
    resetZombieJobs,

    -- * Trigger initialization
    initJobStatusTrigger,
  )
where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Int (Int32)
import Data.Maybe (fromMaybe)
import Data.String (fromString)
import Data.Text (Text)
import Domain.Core
import Domain.Database (AnyVideoJob (..))
import GHC.Generics (Generic)
import Hasql.Connection qualified as Hasql
import Hasql.Decoders qualified as D
import Hasql.Encoders qualified as E
import Hasql.Session qualified as Hasql (run, sql, statement)
import Hasql.Statement (Statement (..))
import Rel8
import System.Environment (lookupEnv)

-- | Initialize a PostgreSQL connection using environment variables.
initPostgreSQL :: IO Hasql.Connection
initPostgreSQL = do
  putStrLn "Initializing PostgreSQL connection..."
  pgHost <- fromMaybe "127.0.0.1" <$> lookupEnv "PG_HOST"
  mPgPort <- lookupEnv "PG_PORT"
  let pgPort = fromMaybe 5432 (mPgPort >>= readMaybe)
  pgUser <- fromMaybe "video_user" <$> lookupEnv "PG_USER"
  pgPass <- fromMaybe "video_password" <$> lookupEnv "PG_PASS"
  pgDb <- fromMaybe "video_db" <$> lookupEnv "PG_DB"
  let connSettings =
        Hasql.settings
          (fromString pgHost)
          (fromIntegral (pgPort :: Int))
          (fromString pgUser)
          (fromString pgPass)
          (fromString pgDb)
  result <- Hasql.acquire connSettings
  case result of
    Left err -> error $ "Failed to connect to PostgreSQL: " ++ show err
    Right c -> return c

-- | Parse an integer from a string, returning Nothing on failure.
readMaybe :: (Read a) => String -> Maybe a
readMaybe s = case reads s of
  [(x, "")] -> Just x
  _ -> Nothing

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
      fromText "FAILED" = Failed
      fromText _ = Pending

      toText :: JobState -> Text
      toText Pending = "PENDING"
      toText Processing = "PROCESSING"
      toText Completed = "COMPLETED"
      toText Failed = "FAILED"

instance DBEq JobState

-- | Represents a row in the video_jobs table, corresponding to a VideoJob in the domain model.
data VideoJobRow f = VideoJobRow
  { jobId :: Column f (EntityId Video),
    jobStatus :: Column f JobState,
    sourceUrl :: Column f Text,
    assignedTo :: Column f (Maybe (EntityId Worker)),
    progress :: Column f (Maybe Int32),
    outputChunks :: Column f [Text],
    errorMessage :: Column f (Maybe Text)
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
            outputChunks = "output_chunks",
            errorMessage = "error_message"
          }
    }

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
  Failed ->
    case errorMessage row of
      Just err -> Right $ MkAnyVideoJob (FailedJob (jobId row) err)
      Nothing -> Right $ MkAnyVideoJob (FailedJob (jobId row) "Unknown error")

-- | Insert a new video job with 'Pending' status.
insertPendingJob :: Hasql.Connection -> EntityId Video -> Text -> IO ()
insertPendingJob conn vid source = do
  let row :: VideoJobRow Expr
      row =
        VideoJobRow
          { jobId = lit vid,
            jobStatus = lit Pending,
            sourceUrl = lit source,
            assignedTo = lit Nothing,
            progress = lit Nothing,
            outputChunks = lit [],
            errorMessage = lit Nothing
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
findVideoJobById :: Hasql.Connection -> EntityId Video -> IO (Maybe AnyVideoJob)
findVideoJobById conn vid = do
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
updateJobToProcessing :: Hasql.Connection -> EntityId Video -> EntityId Worker -> IO ()
updateJobToProcessing conn vid worker = do
  let stmt =
        update
          Update
            { target = videoJobTable,
              from = pure (),
              set = \_ row ->
                row
                  { jobStatus = lit Processing,
                    assignedTo = lit (Just worker),
                    progress = lit (Just 0),
                    errorMessage = lit Nothing
                  },
              updateWhere = \_ row -> jobId row ==. lit vid,
              returning = NoReturning
            }
  _ <- Hasql.run (Hasql.statement () (run_ stmt)) conn
  return ()

-- | Reset a job back to 'Pending', clearing worker assignment and progress.
updateJobToPending :: Hasql.Connection -> EntityId Video -> IO ()
updateJobToPending conn vid = do
  let stmt =
        update
          Update
            { target = videoJobTable,
              from = pure (),
              set = \_ row ->
                row
                  { jobStatus = lit Pending,
                    assignedTo = lit Nothing,
                    progress = lit Nothing,
                    errorMessage = lit Nothing
                  },
              updateWhere = \_ row -> jobId row ==. lit vid,
              returning = NoReturning
            }
  _ <- Hasql.run (Hasql.statement () (run_ stmt)) conn
  return ()

-- | Mark a job as completed, storing the HLS output chunk paths.
updateJobToCompleted :: Hasql.Connection -> EntityId Video -> [Text] -> IO ()
updateJobToCompleted conn vid chunks = do
  let stmt =
        update
          Update
            { target = videoJobTable,
              from = pure (),
              set = \_ row ->
                row
                  { jobStatus = lit Completed,
                    outputChunks = lit chunks,
                    errorMessage = lit Nothing
                  },
              updateWhere = \_ row -> jobId row ==. lit vid,
              returning = NoReturning
            }
  _ <- Hasql.run (Hasql.statement () (run_ stmt)) conn
  return ()

-- | Mark a job as failed, storing the error message.
updateJobToFailed :: Hasql.Connection -> EntityId Video -> Text -> IO ()
updateJobToFailed conn vid err = do
  let stmt =
        update
          Update
            { target = videoJobTable,
              from = pure (),
              set = \_ row ->
                row
                  { jobStatus = lit Failed,
                    errorMessage = lit (Just err)
                  },
              updateWhere = \_ row -> jobId row ==. lit vid,
              returning = NoReturning
            }
  _ <- Hasql.run (Hasql.statement () (run_ stmt)) conn
  return ()

-- | Update the progress of a 'Processing' job.
updateJobProgress :: Hasql.Connection -> EntityId Video -> Int32 -> IO ()
updateJobProgress conn vid prog = do
  let stmt =
        update
          Update
            { target = videoJobTable,
              from = pure (),
              set = \_ row ->
                row
                  { progress = lit (Just prog)
                  },
              updateWhere = \_ row -> jobId row ==. lit vid &&. jobStatus row ==. lit Processing,
              returning = NoReturning
            }
  _ <- Hasql.run (Hasql.statement () (run_ stmt)) conn
  return ()

-- | Revert stalled 'Processing' jobs back to 'Pending' and return their IDs and source URLs.
resetZombieJobs :: Hasql.Connection -> IO [(EntityId Video, Text)]
resetZombieJobs conn = do
  let sql =
        "UPDATE video_jobs \
        \SET status = 'PENDING', assigned_worker_id = NULL, progress_percent = NULL, error_message = NULL \
        \WHERE status = 'PROCESSING' AND updated_at < NOW() - INTERVAL '1 hour' \
        \RETURNING id, source_url"
      decoder =
        D.rowList $
          (,)
            <$> (EntityId <$> D.column (D.nonNullable D.uuid))
            <*> D.column (D.nonNullable D.text)
      stmt = Statement sql E.noParams decoder True
  result <- Hasql.run (Hasql.statement () stmt) conn
  case result of
    Left err -> putStrLn ("[ERROR] resetZombieJobs failed: " ++ show err) >> return []
    Right rows -> return rows

-- | Initialize the PostgreSQL trigger that sends NOTIFY on job status changes.
--   Idempotent — uses CREATE OR REPLACE, safe to call on every startup.
initJobStatusTrigger :: Hasql.Connection -> IO ()
initJobStatusTrigger conn = do
  let createFunctionSql =
        BS.concat
          [ "CREATE OR REPLACE FUNCTION notify_job_status_change() ",
            "RETURNS TRIGGER AS $$ ",
            "BEGIN ",
            "IF (TG_OP = 'INSERT') OR (OLD.status IS DISTINCT FROM NEW.status) OR (OLD.progress_percent IS DISTINCT FROM NEW.progress_percent) THEN ",
            "PERFORM pg_notify('job_status_changed', ",
            "json_build_object(",
            "'videoId', NEW.id::text, ",
            "'oldStatus', CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE OLD.status::text END, ",
            "'newStatus', NEW.status::text, ",
            "'outputChunks', COALESCE(to_json(NEW.output_chunks), '[]'::json), ",
            "'progress', NEW.progress_percent, ",
            "'errorMessage', NEW.error_message ",
            ")::text); ",
            "END IF; ",
            "RETURN NEW; ",
            "END; ",
            "$$ LANGUAGE plpgsql;"
          ]
      dropTriggerSql = "DROP TRIGGER IF EXISTS job_status_change_trigger ON video_jobs;" :: ByteString
      createTriggerSql =
        "CREATE TRIGGER job_status_change_trigger "
          <> "AFTER INSERT OR UPDATE OF status, progress_percent "
          <> "ON video_jobs "
          <> "FOR EACH ROW "
          <> "EXECUTE FUNCTION notify_job_status_change();" ::
          ByteString

  _ <- Hasql.run (Hasql.sql createFunctionSql) conn
  _ <- Hasql.run (Hasql.sql dropTriggerSql) conn
  _ <- Hasql.run (Hasql.sql createTriggerSql) conn
  putStrLn "[INFO] PostgreSQL trigger 'job_status_change_trigger' initialized."
  return ()
