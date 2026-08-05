{-# LANGUAGE OverloadedStrings #-}

module Adapters.PostgreSQL where

import Data.Maybe (fromMaybe)
import Data.String (fromString)
import Hasql.Connection qualified as Hasql (Connection, acquire, settings)
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
          (fromIntegral pgPort)
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
