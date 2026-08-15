{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Config where

import Adapters.PostgreSQL (initPostgreSQL)
import Adapters.RabbitMQ (RabbitConfig, initRabbitMQ, publish)
import Adapters.S3 (S3Config, generateUploadUrl, initMinioEnv)
import Control.Exception (SomeException, try)
import Control.Monad.Except (ExceptT (..), MonadError, runExceptT)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT, asks, local, runReaderT)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Maybe (fromMaybe)
import System.Environment (lookupEnv)
import Data.Time (NominalDiffTime)
import Domain.Core (EntityId, Resolution, Video, Worker)
import Domain.Database (MonadDatabase (..), findVideoJobById', insertPendingJob', updateJobToCompleted', updateJobToFailed', updateJobToPending', updateJobToProcessing', updateJobProgress')
import Domain.Event (SystemEvent)
import Domain.Queue (MonadQueue (..))
import Domain.Storage (MonadStorage (..))
import Domain.Logger
import Hasql.Connection qualified as Hasql (Connection)
import Servant

-- | Unified application configuration bundling all infrastructure configs.
data AppConfig = AppConfig
  { appS3Config :: S3Config,
    appRabbitConfig :: RabbitConfig,
    appDbConn :: Hasql.Connection,
    appWebhookSecret :: Text,
    appLogEnv :: LogEnv,
    appKatipContext :: LogContexts,
    appKatipNamespace :: Namespace
  }

-- | The concrete application monad stack, parameterized by 'AppConfig'.
newtype AppM a = AppM {runAppM :: ReaderT AppConfig (ExceptT ServerError IO) a}
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader AppConfig, MonadError ServerError)

instance Katip AppM where
  getLogEnv = asks appLogEnv
  localLogEnv f (AppM m) = AppM (local (\c -> c {appLogEnv = f (appLogEnv c)}) m)

instance KatipContext AppM where
  getKatipContext = asks appKatipContext
  localKatipContext f (AppM m) = AppM (local (\c -> c {appKatipContext = f (appKatipContext c)}) m)
  getKatipNamespace = asks appKatipNamespace
  localKatipNamespace f (AppM m) = AppM (local (\c -> c {appKatipNamespace = f (appKatipNamespace c)}) m)

-- | MonadStorage instance: delegates to the S3 adapter using the config extractor.
instance MonadStorage AppM where
  generateUploadUrl :: EntityId Video -> Resolution -> NominalDiffTime -> AppM Text
  generateUploadUrl videoId res ttl = do
    s3cfg <- asks appS3Config
    Adapters.S3.generateUploadUrl s3cfg videoId res ttl
    
  downloadBlob _bucket _key _dest = error "downloadBlob not implemented in API server"
  uploadBlob _bucket _key _src = error "uploadBlob not implemented in API server"

-- | MonadQueue instance: delegates to the RabbitMQ adapter using the config extractor.
instance MonadQueue SystemEvent AppM where
  publish :: SystemEvent -> AppM ()
  publish event = do
    rcfg <- asks appRabbitConfig
    liftIO $ Adapters.RabbitMQ.publish rcfg event

  consume :: (SystemEvent -> AppM ()) -> AppM ()
  consume _handler =
    $(logTM) WarningS "Consume not implemented for api-server."

-- | MonadDatabase instance: delegates to the standalone functions in Domain.Database.
instance MonadDatabase AppM where
  insertPendingJob vid source = do
    conn <- asks appDbConn
    liftIO $ Domain.Database.insertPendingJob' conn vid source

  findVideoJobById vid = do
    conn <- asks appDbConn
    liftIO $ Domain.Database.findVideoJobById' conn vid

  updateJobToProcessing vid worker = do
    conn <- asks appDbConn
    liftIO $ Domain.Database.updateJobToProcessing' conn vid worker

  updateJobToPending vid = do
    conn <- asks appDbConn
    liftIO $ Domain.Database.updateJobToPending' conn vid

  updateJobToCompleted vid chunks = do
    conn <- asks appDbConn
    liftIO $ Domain.Database.updateJobToCompleted' conn vid chunks

  updateJobToFailed vid err = do
    conn <- asks appDbConn
    liftIO $ Domain.Database.updateJobToFailed' conn vid err

  updateJobProgress vid prog = do
    conn <- asks appDbConn
    liftIO $ Domain.Database.updateJobProgress' conn vid prog

-- | Initialize all infrastructure and return a unified 'AppConfig'.
initAppConfig :: IO AppConfig
initAppConfig = do
  logEnv <- setupLogEnv "api-server" "development"
  runKatipContextT logEnv (mempty :: LogContexts) "startup" $ do
    dbConn <- liftIO initPostgreSQL

    secretStr <- liftIO $ fromMaybe "secret123" <$> lookupEnv "WEBHOOK_SECRET"

    $(logTM) InfoS "Initializing S3/MinIO connection..."
    s3cfg <- liftIO initMinioEnv
    $(logTM) InfoS "Initializing RabbitMQ connection..."
    rabbitCfg <- liftIO initRabbitMQ
    return $
      AppConfig
        { appS3Config = s3cfg,
          appRabbitConfig = rabbitCfg,
          appDbConn = dbConn,
          appWebhookSecret = T.pack secretStr,
          appLogEnv = logEnv,
          appKatipContext = mempty :: LogContexts,
          appKatipNamespace = "api-server"
        }

-- | Natural transformation from 'AppM' to 'Handler'.
--   Catches IO exceptions and wraps them as HTTP 500 errors.
nt :: AppConfig -> AppM a -> Handler a
nt cfg action = Handler $ ExceptT $ do
  result <- try $ runExceptT $ runReaderT (runAppM action) cfg
  case result of
    Left (e :: SomeException) ->
      pure $ Left $ err500 {errBody = fromString $ "Internal server error: " ++ show e}
    Right (Left e) -> pure $ Left e
    Right (Right a) -> pure $ Right a
