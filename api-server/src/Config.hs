{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Config where

import Control.Exception (SomeException, try)
import Control.Monad.Except (ExceptT (..))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.String (fromString)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Servant

import Adapters.RabbitMQ (RabbitConfig, initRabbitMQ)
import Adapters.S3 (S3Config, initMinioEnv)
import Domain.Core (EntityId, Resolution, Video)
import Domain.Event (SystemEvent)
import Domain.Queue (MonadQueue (..))
import Domain.Storage (MonadStorage (..))

-- | Unified application configuration bundling all infrastructure configs.
data AppConfig = AppConfig
  { appS3Config      :: S3Config
  , appRabbitConfig  :: RabbitConfig
  }

-- | The concrete application monad stack, parameterized by 'AppConfig'.
newtype AppM a = AppM { runAppM :: ReaderT AppConfig IO a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader AppConfig)

-- | MonadStorage instance: delegates to the S3 adapter using the config extractor.
instance MonadStorage AppM where
  generateUploadUrl :: EntityId Video -> Resolution -> NominalDiffTime -> AppM Text
  generateUploadUrl videoId res ttl = do
    s3cfg <- asks appS3Config
    Adapters.S3.generateUploadUrl s3cfg videoId res ttl

-- | MonadQueue instance: delegates to the RabbitMQ adapter using the config extractor.
instance MonadQueue SystemEvent AppM where
  publish :: SystemEvent -> AppM ()
  publish event = do
    rcfg <- asks appRabbitConfig
    liftIO $ Adapters.RabbitMQ.publish rcfg event

  consume :: (SystemEvent -> AppM ()) -> AppM ()
  consume _handler = 
    liftIO $ putStrLn "Consume not implemented for api-server."

-- | Initialize all infrastructure and return a unified 'AppConfig'.
initAppConfig :: IO AppConfig
initAppConfig = do
  putStrLn "Initializing S3/MinIO connection..."
  s3cfg <- initMinioEnv
  putStrLn "Initializing RabbitMQ connection..."
  rabbitCfg <- initRabbitMQ
  return $ AppConfig
    { appS3Config     = s3cfg
    , appRabbitConfig = rabbitCfg
    }

-- | Natural transformation from 'AppM' to 'Handler'.
--   Catches IO exceptions and wraps them as HTTP 500 errors.
nt :: AppConfig -> AppM a -> Handler a
nt cfg action = Handler $ ExceptT $ do
  result <- try $ runReaderT (runAppM action) cfg
  case result of
    Left (e :: SomeException) ->
      pure $ Left $ err500 { errBody = fromString $ "Internal server error: " ++ show e }
    Right a ->
      pure $ Right a
