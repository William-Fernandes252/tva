{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Adapter.S3 where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT, asks, runReaderT)
import Data.Text.Encoding (decodeUtf8)
import Data.Time (NominalDiffTime)
import Network.AWS (Env, newEnv, discover, runResourceT, send, configure)
import Network.AWS.Auth (Credentials(FromSession))
import Network.AWS.Endpoint (Endpoint(..))
import Network.AWS.S3.PresignedUrl (presignURL)
import Network.AWS.S3.PutObject (putObject)
import Network.AWS.S3.Types (BucketName(..), ObjectKey(..))
import qualified Network.AWS.S3 as S3

import Domain.Storage (MonadStorage(..))
import Domain.Core (EntityId(..))
import Data.UUID (toText)

-- | The environment needed for our S3 Adapter
data S3Config = S3Config
  { s3Env    :: Env
  , s3Bucket :: BucketName
  }

-- | Our concrete application monad transformer stack
newtype AppM a = AppM { runAppM :: ReaderT S3Config IO a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader S3Config)

-- | The Adapter: Implementing the Storage port using Amazonka
instance MonadStorage AppM where
  generateUploadUrl (EntityId uuid) ttl = do
    env <- asks s3Env
    bucket <- asks s3Bucket
    
    -- In S3, the "Key" is the file path. We use the UUID as the filename.
    let objectKey = ObjectKey (toText uuid <> ".mkv")
        
        -- We construct a PutObject request
        putReq = putObject bucket objectKey "" 
    
    -- amazonka handles the complex SigV4 cryptographic signing
    liftIO $ do
      url <- runResourceT $ presignURL env putReq ttl
      return $ decodeUtf8 url

-- | Helper to initialize MinIO configuration on boot
initMinioEnv :: IO S3Config
initMinioEnv = do
  -- 'discover' finds credentials from the environment (or mock ones for MinIO)
  baseEnv <- newEnv discover
  
  -- We override the endpoint to point to our local Docker MinIO.
  -- To make this truly cloud-agnostic in production, you would conditionally
  -- apply this configuration based on an environment variable.
  let minioEndpoint = Endpoint "http" "localhost" 9000
      customEnv = configure (S3.s3 & S3.endpoint .~ pure minioEndpoint) baseEnv
      
  return $ S3Config customEnv (BucketName "videos")