{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Adapters.S3 where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Crypto.Hash.Algorithms (SHA256)
import Crypto.Hash (Digest, hash)
import Crypto.MAC.HMAC (HMAC, hmac, hmacGetDigest)
import Data.ByteArray (convert)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Char8 as B8
import Data.Maybe (fromMaybe)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Data.Time (NominalDiffTime, UTCTime, diffUTCTime, addUTCTime, getCurrentTime)
import Data.Time.Format (formatTime, defaultTimeLocale)
import Domain.Core (EntityId (..))
import Domain.Storage (MonadStorage (..))
import Data.UUID (toText)
import System.Environment (lookupEnv)

-- | Configuration for the S3/MinIO adapter.
data S3Config = S3Config
  { s3AccessKey :: BS.ByteString
  , s3SecretKey :: BS.ByteString
  , s3Region    :: BS.ByteString
  , s3Endpoint  :: BS.ByteString  -- ^ "host:port" (no scheme)
  , s3Secure    :: Bool           -- ^ True = https, False = http
  , s3Bucket    :: BS.ByteString
  }

-- | Our concrete application monad transformer stack.
newtype AppM a = AppM { runAppM :: ReaderT S3Config IO a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader S3Config)

-- | The Adapter: Implementing the Storage port using SigV4 presigned URLs.
instance MonadStorage AppM where
  generateUploadUrl (EntityId uuid) ttl = do
    cfg <- asks id
    let objectKey = encodeUtf8 (toText uuid <> ".mkv")
        ttlSecs   = toSeconds ttl

    now <- liftIO getCurrentTime
    let expiryTime = addUTCTime (fromIntegral ttlSecs) now
        url        = buildPresignedUrl cfg objectKey now expiryTime

    return $ decodeUtf8 url

-- | Convert NominalDiffTime to integer seconds.
toSeconds :: NominalDiffTime -> Int
toSeconds = floor

-- | Initialize the S3/MinIO configuration from environment variables.
initMinioEnv :: IO S3Config
initMinioEnv = do
  endpointStr <- fromMaybe "http://localhost:9000" <$> lookupEnv "MINIO_ENDPOINT"
  bucketStr   <- fromMaybe "videos"               <$> lookupEnv "MINIO_BUCKET"
  accessKey   <- fromMaybe "minio_admin"          <$> lookupEnv "MINIO_ACCESS_KEY"
  secretKey   <- fromMaybe "minio_password"       <$> lookupEnv "MINIO_SECRET_KEY"
  region      <- fromMaybe "us-east-1"            <$> lookupEnv "MINIO_REGION"

  let (schemeStr, rest) = break (== ':') endpointStr
      authority = case rest of
        ('/':'/':hp) -> hp
        _            -> rest
      secure = schemeStr == "https"

  return $ S3Config
    { s3AccessKey = B8.pack accessKey
    , s3SecretKey = B8.pack secretKey
    , s3Region    = B8.pack region
    , s3Endpoint  = B8.pack authority
    , s3Secure    = secure
    , s3Bucket    = B8.pack bucketStr
    }

----------------------------------------------------------------------
-- SigV4 Presigned URL Implementation
----------------------------------------------------------------------

-- | Build an AWS SigV4 presigned URL for a PUT object request.
--
--   Implements the AWS Signature Version 4 signing process for
--   query parameter-based presigned URLs (as used by S3/MinIO).
buildPresignedUrl
  :: S3Config
  -> BS.ByteString  -- ^ Object key (S3 path)
  -> UTCTime        -- ^ Request time (now)
  -> UTCTime        -- ^ Expiry time
  -> BS.ByteString  -- ^ The complete presigned URL
buildPresignedUrl cfg objectKey now expiry = mconcat
  [ scheme, "://", s3Endpoint cfg, "/", s3Bucket cfg, "/", objectKey
  , "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
  , "&X-Amz-Credential=", credential
  , "&X-Amz-Date=", amzDateTime
  , "&X-Amz-Expires=", B8.pack (show expiresSecs)
  , "&X-Amz-SignedHeaders=host"
  , "&X-Amz-Signature=", signatureHex
  ]
  where
    scheme      = if s3Secure cfg then "https" else "http"
    dateStamp   = B8.pack $ formatTime defaultTimeLocale "%Y%m%d" now
    amzDateTime = B8.pack $ formatTime defaultTimeLocale "%Y%m%dT%H%M%SZ" now
    expiresSecs :: Int
    expiresSecs = floor (diffUTCTime expiry now)

    -- Credential scope: <date>/<region>/s3/aws4_request
    credentialScope = dateStamp <> "/" <> s3Region cfg <> "/s3/aws4_request"
    credential      = s3AccessKey cfg <> "/" <> credentialScope

    -- Host header value (host:port, no scheme)
    hostHeader = s3Endpoint cfg

    -- Canonical Request (for query parameter presigned URLs)
    -- Format:
    --   HTTP_VERB\n
    --   CanonicalURI\n
    --   CanonicalQueryString\n
    --   CanonicalHeaders\n
    --   SignedHeaders\n
    --   PayloadHash
    canonicalRequest = mconcat
      [ "PUT\n"
      , "/", s3Bucket cfg, "/", urlEncodePath objectKey, "\n"
      , canonicalQueryString, "\n"
      , "host:", hostHeader, "\n"
      , "\n"
      , "host\n"
      , "UNSIGNED-PAYLOAD"
      ]

    -- Sorted query parameters for the canonical request
    canonicalQueryString = mconcat
      [ "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
      , "X-Amz-Credential=", urlEncode credential, "&"
      , "X-Amz-Date=", amzDateTime, "&"
      , "X-Amz-Expires=", B8.pack (show expiresSecs), "&"
      , "X-Amz-SignedHeaders=host"
      ]

    -- String to Sign
    stringToSign = mconcat
      [ "AWS4-HMAC-SHA256\n"
      , amzDateTime, "\n"
      , credentialScope, "\n"
      , hexHash canonicalRequest
      ]

    -- Derive the signing key:
    --   kDate     = HMAC("AWS4" + secret, date)
    --   kRegion   = HMAC(kDate, region)
    --   kService  = HMAC(kRegion, "s3")
    --   kSigning  = HMAC(kService, "aws4_request")
    signingKey = foldl hmacStep (B8.pack "AWS4" <> s3SecretKey cfg)
      [ dateStamp
      , s3Region cfg
      , "s3"
      , "aws4_request"
      ]

    hmacStep :: BS.ByteString -> BS.ByteString -> BS.ByteString
    hmacStep key msg = convert (hmacGetDigest (hmac key msg :: HMAC SHA256))

    -- Final signature = HMAC(signingKey, stringToSign)
    sigDigest    = hmacGetDigest (hmac signingKey stringToSign :: HMAC SHA256)
    signatureHex = Base16.encode (convert sigDigest :: BS.ByteString)

-- | Hex-encode a SHA256 hash of the input.
hexHash :: BS.ByteString -> BS.ByteString
hexHash input =
  let digest = hash input :: Digest SHA256
  in Base16.encode (convert digest :: BS.ByteString)

-- | Minimal URL encoding for AWS SigV4 query parameters.
--   Encodes '/' as '%2F' as required for the credential scope.
urlEncode :: BS.ByteString -> BS.ByteString
urlEncode = B8.pack . concatMap encodeChar . B8.unpack
  where
    encodeChar '/' = "%2F"
    encodeChar c   = [c]

-- | URL-encode the path segment.
--   S3 object keys may contain special characters that need URL percent-encoding.
urlEncodePath :: BS.ByteString -> BS.ByteString
urlEncodePath = BS.concatMap encodeByte
  where
    encodeByte w
      -- unreserved characters: A-Z a-z 0-9 - _ . ~
      | w >= 65 && w <= 90   = BS.singleton w
      | w >= 97 && w <= 122  = BS.singleton w
      | w >= 48 && w <= 57   = BS.singleton w
      | w == 45              = BS.singleton w   -- '-'
      | w == 95              = BS.singleton w   -- '_'
      | w == 46              = BS.singleton w   -- '.'
      | w == 126             = BS.singleton w   -- '~'
      | otherwise            = BS.cons 37 (Base16.encode (BS.singleton w))
                                  -- 37 is '%' in ASCII