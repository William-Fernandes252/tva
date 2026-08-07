{-# LANGUAGE OverloadedStrings #-}

module MinIO
  ( MinioConfig (..),
    initMinIO,
    downloadObject,
    uploadObject,
  )
where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Crypto.Hash (Digest, hash)
import Crypto.Hash.Algorithms (SHA256)
import Crypto.MAC.HMAC (HMAC, hmac, hmacGetDigest)
import Data.ByteArray (convert)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.ByteString.Char8 qualified as B8
import Data.ByteString.Lazy qualified as BL
import Data.CaseInsensitive (mk)
import Data.Maybe (fromMaybe)
import Data.Time (UTCTime, getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Network.HTTP.Client
  ( Request (method, requestBody, requestHeaders),
    RequestBody (RequestBodyLBS),
    httpLbs,
    newManager,
    parseRequest,
    responseBody,
    responseStatus,
  )
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Network.HTTP.Types (Header, statusIsSuccessful)
import System.Environment (lookupEnv)

data MinioConfig = MinioConfig
  { minioAccessKey :: BS.ByteString,
    minioSecretKey :: BS.ByteString,
    minioRegion :: BS.ByteString,
    minioEndpoint :: BS.ByteString,
    minioSecure :: Bool,
    minioBucket :: BS.ByteString
  }

initMinIO :: IO MinioConfig
initMinIO = do
  endpointStr <- fromMaybe "http://localhost:9000" <$> lookupEnv "MINIO_ENDPOINT"
  bucketStr <- fromMaybe "raw-videos" <$> lookupEnv "MINIO_BUCKET"
  accessKey <- fromMaybe "minio_admin" <$> lookupEnv "MINIO_ACCESS_KEY"
  secretKey <- fromMaybe "minio_password" <$> lookupEnv "MINIO_SECRET_KEY"
  region <- fromMaybe "us-east-1" <$> lookupEnv "MINIO_REGION"

  let (schemeStr, rest) = break (== ':') endpointStr
      authority = case rest of
        ('/' : '/' : hp) -> hp
        _ -> rest
      secure = schemeStr == "https"

  return $
    MinioConfig
      { minioAccessKey = B8.pack accessKey,
        minioSecretKey = B8.pack secretKey,
        minioRegion = B8.pack region,
        minioEndpoint = B8.pack authority,
        minioSecure = secure,
        minioBucket = B8.pack bucketStr
      }

----------------------------------------------------------------------
-- SigV4 Signing
----------------------------------------------------------------------

sigV4Headers ::
  MinioConfig ->
  BS.ByteString ->
  BS.ByteString ->
  BS.ByteString ->
  UTCTime ->
  [Header]
sigV4Headers cfg method uri payloadHash now =
  [ (mk "Host", minioEndpoint cfg),
    (mk "X-Amz-Date", amzDateTime),
    (mk "X-Amz-Content-SHA256", payloadHash),
    (mk "Authorization", authHeader)
  ]
  where
    dateStamp = B8.pack $ formatTime defaultTimeLocale "%Y%m%d" now
    amzDateTime = B8.pack $ formatTime defaultTimeLocale "%Y%m%dT%H%M%SZ" now
    credentialScope = dateStamp <> "/" <> minioRegion cfg <> "/s3/aws4_request"

    canonicalRequest =
      mconcat
        [ method,
          "\n",
          uri,
          "\n",
          "\n",
          "host:",
          minioEndpoint cfg,
          "\n",
          "\n",
          "host\n",
          payloadHash
        ]

    stringToSign =
      mconcat
        [ "AWS4-HMAC-SHA256\n",
          amzDateTime,
          "\n",
          credentialScope,
          "\n",
          hexHash canonicalRequest
        ]

    signingKey =
      foldl
        hmacStep
        (B8.pack "AWS4" <> minioSecretKey cfg)
        [dateStamp, minioRegion cfg, "s3", "aws4_request"]

    hmacStep key msg = convert (hmacGetDigest (hmac key msg :: HMAC SHA256))

    sigDigest = hmacGetDigest (hmac signingKey stringToSign :: HMAC SHA256)
    signatureHex = Base16.encode (convert sigDigest :: BS.ByteString)

    authHeader =
      "AWS4-HMAC-SHA256 Credential="
        <> minioAccessKey cfg
        <> "/"
        <> credentialScope
        <> ", SignedHeaders=host, Signature="
        <> signatureHex

hexHash :: BS.ByteString -> BS.ByteString
hexHash input =
  let digest = hash input :: Digest SHA256
   in Base16.encode (convert digest :: BS.ByteString)

downloadObject ::
  (MonadIO m) =>
  MinioConfig ->
  BS.ByteString ->
  m BL.ByteString
downloadObject cfg objectKey = liftIO $ do
  now <- getCurrentTime
  let scheme = if minioSecure cfg then "https" else "http"
      host = minioEndpoint cfg
      bucket = minioBucket cfg
      uri = "/" <> bucket <> "/" <> objectKey
      url = scheme <> "://" <> host <> uri
      payloadHash = hexHash BS.empty
      headers = sigV4Headers cfg "GET" uri payloadHash now

  request <- parseRequest (B8.unpack url)
  let signedReq =
        request
          { method = "GET",
            requestHeaders = headers
          }
  manager <- newManager tlsManagerSettings
  response <- httpLbs signedReq manager
  if statusIsSuccessful (responseStatus response)
    then return (responseBody response)
    else error $ "MinIO download failed: " <> show (responseStatus response)

uploadObject ::
  (MonadIO m) =>
  MinioConfig ->
  BS.ByteString ->
  FilePath ->
  m ()
uploadObject cfg objectKey filePath = liftIO $ do
  fileBytes <- BL.readFile filePath
  now <- getCurrentTime
  let scheme = if minioSecure cfg then "https" else "http"
      host = minioEndpoint cfg
      bucket = minioBucket cfg
      uri = "/" <> bucket <> "/" <> objectKey
      url = scheme <> "://" <> host <> uri
      payloadHash = hexHash (BL.toStrict fileBytes)
      headers = sigV4Headers cfg "PUT" uri payloadHash now

  request <- parseRequest (B8.unpack url)
  let signedReq =
        request
          { method = "PUT",
            requestHeaders = headers,
            requestBody = RequestBodyLBS fileBytes
          }
  manager <- newManager tlsManagerSettings
  response <- httpLbs signedReq manager
  if statusIsSuccessful (responseStatus response)
    then return ()
    else error $ "MinIO upload failed: " <> show (responseStatus response)
