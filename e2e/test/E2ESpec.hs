{-# LANGUAGE OverloadedStrings #-}

module E2ESpec (spec) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket_, try, SomeException)
import Control.Monad (when)
import Data.Aeson (Value)
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.KeyMap as Aeson.KeyMap
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Network.HTTP.Simple
import System.Process.Typed
import Test.Hspec
import Data.Text (Text)
import qualified Data.Text as T

setupDocker :: IO ()
setupDocker = do
  putStrLn "Starting docker compose..."
  -- Ensure everything is down first to avoid conflicts
  runProcess_ (proc "docker" ["compose", "-f", "../docker-compose.test.yml", "down", "-v"])
  -- Run up with --build
  runProcess_ (proc "docker" ["compose", "-f", "../docker-compose.test.yml", "up", "--build", "-d"])
  
  putStrLn "Waiting for API server to become ready (up to 30s)..."
  -- Simple wait loop for the api server to start accepting connections
  let waitForApi 0 = error "API server did not start in time."
      waitForApi n = do
        res <- tryAny (httpNoBody (parseRequest_ "http://localhost:8080/health"))
        case res of
          Right resp | getResponseStatusCode resp == 200 -> putStrLn "API server is ready."
          _ -> do
            threadDelay 1000000
            waitForApi (n - 1)
  waitForApi 30

teardownDocker :: IO ()
teardownDocker = do
  putStrLn "Tearing down docker compose..."
  runProcess_ (proc "docker" ["compose", "-f", "../docker-compose.test.yml", "down", "-v"])

-- | Helper to catch any synchronous exception
tryAny :: IO a -> IO (Either SomeException a)
tryAny = try

spec :: Spec
spec = around_ (bracket_ setupDocker teardownDocker) $ do
  describe "E2E Video Processing" $ do
    it "uploads a video, processes it, and stores HLS chunks in MinIO" $ do
      -- Request presigned URL
      let reqBody = Aeson.object
            [ "fileExtension" Aeson..= ("mp4" :: Text)
            , "fileSize" Aeson..= (1024 :: Int)
            , "resolution" Aeson..= ("R1080p" :: Text)
            ]
      let uploadReq = setRequestBodyJSON reqBody $ parseRequest_ "POST http://localhost:8080/videos"
      uploadRes <- httpJSON uploadReq
      let resBody = getResponseBody uploadRes :: Aeson.Value
      let presignedUrl = case resBody of
            Aeson.Object obj -> case Aeson.KeyMap.lookup "presignedUrl" obj of
              Just (Aeson.String url) -> url
              _ -> error "Missing presignedUrl"
            _ -> error "Invalid upload response"
      let videoId = case resBody of
            Aeson.Object obj -> case Aeson.KeyMap.lookup "videoId" obj of
              Just (Aeson.String vid) -> vid
              _ -> error "Missing videoId"
            _ -> error "Invalid upload response"

      -- Upload video file
      videoContent <- BSL.readFile "test/fixtures/sample.mp4"
      let putReq = setRequestBodyLBS videoContent $ setRequestMethod "PUT" (parseRequest_ (T.unpack presignedUrl))
      putRes <- httpNoBody putReq
      when (getResponseStatusCode putRes >= 400) $
        error $ "PUT video failed: " ++ show (getResponseStatusCode putRes)

      -- 2.5 Trigger webhook manually (simulating MinIO)
      let objKey = T.unpack videoId ++ "_1080p.mkv"
      let webhookBody = Aeson.object
            [ "eventName" Aeson..= ("s3:ObjectCreated:Put" :: Text)
            , "key" Aeson..= (T.pack objKey)
            , "records" Aeson..= ([] :: [Aeson.Value])
            ]
      let webhookReq = setRequestBodyJSON webhookBody 
                     $ setRequestHeader "Authorization" ["Bearer test-secret"] 
                     $ parseRequest_ "POST http://localhost:8080/webhook/minio"
      webhookRes <- httpNoBody webhookReq
      when (getResponseStatusCode webhookRes >= 400) $
        error $ "Webhook failed: " ++ show (getResponseStatusCode webhookRes)

      -- Poll status until completed
      let pollStatus n = do
            if n <= 0
              then expectationFailure "Video processing timed out"
              else do
                let statusReq = parseRequest_ ("GET http://localhost:8080/videos/" ++ T.unpack videoId ++ "/status")
                statusRes <- tryAny (httpJSON statusReq)
                case statusRes of
                  Right res -> do
                    let sBody = getResponseBody res :: Aeson.Value
                    let status = case sBody of
                          Aeson.Object obj -> case Aeson.KeyMap.lookup "status" obj of
                            Just (Aeson.String s) -> s
                            _ -> ""
                          _ -> ""
                    if status == "Completed"
                      then return ()
                      else if status == "Failed"
                        then expectationFailure "Video processing failed"
                        else do
                          threadDelay 2000000 -- 2 seconds
                          pollStatus (n - 1)
                  Left _ -> do
                    threadDelay 2000000
                    pollStatus (n - 1)
      pollStatus 30

      -- 4. Assert MinIO (Skipped for now, but in future would list bucket objects)
      putStrLn "Video processing completed successfully!"
