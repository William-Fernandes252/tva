{-# LANGUAGE OverloadedStrings #-}

module Adapters.FFmpeg (runFFmpeg) where

import Control.Monad (when)
import Data.ByteString.Char8 qualified as B8
import Data.ByteString.Lazy qualified as BL
import Data.Int (Int32)
import Data.Maybe (fromMaybe)
import Data.Text qualified as T
import Domain.Core (Resolution (..))
import System.Directory (listDirectory)

import System.FilePath (takeExtension, (</>))
import System.IO (hGetLine, hIsEOF)
import System.Process.Typed (checkExitCode, createPipe, getStdout, proc, readProcessStdout_, setStdout, withProcessWait)
import Text.Read (readMaybe)

resolutionToScale :: Resolution -> T.Text
resolutionToScale R1080p = "scale=1920:1080"
resolutionToScale R720p = "scale=1280:720"
resolutionToScale R480p = "scale=854:480"

-- | Parse an integer from a string, returning Nothing on failure.
readMaybe' :: (Read a) => String -> Maybe a
readMaybe' s = case reads s of
  [(x, "")] -> Just x
  _ -> Nothing

runFFmpeg :: FilePath -> FilePath -> Resolution -> (Int32 -> IO ()) -> IO [FilePath]
runFFmpeg inputFile outputDir res onProgress = do
  -- Get the exact duration in seconds using ffprobe
  let probeCmd = proc "ffprobe" ["-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", inputFile]
  probeOut <- readProcessStdout_ probeCmd
  let durationStr = T.strip $ T.pack $ B8.unpack $ BL.toStrict probeOut
      totalDurationSeconds = fromMaybe 0.0 (readMaybe' (T.unpack durationStr) :: Maybe Double)
      totalDurationMs = round (totalDurationSeconds * 1000000) :: Integer

  let scale = resolutionToScale res
      segmentPattern = outputDir </> "segment_%03d.ts"
      outputPlaylist = outputDir </> "output.m3u8"
      ffmpegProc =
        setStdout createPipe $
          proc
            "ffmpeg"
            [ "-i", inputFile,
              "-c:v", "libx264",
              "-preset", "fast",
              "-crf", "23",
              "-force_key_frames", "expr:gte(t,n_forced*2)",
              "-c:a", "aac",
              "-b:a", "128k",
              "-vf", T.unpack scale,
              "-f", "hls",
              "-hls_time", "6",
              "-hls_list_size", "0",
              "-hls_segment_type", "mpegts",
              "-hls_segment_filename", segmentPattern,
              "-progress", "pipe:1",
              "-nostats",
              outputPlaylist
            ]

  withProcessWait ffmpegProc $ \p -> do
    let outH = getStdout p
        loop lastPct = do
          eof <- hIsEOF outH
          if eof then return () else do
            line <- hGetLine outH
            let tline = T.pack line
            case T.stripPrefix "out_time_us=" tline of
              Just usStr -> do
                let outTimeUs = fromMaybe 0 (readMaybe' (T.unpack usStr) :: Maybe Integer)
                    pct :: Int32
                    pct = if totalDurationMs > 0
                            then fromIntegral (min 100 ((outTimeUs * 100) `div` totalDurationMs))
                            else 0
                if pct > lastPct
                  then do
                    onProgress pct
                    loop pct
                  else loop lastPct
              Nothing -> loop lastPct
        
    loop 0
    checkExitCode p

  files <- listDirectory outputDir
  let outputFiles = filter (\f -> takeExtension f `elem` [".m3u8", ".ts"]) files
  return outputFiles
