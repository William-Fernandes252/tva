module Domain.Transcoder where

import Data.Int (Int32)
import Domain.Core (Resolution)

-- | Monad to abstract video transcoding operations.
class Monad m => MonadTranscoder m where
  -- | Transcode a video file to HLS format.
  --   Takes:
  --     1. Input file path
  --     2. Output directory path
  --     3. Target resolution
  --     4. A callback function for progress updates (0-100)
  --   Returns:
  --     A list of relative file paths of the generated HLS segments and playlist.
  transcode :: FilePath -> FilePath -> Resolution -> (Int32 -> m ()) -> m [FilePath]
