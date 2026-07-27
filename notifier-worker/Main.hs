module Main (main) where

import qualified Lib

main :: IO ()
main = putStrLn $ Lib.greet "Notifier Worker"
