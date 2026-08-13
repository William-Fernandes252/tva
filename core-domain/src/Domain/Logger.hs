{-# LANGUAGE OverloadedStrings #-}

module Domain.Logger
  ( module Katip,
    setupLogEnv,
  )
where

import Katip
import System.IO (stdout)

-- | Initializes a Katip LogEnv with a JSON scribe writing to stdout.
setupLogEnv :: Namespace -> Environment -> IO LogEnv
setupLogEnv ns env = do
  handleScribe <- mkHandleScribeWithFormatter jsonFormat ColorIfTerminal stdout (permitItem InfoS) V3
  le <- Katip.initLogEnv ns env
  registerScribe "stdout" handleScribe defaultScribeSettings le
