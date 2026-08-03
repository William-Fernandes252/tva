{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Adapters.MockQueue where

import Control.Monad.State (State, MonadState, modify, runState)
import Domain.Queue (MonadQueue(..))
import Domain.Event (SystemEvent)

-- | A pure test monad that holds a list of emitted events in its state.
newtype TestAppM a = TestAppM { runTestApp :: State [SystemEvent] a }
  deriving (Functor, Applicative, Monad, MonadState [SystemEvent])

-- | Mocked implementation of the MonadQueue port for testing purposes.
instance MonadQueue SystemEvent TestAppM where
  
  -- Instead of hitting RabbitMQ, 'publish' just appends the event to our State list.
  publish msg = modify (\msgs -> msgs ++ [msg])
  
  -- For testing the API, we don't need 'consume'.
  consume _handler = return ()

-- | A helper to run a test and see what events were published.
-- Example usage in a test: 
--   let (_, emittedEvents) = executeTest testUploadHandler
executeTest :: TestAppM a -> (a, [SystemEvent])
executeTest app = runState (runTestApp app) []