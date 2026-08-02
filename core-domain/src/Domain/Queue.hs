{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Domain.Queue where

-- | Monad to abstract message queue operations, such as publishing and consuming events.
class Monad m => MonadQueue msg m | m -> msg where
  
  -- | Publishes an event to the message broker.
  publish :: msg -> m ()
  
  -- | Consumes events from the broker. (We will use this in the Worker on Day 9).
  consume :: (msg -> m ()) -> m ()