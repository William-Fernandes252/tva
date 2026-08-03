{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

module Adapter.RabbitMQ where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, asks)
import Data.Aeson (encode, decode)
import qualified Data.ByteString.Lazy.Char8 as BL
import Network.AMQP
import qualified Data.Text as T

import Domain.Queue (MonadQueue(..))
import Domain.Event (SystemEvent(..))

-- | Configuration for the RabbitMQ adapter.
data RabbitConfig = RabbitConfig
  { rabbitConn :: Connection
  , rabbitChan :: Channel
  }

-- | Extract the RabbitConfig from the environment.
class HasRabbitConfig env where
  getRabbitConfig :: env -> RabbitConfig

-- | Initialize the RabbitMQ connection and channel.
initRabbitMQ :: IO RabbitConfig
initRabbitMQ = do
  -- Connect to the local Docker RabbitMQ instance
  conn <- openConnection "127.0.0.1" "/" "guest" "guest"
  chan <- openChannel conn

  -- Declare a Topic Exchange. This allows us to route messages like 
  -- "transcode.1080p" to specific queues, while other queues might listen 
  -- to "transcode.*" (all resolutions).
  declareExchange chan newExchange 
    { exchangeName = "video_exchange"
    , exchangeType = "topic"
    , exchangeDurable = True -- Survives broker restarts
    }

  return $ RabbitConfig conn chan

-- | Implement the MonadQueue port for our RabbitMQ adapter.
instance (MonadIO m, MonadReader env m, HasRabbitConfig env) => MonadQueue SystemEvent m where
  
  publish event = do
    config <- asks getRabbitConfig
    let chan = rabbitChan config
        payload = encode event 
        
        -- Pattern match to determine the correct routing key at runtime.
        -- Because SystemEvent is a closed ADT, the compiler warns us if we 
        -- forget to assign a routing key to a newly added event type.
        rKey = case event of
          VideoUploadedEvent _ _ -> "video.uploaded"
          TranscodeFinishedEvent _ _ -> "video.finished"
          TranscodeFailedEvent _ _ -> "video.failed"

        message = newMsg { msgBody = payload, msgDeliveryMode = Just Persistent }
    
    -- Publish to the exchange
    liftIO $ publishMsg chan "video_exchange" rKey message
    return ()

  consume handler = do
    -- The API Server doesn't consume messages, but the Worker will use this exact 
    -- same adapter. The worker will bind to a queue and use 'consumeMsgs'.
    liftIO $ putStrLn "Consume not implemented for this microservice."