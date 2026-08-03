{-# LANGUAGE OverloadedStrings #-}

module Adapters.RabbitMQ where

import Data.Aeson (encode)
import Domain.Event (SystemEvent(..))
import Network.AMQP
import System.Environment (lookupEnv)
import Data.Maybe (fromMaybe)

-- | Configuration for the RabbitMQ adapter.
data RabbitConfig = RabbitConfig
  { rabbitConn :: Connection
  , rabbitChan :: Channel
  }

-- | Initialize the RabbitMQ connection and channel using environment variables.
initRabbitMQ :: IO RabbitConfig
initRabbitMQ = do
  host <- fromMaybe "127.0.0.1" <$> lookupEnv "RABBITMQ_HOST"
  user <- fromMaybe "mq_user"    <$> lookupEnv "RABBITMQ_USER"
  pass <- fromMaybe "mq_password" <$> lookupEnv "RABBITMQ_PASS"
  vhost <- fromMaybe "/"          <$> lookupEnv "RABBITMQ_VHOST"

  conn <- openConnection host vhost user pass
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

-- | Publish a 'SystemEvent' to the RabbitMQ exchange.
--   This is a standalone IO function, not tied to any monad stack.
publish :: RabbitConfig -> SystemEvent -> IO ()
publish config event = do
  let chan = rabbitChan config
      payload = encode event

      -- Pattern match to determine the correct routing key at runtime.
      rKey = case event of
        VideoUploadedEvent _ _ -> "video.uploaded"
        TranscodeFinishedEvent _ _ -> "video.finished"
        TranscodeFailedEvent _ _ -> "video.failed"

      message = newMsg
        { msgBody = payload
        , msgDeliveryMode = Just Persistent
        }

  publishMsg chan "video_exchange" rKey message