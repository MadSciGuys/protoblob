{-|
Module      : Data.ProtoBlob
Description : Storable Message Frames for Protocol Buffers
Copyright   : Travis Whitaker 2015
License     : MIT
Maintainer  : twhitak@its.jnj.com
Stability   : Provisional
Portability : POSIX

ProtoBlob provides a simple length-encoded message frame for protocol buffer
payloads. This allows sequences of messages to be simply retrieved from a file
on disk, or transmitted by any means that does not preserve message length or
otherwise delimit messages.

32 bit unsigned integers are used for length encoding, so individual payloads
must not exceed 2^32 bytes.
-}

{-# LANGUAGE DeriveDataTypeable #-}

module Data.ProtoBlob (
    -- * Serialization
    PutBlob
  , lenMessagePutM
  , lenMessageUnsafePutM
  , runPutBlob
    -- * Deserialization
  , GetBlob
  , lenMessageGetM
  , runGetBlob
  ) where

import Control.Monad (ap)

import Control.Exception ( Exception
                         , throw
                         )

import Data.Typeable (Typeable)

import Text.ProtocolBuffers ( ReflectDescriptor
                            , Wire
                            , messageSize
                            , messagePutM
                            , messageGetM
                            , runGetOnLazy
                            )

import Text.ProtocolBuffers.Get ( Get
                                , Result(..)
                                , getWord32le
                                , getLazyByteString
                                , runGetAll
                                )

import Data.Binary.Put ( Put
                       , putWord32le
                       , runPut
                       )

import Data.ByteString.Lazy (ByteString)

-- | 'Text.ProtocolBuffers' uses the 'Put' monad from 'Data.Binary', so this is
--   just a type synonym.
type PutBlob = Put

-- | Exception thrown when the frame payload is too large.
data ProtoBlobFrameError = ProtoBlobFrameError deriving (Show, Typeable)

instance Exception ProtoBlobFrameError

-- | Create a 'PutBlob' that serializes the provided payload within a ProtoBlob
--   frame. This function checks the length of the payload; if you can guarantee
--   that your payloads are less than 2^32 bytes use 'lenMessageUnsafePutM'
--   instead (you should try to do that).
lenMessagePutM :: (ReflectDescriptor msg, Wire msg) => msg -> PutBlob
lenMessagePutM p = if messageSize p >= 4294967295
                   then throw ProtoBlobFrameError
                   else lenMessageUnsafePutM p

-- | Create a 'PutBlob' that serializes the provided payload within a ProtoBlob
--   frame. This function does not check the length of the payload; if the
--   payload is larger than 2^32 bytes an invalid frame will be generated.
lenMessageUnsafePutM :: (ReflectDescriptor msg, Wire msg) => msg -> PutBlob
lenMessageUnsafePutM = ap ((>>) . putWord32le . fromIntegral . messageSize) messagePutM

runPutBlob :: PutBlob -> ByteString
runPutBlob = runPut

-- | 'Text.ProtocolBuffers' uses its own 'Get' monad implementation, defined in
--   'Text.ProtocolBuffers.Get'. This is a type synonym for convenience.
type GetBlob = Get

-- | Create a 'GetBlob' that deserializes a single message of type 'msg'. This
--   works by nesting the 'Get' monad. Failures in the inner monad are
--   propagated to the outer level with 'fail'.
lenMessageGetM :: (ReflectDescriptor msg, Wire msg) => GetBlob msg
lenMessageGetM = getWord32le           >>=
    (getLazyByteString . fromIntegral) >>=
    (checkResult . runGetAll messageGetM)
    where checkResult (Finished _ _ x) = return x
          checkResult (Failed _ e)     = fail e
          checkResult _                = fail "sub runGetAll returned 'Partial'"

runGetBlob :: GetBlob a -> ByteString -> Either String a
runGetBlob g l = case runGetOnLazy g l of (Left e)       -> Left e
                                          (Right (x, _)) -> Right x
