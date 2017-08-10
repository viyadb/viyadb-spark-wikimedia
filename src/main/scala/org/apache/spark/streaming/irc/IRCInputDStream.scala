package org.apache.spark.streaming.irc

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.pircbotx.hooks.events.MessageEvent

import scala.reflect.ClassTag

class IRCInputDStream[T: ClassTag](_ssc: StreamingContext,
                                   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER,
                                   server: String, port: Int, channels: Seq[String],
                                   messageHandler: MessageEvent => Option[T]
                                  ) extends ReceiverInputDStream[T](_ssc) with Logging {

  override def getReceiver(): Receiver[T] = {
    new IRCReceiver[T](storageLevel, server, port, channels, messageHandler)
  }
}

object IRCInputDStream {
  def create[T: ClassTag](ssc: StreamingContext,
                          storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER,
                          server: String, port: Int, channels: Seq[String],
                          messageHandler: MessageEvent => Option[T]
                         ): IRCInputDStream[T] = {
    new IRCInputDStream[T](ssc, storageLevel, server, port, channels, messageHandler)
  }
}
