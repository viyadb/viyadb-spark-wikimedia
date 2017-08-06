package org.apache.spark.streaming.irc

import java.nio.charset.Charset

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.ThreadUtils
import org.pircbotx.hooks.ListenerAdapter
import org.pircbotx.hooks.events.MessageEvent
import org.pircbotx.hooks.managers.ListenerManager
import org.pircbotx.{Configuration, PircBotX}

import scala.util.{Random, Try}

class IRCReceiver[T](storageLevel: StorageLevel,
                     server: String, port: Int, channels: Seq[String],
                     messageHandler: MessageEvent => Option[T]
                    ) extends Receiver[T](storageLevel) with Logging {

  val nick = s"viyadb-${Random.nextLong()}"

  @transient
  lazy val botX: PircBotX = createBot()

  protected def createBot(): PircBotX = {
    val config = new Configuration.Builder
    config.addServer(server, port)
    config.setName(nick)
    config.setLogin(nick)
    config.setRealName(nick)
    config.setAutoNickChange(true)
    config.getListenerManager[ListenerManager]().addListener(new ListenerAdapter {
      override def onMessage(event: MessageEvent): Unit = {
        messageHandler(event).foreach(store)
      }
    })
    config.setEncoding(Charset.forName("UTF-8"))
    channels.foreach(config.addAutoJoinChannel)
    config.setAutoReconnect(true)
    new PircBotX(config.buildConfiguration)
  }

  override def onStart(): Unit = {
    val executor = ThreadUtils.newDaemonFixedThreadPool(1, "IrcReceiver")
    executor.submit(new BotWorker())
    executor.shutdown()
  }

  override def onStop(): Unit = {
    botX.stopBotReconnect()
    Try {
      logInfo("Sending QUIT command to IRC server")
      botX.sendIRC().quitServer()
    }.getOrElse((): Unit)
  }

  private class BotWorker extends Runnable {
    override def run(): Unit = {
      logInfo(s"Connecting to IRC server ${server}:${port} as @${nick}")
      botX.startBot()
      logInfo("Disconnected from IRC server")
    }
  }
}
