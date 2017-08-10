package com.github.viyadb.spark.samples.wikimedia

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.streaming.StreamSource
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.irc.IRCInputDStream
import org.pircbotx.hooks.events.MessageEvent

class WikimediaSource(config: JobConf) extends StreamSource(config) {

  @transient
  private val messageHandler = (m: MessageEvent) => messageFactory.createMessage(m.getChannel.getName, m.getMessage)

  override protected def saveDataFrame(df: DataFrame, time: Time): Unit = {
    super.saveDataFrame(df.coalesce(1), time)
  }

  override protected def createStream(ssc: StreamingContext): DStream[Row] = {
    val streams = Seq(
      "#en.wikisource", "#en.wikibooks", "#en.wikinews", "#en.wikiquote", "#en.wikipedia", "#wikidata.wikipedia"
    ).map(channel =>
      IRCInputDStream.create[Row](ssc,
        server = "irc.wikimedia.org",
        port = 6667,
        channels = Seq(channel),
        messageHandler = messageHandler
      )
    )
    ssc.union(streams)
  }
}
