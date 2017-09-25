package com.github.viyadb.spark.samples.wikimedia

import com.github.viyadb.spark.Configs.{JobConf, TableConf}
import com.github.viyadb.spark.streaming.StreamingProcess
import com.github.viyadb.spark.streaming.parser.Record
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.irc.IRCInputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.pircbotx.hooks.events.MessageEvent

class WikimediaProcess(config: JobConf) extends StreamingProcess(config) {

  @transient
  private val messageHandler = (m: MessageEvent) => recordParser.parseRecord(m.getChannel.getName, m.getMessage)

  override protected def saveDataFrame(tableConf: TableConf, df: DataFrame, time: Time): Unit = {
    super.saveDataFrame(tableConf, df.coalesce(1), time)
  }

  override protected def createStream(ssc: StreamingContext): DStream[Record] = {
    val streams = Seq(
      "#en.wikisource", "#en.wikibooks", "#en.wikinews", "#en.wikiquote", "#en.wikipedia", "#wikidata.wikipedia"
    ).map(channel =>
      IRCInputDStream.create[Record](ssc,
        server = "irc.wikimedia.org",
        port = 6667,
        channels = Seq(channel),
        messageHandler = messageHandler
      )
    )
    ssc.union(streams)
  }
}
