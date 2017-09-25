package com.github.viyadb.spark.samples.wikimedia

import java.sql.Timestamp

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.streaming.parser.{Record, RecordParser}
import org.apache.spark.internal.Logging

class WikimediaRecordParser(config: JobConf) extends RecordParser(config) with Logging {

  val Regex = """^.*\[\[(.+?)\]\].\s(.*)\s.*(https?://[^\s]+).*[*]\s(.*?)\s.*[*]\s\((.*?)\)(.*)$""".r

  override def parseRecord(channel: String, message: String): Option[Record] = {
    message.replaceAll("[^\\x20-\\x7E]", "") match {
      case Regex(title, flags, diffUrl, user, byteDiff, summary) =>
        Some(new Record(
          Array(channel, new Timestamp(System.currentTimeMillis()).asInstanceOf[Any],
            title, flags, diffUrl, user, byteDiff, summary)))
      case _ => None
    }
  }
}
