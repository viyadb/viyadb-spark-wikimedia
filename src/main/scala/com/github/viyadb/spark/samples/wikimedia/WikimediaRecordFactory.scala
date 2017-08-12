package com.github.viyadb.spark.samples.wikimedia

import java.sql.Timestamp

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.record.Record
import com.github.viyadb.spark.streaming.record.RecordFactory
import org.apache.spark.sql.Row

class WikimediaRecordFactory(config: JobConf) extends RecordFactory(config) {

  val Regex = """^.*\[\[(.+?)\]\].\s(.*)\s.*(https?://[^\s]+).*[*]\s(.*?)\s.*[*]\s\((.*?)\)(.*)$""".r

  override def createRecord(channel: String, message: String): Option[Row] = {
    message.replaceAll("[^\\x20-\\x7E]", "") match {
      case Regex(title, flags, diffUrl, user, byteDiff, summary) => Some(
        new Record(
          Array(channel, new Timestamp(System.currentTimeMillis()).asInstanceOf[Any],
            title, flags, diffUrl, user, byteDiff, summary)))
      case _ => None
    }
  }
}
