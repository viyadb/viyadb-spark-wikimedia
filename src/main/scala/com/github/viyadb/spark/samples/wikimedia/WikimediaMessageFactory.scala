package com.github.viyadb.spark.samples.wikimedia

import java.sql.Timestamp

import com.github.viyadb.spark.Configs.JobConf
import com.github.viyadb.spark.streaming.message.{Message, MessageFactory}
import org.apache.spark.sql.Row

class WikimediaMessageFactory(config: JobConf) extends MessageFactory(config) {

  val Regex = """^.*\[\[(.+?)\]\].\s(.*)\s.*(https?://[^\s]+).*[*]\s(.*?)\s.*[*]\s\((.*?)\)(.*)$""".r

  override def createMessage(channel: String, message: String): Option[Row] = {
    message.replaceAll("[^\\x20-\\x7E]", "") match {
      case Regex(title, flags, diffUrl, user, byteDiff, summary) => Some(
        new Message(
          Array(channel, new Timestamp(System.currentTimeMillis()).asInstanceOf[Any],
            title, flags, diffUrl, user, byteDiff, summary)))
      case _ => None
    }
  }
}
