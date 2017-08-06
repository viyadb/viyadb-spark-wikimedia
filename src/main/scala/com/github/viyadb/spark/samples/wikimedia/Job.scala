package com.github.viyadb.spark.samples.wikimedia

import org.apache.spark.SparkConf

class Job extends com.github.viyadb.spark.streaming.Job {

  override protected def appName(): String = "ViyaDB Wikimedia Streaming"

  override protected def kryoRegistrator(): Class[_] = {
    classOf[KryoRegistrator]
  }

  override protected def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.streaming.blockInterval", "10s")
  }
}

object Job {
  def main(args: Array[String]): Unit = {
    new Job().run(args)
  }
}
