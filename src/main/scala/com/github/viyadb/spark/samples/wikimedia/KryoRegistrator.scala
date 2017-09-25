package com.github.viyadb.spark.samples.wikimedia

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.streaming.irc.IRCReceiver

class KryoRegistrator extends com.github.viyadb.spark.streaming.KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    super.registerClasses(kryo)
    kryo.register(classOf[IRCReceiver[_]])
    kryo.register(classOf[WikimediaProcess])
    kryo.register(Class.forName("com.github.viyadb.spark.samples.wikimedia.WikimediaProcess$$anonfun$1"))
  }
}
