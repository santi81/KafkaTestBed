package com.sap.kafka.partitioner

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster


class SimplePartitioner extends Partitioner{


  override def configure (configs: java.util.Map[String, _]) : Unit = {


    println("configure method called")
  }

  override def close: Unit = {

    println("Close method called")

  }

  override def partition(topic: String, key: AnyRef, keyBytes: Array[Byte],
                         value: AnyRef, valueBytes: Array[Byte], cluster: Cluster) : Int = {

    //println(s"Topic Name : $topic")
    //println(s"Key is $key.toString")
    val r = scala.util.Random
    r.nextInt(3)
  }

}
