package com.sap.kafka.consumer

import java.util

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException


import akka.actor.{ActorRef, Actor, ActorSystem, Props}

import scala.collection.JavaConverters._

object KafkaAKKAConsumer {

  def main(args: Array[String]): Unit = {

    val numConsumers = 5

    val actorSytem = ActorSystem("KafkaAkkaSystem")

    val actorList = new java.util.ArrayList[ActorRef]()
    for (x <- 1 to numConsumers) {
      val akkaActor = actorSytem.actorOf(Props[KafkaActor])
      actorList.add(akkaActor)
      akkaActor ! "start"
    }
  }
}

class KafkaActor extends Actor {

  val kafkaTopic = "topic-with-3-partitions"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host
  val props = new java.util.HashMap[String, Object]()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("schema.registry.url", "http://10.97.136.161:8081")
  props.put("group.id", "testgroup1")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")

  val consumer = new KafkaConsumer[String,String](props)

  def receive = {
    case _ =>
    println(s"""Run Method invoked for Thread : ${Thread.currentThread().getId}""")
    try{
      consumer.subscribe(util.Arrays.asList(kafkaTopic),new ConsumerRebalanceListener(){
        override def onPartitionsAssigned (partitions: util.Collection[TopicPartition]) : Unit = {
          println(s"Partition Assigned currently to this Thread with ID:" +
            s" ${Thread.currentThread().getId}  are ${util.Arrays.toString(partitions.toArray)}")
        }
        override def  onPartitionsRevoked (partitions: util.Collection[TopicPartition]) : Unit = {

          println(s"Partition Revoked currently to this Thread with ID ${Thread.currentThread().getId}" +
            s" are ${util.Arrays.toString(partitions.toArray)}")
        }
      })
      while(true) {
        val records: ConsumerRecords[String, String] = consumer.poll(100)
        val recordIterator: java.util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        while(recordIterator.hasNext)
        {
          val currentRecord :ConsumerRecord[String,String] = recordIterator.next()
          // println(s"""Thread ID: ${Thread.currentThread().getId} and Partition : ${currentRecord.partition()}""")
        }
      }
    }
    catch {
      case ex: WakeupException =>
        println( s"""WakeupException : ${ex.printStackTrace()}""")
      // Do Nothing
      case e: Exception => e.printStackTrace()
    }
    finally {
      println("Finally Called")
      consumer.close()
    }
  }
}
