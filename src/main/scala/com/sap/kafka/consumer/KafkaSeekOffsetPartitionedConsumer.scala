package com.sap.kafka.consumer

import java.util

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

object KafkaSeekOffsetPartitionedConsumer {


  val kafkaTopic = "attendee00-partitioned-topic"    // command separated list of topics
  val kafkaBrokers = "10.97.183.115:9092,10.97.191.51:9092,10.97.152.59:9092,10.97.152.66:9092"
  // comma separated list of broker:host

  def main(args: Array[String]): Unit = {

    val props = new java.util.HashMap[String, Object]()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "testgroup1")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")

    val consumer = new KafkaConsumer[String,String](props)
    val topicPartition = new TopicPartition(kafkaTopic,0)
    consumer.assign(util.Arrays.asList(topicPartition))
    consumer.seekToBeginning(util.Arrays.asList(topicPartition))
    try{
      while(true) {
        val records: ConsumerRecords[String, String] = consumer.poll(100)
        val recordIterator: java.util.Iterator[ConsumerRecord[String, String]] = records.iterator()

        while(recordIterator.hasNext)
        {
          val currentRecord :ConsumerRecord[String,String] = recordIterator.next()
          println(currentRecord.offset())
        }
      }
    }

  }

  }
