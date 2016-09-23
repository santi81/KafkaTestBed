package com.sap.kafka.consumer

import java.util

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._

object KafkaConsumer_ManualOffset {


  val kafkaTopic = "topic-with-3-partitions"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host
  val schemaRegistryUrl = "http://10.97.136.161:8081" // Schema registry URL
  val VALUE_SERIALIZATION_FLAG = "value"

  def main(args: Array[String]): Unit = {

    val props = new java.util.HashMap[String, Object]()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "testgroup1")
    props.put("enable.auto.commit", "false")
    props.put("session.timeout.ms", "30000")

    val consumer = new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Arrays.asList(kafkaTopic),new ConsumerRebalanceListener(){
      override def onPartitionsAssigned (partitions: util.Collection[TopicPartition]) : Unit = {
        println(s"Partition Assigned currently to this consumer are ${util.Arrays.toString(partitions.toArray)}")

        partitions.asScala.foreach(topicPartition => {

          val offsetMetadata = consumer.committed(topicPartition)
          println(s"Topic: ${topicPartition.topic()} Partition: ${topicPartition.partition()}")
          println(s"""committed offset ${offsetMetadata.offset}""")

        })
      }

      override def  onPartitionsRevoked (partitions: util.Collection[TopicPartition]) : Unit = {
        println(s"Partition Revoked currently to this consumer are ${util.Arrays.toString(partitions.toArray)}")
      }
    })


    try{
      while(true) {
        val records: ConsumerRecords[String, String] = consumer.poll(100)
        val recordIterator: java.util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        /*if(recordIterator.hasNext)
          println(s"""Offset being read is : ${recordIterator.next().offset()}""")*/
        while(recordIterator.hasNext)
        {
          val currentRecord :ConsumerRecord[String,String] = recordIterator.next()
          // println(currentRecord)

        }
        /*Commits the offset for all topics/partitions read by the last poll */
        consumer.commitAsync()
      }
    }

  }

  }
