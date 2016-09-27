package com.sap.kafka.consumer

import java.util

import org.apache.kafka.clients.consumer._

object SimpleKafkaConsumer {


  val kafkaTopic = "attendee00-simple-topic1"    // command separated list of topics
  val kafkaBrokers = "10.97.183.115:9092,10.97.191.51:9092,10.97.152.59:9092,10.97.152.66:9092" //comma seperated list of brokers

  def main(args: Array[String]): Unit = {

    val props = new java.util.HashMap[String, Object]()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)

    //Key-Serializer
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")

    //Value-Serializer
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")

    // Consumer Group ID
    props.put("group.id", "attendee00Group")

    // Auto Commit of Offsets
    props.put("enable.auto.commit", "true")

    // Auto Commit Interval
    props.put("auto.commit.interval.ms", "1000")

    //Consumer Time to Live before its assumed dead
    props.put("session.timeout.ms", "30000")

    //Create the Kafka Consumer
    val consumer = new KafkaConsumer[String,String](props)

    //Subscribe to the Topic
    consumer.subscribe(util.Arrays.asList(kafkaTopic))

    try{
      while(true) {
        val records: ConsumerRecords[String, String] = consumer.poll(100)
        val recordIterator: java.util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        println(s"""Number of records received is ${records.count()}""")
        while(recordIterator.hasNext)
        {
          val currentRecord :ConsumerRecord[String,String] = recordIterator.next()
          println(currentRecord.offset())

        }
      }
    }
    catch {
      case ex : Exception =>
        ex.printStackTrace()
    }
    finally {
      consumer.close()
    }

  }

  }
