package com.sap.kafka.consumer

import java.util

import org.apache.kafka.clients.consumer._

object KafkaConsumer {


  val kafkaTopic = "attendee00-simple-topic1"    // command separated list of topics
  val kafkaBrokers = "10.97.183.115:9092,10.97.191.51:9092,10.97.152.59:9092,10.97.152.66:9092" //comma seperated list of brokers

  def main(args: Array[String]): Unit = {

    val props = new java.util.HashMap[String, Object]()

    // List of Broker host/port pairs used to establish the initial connection to the cluster
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)

    //Key-Serializer
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")

    //Value-Serializer
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")

    // Consumer Group ID
    // A unique string that identifies the Consumer Group this Consumer belongs to.
    props.put("group.id", "attendee00Group")

    // Auto Commit of Offsets
    //When set to true (the default), the Consumer will trigger offset commits based
    // on the value of auto.commit.interval.ms (default 5000ms)
    props.put("enable.auto.commit", "true")

    // Auto Commit Interval
    props.put("auto.commit.interval.ms", "1000")

    //Consumer Time to Live before its assumed dead
    props.put("session.timeout.ms", "30000")

    //Create the Kafka Consumer
    val consumer = new KafkaConsumer[String,String](props)

    //Subscribe to the Topic
    //The Consumer can subscribe to as many topics as it wishes,
    // although typically this is often just a single topic.
    // Note that this call is not additive; calling subscribe again will remove the existing list of topics,
    // and will only subscribe to those specified in the new call
    consumer.subscribe(util.Arrays.asList(kafkaTopic))

    try{
      while(true) {

        // Each call to poll returns a (possibly empty) list of messages.
        // The parameter controls the maximum amount of time in ms that the Consumer will block
        // if no new records are available. If records are available, it will return immediately.
        val records: ConsumerRecords[String, String] = consumer.poll(100)

        val recordIterator: java.util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        println(s"""Number of records received is ${records.count()}""")
        while(recordIterator.hasNext)
        {
          val currentRecord :ConsumerRecord[String,String] = recordIterator.next()
          // println(currentRecord.offset())
          println(currentRecord.value())

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
