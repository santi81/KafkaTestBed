package com.sap.kafka.producer


import java.util.Properties

import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.util.Random
import kafka.admin.AdminUtils


object KafkaPartitionedProducer {

  val kafkaTopic = "attendee00-partitioned-topic"    // command separated list of topics
  val kafkaBrokers = "10.97.183.115:9092,10.97.191.51:9092,10.97.152.59:9092,10.97.152.66:9092"
     // comma separated list of broker:host

  // === Configurations of amount of data to produce ===
  val recordsPerSecond = 100000
  val wordsPerRecord = 10
  val numSecondsToSend = 120
  val randomWords = List("Germany", "India", "USA")
  val totals = scala.collection.mutable.Map[String, Int]()

  val numPartitions = 3
  val iSR = 2


  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    // Specify the partitioner

    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
      "com.sap.kafka.partitioner.SimplePartitioner")


    // Topic doesnt exist.Though auto.create is enabled it by default it creates 1 partition.
    // Here we try to directly create a topic by using kafkaCreateTopic
    val topicExists = AdminUtils.topicExists(ZkUtils.apply("10.97.183.115:2181", 10000, 10000, isZkSecurityEnabled = false)
      , kafkaTopic)
    if(!topicExists) {
      try {
        AdminUtils.createTopic(ZkUtils.apply("10.97.183.115:2181", 10000, 10000, isZkSecurityEnabled = false)
          , kafkaTopic, numPartitions, iSR, new Properties())
      }
      catch {
        case ex: Exception =>
          println("Exception creating topic.Terminating")
          System.exit(1)
      }
    }

    println(s"Putting records onto Kafka topic $kafkaTopic at a rate of" +
      s" $recordsPerSecond records per second with $wordsPerRecord words per record for $numSecondsToSend seconds")

    val producer = new KafkaProducer[String, String](props)

    // Generate and send the data
    for (round <- 1 to numSecondsToSend) {
      for (recordNum <- 1 to recordsPerSecond) {
        val data = generateData()
        val message = new ProducerRecord[String, String](kafkaTopic, null, data)
        producer.send(message)
      }
      Thread.sleep(1000) // Sleep for a second
      println(s"Sent $recordsPerSecond records with $wordsPerRecord words each")
    }

    println("\nTotal number of records sent")
    totals.toSeq.sortBy(_._1).mkString("\n")

  }

  def generateData() = (1 to wordsPerRecord).map { x =>
    val randomWord = randomWords(Random.nextInt(randomWords.size))
    totals(randomWord) = totals.getOrElse(randomWord, 0) + 1
    randomWord
  }.mkString(" ")

}
