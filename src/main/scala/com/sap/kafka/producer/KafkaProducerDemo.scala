package com.sap.kafka.producer
import java.util.HashMap
import scala.util.Random
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

object KafkaProducerDemo {

  val kafkaTopic = "kafka_streams_testing"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host
  //val kafkaBrokers = "172.16.204.11:6667,172.16.204.14:6667,172.16.204.15:6667,172.16.204.16:6667"   // comma separated list of broker:host

  // === Configurations of amount of data to produce ===
  val recordsPerSecond = 100000
  val wordsPerRecord = 10
  val numSecondsToSend = 120
  val randomWords = List("spark", "you", "are", "my", "father")
  val totals = scala.collection.mutable.Map[String, Int]()


  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")


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
