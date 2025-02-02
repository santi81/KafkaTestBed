package com.sap.kafka.producer

import org.apache.kafka.clients.producer._

import scala.util.Random

object KafkaProducer_Async {

  //TO-DO : Replace with your Kafka Topic
  val kafkaTopic = "attendee00-simple-topic1"    // command separated list of topics
  // topic.autocreate is set to true. Hence it creates a topic if the topic doesnt exist
  // default topic.partitions is set to 1


  val kafkaBrokers = "10.97.183.115:9092,10.97.191.51:9092,10.97.152.59:9092,10.97.152.66:9092"
     // comma separated list of broker:host

  // === Configurations of amount of data to produce ===
  val recordsPerSecond = 10000
  val wordsPerRecord = 10
  val numSecondsToSend = 120
  val randomWords = List("Germany", "India", "USA")
  val totals = scala.collection.mutable.Map[String, Int]()


  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    props.put("acks","all")


    println(s"Putting records onto Kafka topic $kafkaTopic at a rate of" +
      s" $recordsPerSecond records per second with $wordsPerRecord words per record for $numSecondsToSend seconds")

    val producer = new KafkaProducer[String, String](props)

    // Generate and send the data
    for (round <- 1 to numSecondsToSend) {
      for (recordNum <- 1 to recordsPerSecond) {
        val data = generateData()
        val message = new ProducerRecord[String, String](kafkaTopic, null, data)
        producer.send(message,new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            // println(s"Message has been published to ${metadata.partition()}")
            println(s"""The message offset is ${metadata.offset()}""")
          }
        })
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
