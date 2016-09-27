package com.sap.kafka.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random


// This is a Simple Asynchronous Kafka Producer producing to a topic with single partition
object SimpleKafkaProducer_Sync {

  //TO-DO : Replace with your Kafka Topic
  val kafkaTopic = "attendee00-simple-topic1"    // command separated list of topics
  // topic.autocreate is set to true. Hence it creates a topic if the topic doesnt exist
  // default topic.partitions is set to 1


  val kafkaBrokers = "10.97.183.115:9092,10.97.191.51:9092,10.97.152.59:9092,10.97.152.66:9092"   // comma separated list of broker:host

  // === Configurations of amount of data to produce ===
  val recordsPerSecond = 10000
  val wordsPerRecord = 10
  val numSecondsToSend = 120
  val randomWords = List("Germany", "India", "USA")
  val totals = scala.collection.mutable.Map[String, Int]()


  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()

    //Kafka Brokers
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)

    //Key Serializer
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    // Value Serializer
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")


    println(s"Putting records onto Kafka topic $kafkaTopic at a rate of" +
      s" $recordsPerSecond records per second with $wordsPerRecord words per record for $numSecondsToSend seconds")

    //Instantiate the Kafka Producer with the properties config
    val producer = new KafkaProducer[String, String](props)

    // Generate and send the data
    for (round <- 1 to numSecondsToSend) {
      for (recordNum <- 1 to recordsPerSecond) {
        val data = generateData()
        val message = new ProducerRecord[String, String](kafkaTopic, null, data)
        // Here, we are using Future.get() to wait until the reply from Kafka arrives back.
        // The specific Future implemented by the Producer will throw an exception if Kafka broker sent back an error
        // and our application can handle the problem.
        // If there were no errors, we will get a RecordMetadata object which we can use to retrieve
        // the offset the message was written to.
        val recordMetadata = producer.send(message).get()
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
