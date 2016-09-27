package com.sap.kafka.producer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random


// This is a Simple Asynchronous Kafka Producer producing to a topic with single partition
object SimpleKafkaProducer_Fire_And_Forget {

  //TO-DO : Replace with your Kafka Topic
  val kafkaTopic = "attendee00-simple-topic1"    // command separated list of topics
  // topic.autocreate is set to true.Hence it creates a topic if the topic doesnt exist
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

    // More advanced options
    // If the request fails, the producer can automatically retry,
    // though since we have specified retries as 0 it won't. Enabling retries also opens up the possibility of duplicates
    // props.put("retries", 0)

    //The producer maintains buffers of unsent records for each partition.
    // These buffers are of a size specified by the batch.size config.
    // Making this larger can result in more batching, but requires more memory
    // (since we will generally have one of these buffers for each active partition).
    // props.put("batch.size", 16384)

    // props.put("linger.ms", 1)

    // The buffer.memory controls the total amount of memory available to the producer for buffering.
    // If records are sent faster than they can be transmitted to the server then this buffer space will be exhausted.
    // When the buffer space is exhausted additional send calls will block.
    // For uses where you want to avoid any blocking you can set block.on.buffer.full=false
    // which will cause the send call to result in an exception.
    // props.put("buffer.memory", 33554432)


    println(s"Putting records onto Kafka topic $kafkaTopic at a rate of" +
      s" $recordsPerSecond records per second with $wordsPerRecord words per record for $numSecondsToSend seconds")

    //Instantiate the Kafka Producer with the properties config
    val producer = new KafkaProducer[String, String](props)

    // Generate and send the data
    for (round <- 1 to numSecondsToSend) {
      for (recordNum <- 1 to recordsPerSecond) {
        val data = generateData()
        val message = new ProducerRecord[String, String](kafkaTopic, null, data)

        // Fire-and-forget - in which we send a message to the server and
        // don’t really care if it arrived succesfully or not.
        // Most of the time, it will arrive successfully, since Kafka is highly available and the producer will
        // retry sending messages automatically. However, some messages will get lost using this method.
        try {
          // Asynchronously send a record to a topic.
          // Equivalent to send(record, null). See send(ProducerRecord, Callback) for details.
          producer.send(message)
        }
        catch {
          case ex : Exception =>
            ex.printStackTrace()
        }
        finally
        {
           producer.close()
        }
      }
      Thread.sleep(1000) // Sleep for a second
      println(s"Sent $recordsPerSecond records with $wordsPerRecord words each")
    }
    producer.close()
    println("\nTotal number of records sent")
    totals.toSeq.sortBy(_._1).mkString("\n")

  }

  def generateData() = (1 to wordsPerRecord).map { x =>
    val randomWord = randomWords(Random.nextInt(randomWords.size))
    totals(randomWord) = totals.getOrElse(randomWord, 0) + 1
    randomWord
  }.mkString(" ")

}
