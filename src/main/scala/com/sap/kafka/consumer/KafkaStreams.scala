package com.sap.kafka.consumer

import java.util.{Arrays, Locale}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, ValueMapper}
import scala.collection.JavaConverters._

object KafkaStreams {


  val kafkaTopic = "kafka_streams_testing"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host

  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "10.97.136.161:2181")
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
      classOf[Serdes.StringSerde])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val builder = new KStreamBuilder()
    val textLines: KStream[String, String] = builder.stream(kafkaTopic)

    val valueMapper = new ValueMapper[String,java.lang.Iterable[String]]{
      override def apply(value: String): java.lang.Iterable[String]  = {
        value.toLowerCase(Locale.getDefault).split(" ").toList.asJava
      }
    }
    textLines.flatMapValues[String](valueMapper)

  }
}
