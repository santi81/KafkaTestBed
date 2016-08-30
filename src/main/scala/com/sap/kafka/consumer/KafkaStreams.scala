package com.sap.kafka.consumer

import java.util.{Locale, Arrays}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringSerializer, Serdes, StringDeserializer}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{ValueMapper, KStream, KStreamBuilder}

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
    val textLines: KStream[String,String] = builder.stream(kafkaTopic)



    val valueMapper = new ValueMapper[String,Iterable[String]]{
      override def apply(value: String): Iterable[String]  = {

        value.toLowerCase(Locale.getDefault).split(" ").toList
      }
    }
    textLines.flatMapValues[String](valueMapper)



  }
}
