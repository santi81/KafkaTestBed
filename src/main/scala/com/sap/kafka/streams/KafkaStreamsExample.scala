package com.sap.kafka.streams

import java.util.Locale

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}

import scala.collection.JavaConverters._

object KafkaStreamsExample {


  val kafkaTopic = "kafka_streams_testing4"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host

  def main(args: Array[String]): Unit = {
    val props = new java.util.Properties()
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
    val keyValueMapper = new KeyValueMapper[String,String,KeyValue[String,Int]] {

      override def apply(key:String, value: String):KeyValue[String,Int] = {
        new KeyValue(value, 1)
      }

    }
    val lines = textLines.flatMapValues[String](valueMapper).map[String,Int](keyValueMapper)
    val kTable = lines.countByKey(Serdes.String(),"Counts")
    //val kTable = lines.countByKey("Counts")
    val newStream = kTable.toStream
    newStream.print()

    val streams = new KafkaStreams(builder,props)
    streams.start()

  }
}
