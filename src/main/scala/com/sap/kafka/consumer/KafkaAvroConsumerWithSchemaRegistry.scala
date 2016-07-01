package com.sap.kafka.consumer

import java.util

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import model.{CardSuite, SimpleCard}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}

object KafkaAvroConsumerWithSchemaRegistry {


  val kafkaTopic = "kafka-avro-registry1"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host

  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroDeserializer])
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroDeserializer])
    props.put("schema.registry.url", "http://10.97.136.161:8081")
    props.put("specific.avro.reader", "true")
    props.put("group.id", "testgroup1")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")

    val consumer = new KafkaConsumer[CardSuite,SimpleCard](props)
    consumer.subscribe(util.Arrays.asList(kafkaTopic))

    while(true) {
      val records: ConsumerRecords[CardSuite, SimpleCard] = consumer.poll(100)
      val recordIterator: java.util.Iterator[ConsumerRecord[CardSuite, SimpleCard]] = records.iterator()
      while(recordIterator.hasNext)
        {
          val currentRecord :ConsumerRecord[CardSuite,SimpleCard] = recordIterator.next()
          println(s"""${currentRecord.offset()}, ${currentRecord.
            key().getSuit}, ${currentRecord.value().getCard}""")

        }


    }


  }
}
