package com.sap.kafka.producer

import io.confluent.kafka.serializers.KafkaAvroSerializer
import model.{SimpleCard, CardSuite}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaAvroProducerWithSchemaRegistry {

  val kafkaTopic = "kafka-avro-registry1"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host

  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer])
    props.put("schema.registry.url", "http://10.97.136.161:8081")



    val avroProducer = new KafkaProducer[GenericRecord, GenericRecord](props)

    // Create the Avro objects for the key and value
    val suit = new CardSuite("spades")
    val card = new SimpleCard("spades", "ace")
    val record = new ProducerRecord[GenericRecord, GenericRecord](kafkaTopic, suit, card)
    avroProducer.send(record)
    avroProducer.close()
  }
}
