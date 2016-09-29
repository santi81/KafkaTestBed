package com.sap.kafka.producer

import io.confluent.kafka.serializers.KafkaAvroSerializer
import model.{MyRecord}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer._

object KafkaAvroProducer {

  val kafkaTopic = "attendee00-kafka-avro-registry"    // command separated list of topics
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
    val avroRecord = new MyRecord("field1","field2")
    val record = new ProducerRecord[GenericRecord, GenericRecord](kafkaTopic, null, avroRecord )
    avroProducer.send(record,new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        // println(s"Message has been published to ${metadata.partition()}")
        println(s"""The message offset is ${metadata.offset()}""")
      }
    })
    avroProducer.close()
  }
}
