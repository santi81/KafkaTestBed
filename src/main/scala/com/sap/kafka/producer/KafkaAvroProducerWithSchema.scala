package com.sap.kafka.producer

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer._

import scala.io.Source

object KafkaAvroProducerWithSchema {

  val kafkaTopic = "vora_1"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host

  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer])
    props.put("schema.registry.url", "http://10.97.136.161:8081")


    val fileStream = getClass.getResourceAsStream("/MyRecord.avsc")
    val schemaFromFile = Source.fromInputStream(fileStream).getLines().mkString
    val schemaParser = new Schema.Parser
    val avroSchema = schemaParser.parse(schemaFromFile)
    val avroProducer = new KafkaProducer[GenericRecord, GenericRecord](props)

    // Create the Avro objects for the key and value
    val avroRecord = new GenericData.Record(avroSchema)
    avroRecord.put("str1","field1")
    avroRecord.put("str2","field2")

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
