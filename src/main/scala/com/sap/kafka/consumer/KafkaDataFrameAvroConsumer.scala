package com.sap.kafka.consumer

import java.util

import com.sap.kafka.schema.SchemaConverters
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import com.sap.kafka.schema.SchemaConverters._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, ConsumerConfig}
import org.apache.spark.sql.types.StructType

object KafkaDataFrameAvroConsumer {


  val kafkaTopic = "kafka-avro-dataframe1"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host
  val schemaRegistryUrl = "http://10.97.136.161:8081" // Schema registry URL
  val VALUE_SERIALIZATION_FLAG = "value"

  def main(args: Array[String]): Unit = {

    val props = new java.util.HashMap[String, Object]()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroDeserializer])
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("schema.registry.url", "http://10.97.136.161:8081")
    // props.put("specific.avro.reader", "true")
    props.put("group.id", "testgroup1")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")

    val consumer = new KafkaConsumer[String,GenericRecord](props)
    consumer.subscribe(util.Arrays.asList(kafkaTopic))

    //Get the schema of the topic
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1)
    val subject = kafkaTopic + "-" + VALUE_SERIALIZATION_FLAG
    val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema
    val parser = new Schema.Parser
    val schema = parser.parse(avroSchema)


   //Convert to Spark Schema
    val sparkSchema = SchemaConverters.toSqlType(schema).dataType match {
      case t: StructType => Some(t)
      case _ => throw new RuntimeException(
        s"""Avro schema cannot be converted to a Spark SQL StructType:
           |
           |${schema.toString(true)}
            |""".stripMargin)
    }

    println(sparkSchema)

    while(true) {
      val records: ConsumerRecords[String, GenericRecord] = consumer.poll(100)
      val recordIterator: java.util.Iterator[ConsumerRecord[String, GenericRecord]] = records.iterator()
      while(recordIterator.hasNext)
      {
        val currentRecord :ConsumerRecord[String,GenericRecord] = recordIterator.next()
        println(s"""${currentRecord.offset()}, ${currentRecord.
          key()}, ${currentRecord.value()}""")

      }
    }
  }




  }
