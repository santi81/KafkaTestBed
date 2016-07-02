package com.sap.kafka.consumer

import java.util

import com.sap.kafka.schema.SchemaConverters
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import com.sap.kafka.schema.SchemaConverters._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, ConsumerConfig}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.JavaConverters._


import org.apache.spark.sql.{SQLContext, Row}

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

    val conf = new SparkConf().setAppName("Test Plans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    while(true) {
      val records: ConsumerRecords[String, GenericRecord] = consumer.poll(100)
      val recordIterator: java.util.Iterator[ConsumerRecord[String, GenericRecord]] = records.iterator()

      var myRows = Seq[Row]()
      while(recordIterator.hasNext)
      {
        val currentRecord :ConsumerRecord[String,GenericRecord] = recordIterator.next()
        var tempSeq = Seq[Any]()
         schema.getFields.asScala.toList.foreach((x : Schema.Field) => {
           val converter = SchemaConverters.createConverterToSQL(x.schema())
           tempSeq = tempSeq :+ converter(currentRecord.value().get(x.pos))
         })
        val myRow = Row.fromSeq(tempSeq)
        myRows = myRows :+ myRow
      }
      if(myRows.nonEmpty) {
        val rddRows = sc.parallelize(myRows)
        val df = sqlContext.createDataFrame(rddRows, sparkSchema.get)
        df.collect().foreach(println)
      }

    }
  }

  }
