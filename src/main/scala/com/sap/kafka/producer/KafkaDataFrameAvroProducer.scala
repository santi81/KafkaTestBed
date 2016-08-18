package com.sap.kafka.producer

import com.sap.kafka.schema.SchemaConverters
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaDataFrameAvroProducer  {

  // val kafkaTopic = "kafka-avro-dataframe1"    // command separated list of topics
  val kafkaTopic = "stream_connect"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host

  def main(args: Array[String]): Unit = {


    val props = new java.util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("schema.registry.url", "http://10.97.136.161:8081")


    val conf = new SparkConf().setAppName("Test Plans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val peopleRdd = sc
      .textFile("src/main/resources/people.txt")
      .map(_.split(";"))
      .map(x => Row(
      x(0).toInt, x(1), x(2).toInt, x(3).toBoolean,
      x(4).toDouble))

    val peopleSchema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("member", BooleanType, true),
      StructField("weight", DoubleType, true)
    ))
    peopleRdd.collect().foreach(println) // todo fix this issue
    val peopleDf = sqlContext.createDataFrame(peopleRdd, peopleSchema)
    val recordName = "topLevelRecord"
    val recordNamespace = "recordNamespace"
    val build = SchemaBuilder.record(recordName).namespace(recordNamespace)
    val schema = SchemaConverters.convertStructToAvro(peopleDf.schema, build, recordNamespace)

    val avroProducer = new KafkaProducer[GenericRecord,GenericRecord](props)
    peopleDf.collect().foreach(x =>
    {
      //Convert an RDD row into an avro Record
      val avroRecord = new GenericData.Record(schema)
      for(i <- 0 to x.length -1 ){
        avroRecord.put(i,x.get(i))
      }
      println(avroRecord)
      // val bytes = recordInjection.apply(avroRecord)
      val record:ProducerRecord[GenericRecord,GenericRecord] = new ProducerRecord(kafkaTopic, avroRecord)
      avroProducer.send(record)
    })

    avroProducer.close()


  }

}
