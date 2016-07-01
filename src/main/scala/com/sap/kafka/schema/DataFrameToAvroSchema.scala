package com.sap.kafka.schema

import java.sql.{Date, Timestamp}

import org.apache.avro.SchemaBuilder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types._

import com.sap.kafka.schema.SchemaConverters._

object DataFrameToAvroSchema extends App{

  val conf = new SparkConf().setAppName("Test Plans").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val peopleRdd = sc
    .textFile("src/main/resources/people.txt")
    .map(_.split(";"))
    .map(x => Row(
    x(0).toInt, x(1), x(2).toInt, x(3).toBoolean,
    x(4).toDouble, Timestamp.valueOf(x(5)), Date.valueOf(x(6))))

  val peopleSchema = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true),
    StructField("member", BooleanType, true),
    StructField("weight", DoubleType, true),
    StructField("datetimeoffset", TimestampType, true),
    StructField("date", TimestampType, true)
  ))
  peopleRdd.collect.foreach(println) // todo fix this issue
  val peopleDf = sqlContext.createDataFrame(peopleRdd, peopleSchema)


  println(peopleDf.schema.toString())

  val recordName = "topLevelRecord"
  val recordNamespace = "recordNamespace"
  val build = SchemaBuilder.record(recordName).namespace(recordNamespace)
  val outputAvroSchema = SchemaConverters.convertStructToAvro(peopleDf.schema, build, recordNamespace)
  println(outputAvroSchema)


}
