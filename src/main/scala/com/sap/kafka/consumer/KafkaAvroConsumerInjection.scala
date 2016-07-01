package com.sap.kafka.consumer

import java.util
import java.util.Properties

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import model.MyRecord
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import scala.util.Try


object KafkaAvroConsumerInjection {
  def main(args: Array[String]): Unit = {

    val props = new Properties
    props.put("bootstrap.servers", "10.97.136.161:9092")
    props.put("group.id", "testgroup")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("specific.avro.reader", "true")

    val consumer: KafkaConsumer[String, Array[Byte]] = new KafkaConsumer(props)
    consumer.subscribe(util.Arrays.asList("kafka-avro-injection"))

    while (true) {
      val records: ConsumerRecords[String, Array[Byte]] = consumer.poll(100)
      val recordIterator: java.util.Iterator[ConsumerRecord[String, Array[Byte]]] = records.iterator()
      while(recordIterator.hasNext)
      {
          val currentRecord :ConsumerRecord[String,Array[Byte]] = recordIterator.next()
          val schema = MyRecord.getClassSchema
          val recordInjection :Injection[GenericRecord, Array[Byte]]  = GenericAvroCodecs.toBinary(schema)
          val record:Try[GenericRecord] = recordInjection.invert(currentRecord.value())
          record.isSuccess match{
            case true=>
             println(record.get.get("str1"))
            case false => println("Cannot DeSerialize")

          }
        }
    }
  }

}
