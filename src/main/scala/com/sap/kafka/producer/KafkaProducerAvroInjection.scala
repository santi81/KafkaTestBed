
package com.sap.kafka.producer

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import model.MyRecord
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}



object KafkaProducerAvroInjection {

  val kafkaTopic = "kafka-avro-injection"    // command separated list of topics
  val kafkaBrokers = "10.97.136.161:9092"   // comma separated list of broker:host

  def main(args: Array[String]): Unit = {
    val props = new java.util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val schema = MyRecord.getClassSchema

      val recordInjection:Injection[GenericRecord,Array[Byte]] = GenericAvroCodecs.toBinary(schema)
       val avroProducer = new KafkaProducer[String,Array[Byte]](props)
       for (i <- 0 to 1000) {
        val avroRecord = new GenericData.Record(schema)
        avroRecord.put("str1", "Str 1-" + i)
        avroRecord.put("str2", "Str 2-" + i)
        val bytes = recordInjection.apply(avroRecord)

        // val record = new ProducerRecord("mytopic", bytes).asInstanceOf[ProducerRecord[String,Array[Byte]]]
        val record:ProducerRecord[String,Array[Byte]] = new ProducerRecord(kafkaTopic, bytes)
         avroProducer.send(record)
         println(record.toString)
         Thread.sleep(250)
    }
    avroProducer.close()

  }

}

