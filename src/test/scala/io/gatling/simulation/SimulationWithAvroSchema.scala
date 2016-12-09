package io.gatling.simulation

import java.util

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.gatling.core.Predef._
import io.gatling.data.generator.RandomDataGenerator
import io.gatling.kafka.{KafkaProducerBuilder, KafkaProducerProtocol}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerConfig
import scala.concurrent.duration._

class SimulationWithAvroSchema extends Simulation {
  val kafkaTopic = "kafka_streams_testing3001"
  val kafkaBrokers = "10.97.183.115:9092"

  val props = new util.HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put("schema.registry.url", "http://10.97.183.115:8081")

  val user_schema1 =
    s"""
       | {
       |    "fields": [
       |        { "name": "int1", "type": "int" }
       |    ],
       |    "name": "myrecord",
       |    "type": "record"
       |}
     """.stripMargin

  val user_schema2 =
    s"""
       | {
       |    "fields": [
       |        { "name": "int2", "type": "int" }
       |    ],
       |    "name": "myrecord",
       |    "type": "record"
       |}
     """.stripMargin

  val tableName = "\"SYSTEM\".\"com.sap.test::hello3001\""

  val keySchema = new Schema.Parser().parse(user_schema1)
  val valueSchema = new Schema.Parser().parse(user_schema2)

  val dataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]()
  val kafkaProducerProtocol = new KafkaProducerProtocol[GenericRecord, GenericRecord](props, kafkaTopic, dataGenerator)
  val scn = scenario("Kafka Producer Call").exec(KafkaProducerBuilder[GenericRecord, GenericRecord](tableName, Some(keySchema), Some(valueSchema)))

  // constantUsersPerSec(100000) during (1 minute)
  setUp(scn.inject(constantUsersPerSec(1) during (1 minute))).protocols(kafkaProducerProtocol)
}