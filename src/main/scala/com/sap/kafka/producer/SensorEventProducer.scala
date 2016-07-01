package com.sap.kafka.producer

import java.io.InputStream
import java.util.HashMap

import com.sap.kafka.producer.SensorEventProducer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.admin.AdminUtils

object SensorEventProducer {

   // val kafkaTopic = "antenna_sensor_data"    // command separated list of topics
   val kafkaBrokers = "10.97.178.172:6667"   // comma separated list of broker:host

   // === Configurations of amount of data to produce ===
   val recordsPerSecond = 100
   val wordsPerRecord = 10
   val numSecondsToSend = 1000

   def main(args: Array[String]): Unit = {
     val props = new java.util.HashMap[String, Object]()
     props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
     props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
       "org.apache.kafka.common.serialization.StringSerializer")
     props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
       "org.apache.kafka.common.serialization.StringSerializer")

     val producer = new KafkaProducer[String, String](props)
     for (i <- 1 to 3) {
       val thread = new Thread {
         override def run {
           // your custom behavior here
           val (resourceName,kafkaTopic) =  i match  {
             case 1 => ("/part-t-0000_ant.csv","antenna_sensor_data1")
             case 2 => ("/part-t-0000_pres.csv","pressure_sensor_data")
             case 3 => ("/part-t-0000_temp.csv", "temperature_sensor_data")
           }
           val stream : InputStream = getClass.getResourceAsStream(resourceName)
           val lines = scala.io.Source.fromInputStream( stream ).getLines()
           var recordNum = 0
           println(s"Putting records onto Kafka topic $kafkaTopic at a rate of" +
             s" $recordsPerSecond records per second with $wordsPerRecord words per record for $numSecondsToSend seconds")
           while(lines.hasNext)
           {
             val data = lines.next()
             val message = new ProducerRecord[String, String](kafkaTopic, null, data)
             producer.send(message)
             recordNum +=1
             if (recordNum >= recordsPerSecond) {
               recordNum = 0
               Thread.sleep(1000)
               println(s"Sent $recordsPerSecond Sensor Records at $wordsPerRecord ")
             }
           }
         }
       }
       thread.start()
     }
   }
 }
