/**
  * Copyright 2016 Confluent Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.sap.kafka.consumer

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{TimeWindows, KStream, KStreamBuilder}
import scala.collection.JavaConverters._
/**
  * Demonstrates how to perform simple, state-less transformations via map functions.
  * Same as [[MapFunctionLambdaExample]] but in Scala.
  *
  * Use cases include e.g. basic data sanitization, data anonymization by obfuscating sensitive data
  * fields (such as personally identifiable information aka PII).  This specific example reads
  * incoming text lines and converts each text line to all-uppercase.
  *
  * Requires a version of Scala that supports Java 8 and SAM / Java lambda (e.g. Scala 2.11 with
  * `-Xexperimental` compiler flag, or 2.12).
  *
  * HOW TO RUN THIS EXAMPLE
  *
  * 1) Start Zookeeper and Kafka.
  * Please refer to <a href='http://docs.confluent.io/3.0.1/quickstart.html#quickstart'>CP3.0.1 QuickStart</a>.
  *
  * 2) Create the input and output topics used by this example.
  *
  * {{{
  * $ bin/kafka-topics --create --topic TextLinesTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * $ bin/kafka-topics --create --topic UppercasedTextLinesTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * $ bin/kafka-topics --create --topic OriginalAndUppercasedTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * }}}
  *
  * Note: The above commands are for CP 3.0.1 only. For Apache Kafka it should be `bin/kafka-topics.sh ...`.
  *
  * 3) Start this example application either in your IDE or on the command line.
  *
  * If via the command line please refer to
  * <a href='https://github.com/confluentinc/examples/tree/master/kafka-streams#packaging-and-running'>Packaging</a>.
  * Once packaged you can then run:
  *
  * {{{
  * $ java -cp target/streams-examples-3.0.1-standalone.jar io.confluent.examples.streams.MapFunctionScalaExample
  * }
  * }}}
  *
  * 4) Write some input data to the source topics (e.g. via `kafka-console-producer`.  The already
  * running example application (step 3) will automatically process this input data and write the
  * results to the output topics.
  *
  * {{{
  * # Start the console producer.  You can then enter input data by writing some line of text,
  * # followed by ENTER:
  * #
  * #   hello kafka streams<ENTER>
  * #   all streams lead to kafka<ENTER>
  * #
  * # Every line you enter will become the value of a single Kafka message.
  * $ bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic
  * }}}
  *
  * 5) Inspect the resulting data in the output topics, e.g. via `kafka-console-consumer`.
  *
  * {{{
  * $ bin/kafka-console-consumer --zookeeper localhost:2181 --topic UppercasedTextLinesTopic --from-beginning
  * $ bin/kafka-console-consumer --zookeeper localhost:2181 --topic OriginalAndUppercasedTopic --from-beginning
  * }}}
  *
  * You should see output data similar to:
  * {{{
  * HELLO KAFKA STREAMS
  * ALL STREAMS LEAD TO KAFKA
  * }}}
  *
  * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`.  If needed,
  * also stop the Kafka broker (`Ctrl-C`), and only then stop the ZooKeeper instance (`Ctrl-C`).
  */
object MapFunctionScalaExample {

  def main(args: Array[String]) {
    val builder: KStreamBuilder = new KStreamBuilder

    val streamingConfig = {
      val settings = new Properties
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-scala-example")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.97.136.161:9092")
      settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "10.97.136.161:2181")
      // Specify default (de)serializers for record keys and for record values.
      settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings
    }

    val stringSerde: Serde[String] = Serdes.String()

    // Read the input Kafka topic into a KStream instance.
    val textLines: KStream[String, String] = builder.stream("kafka_streams_testing4")

    // Variant 1: using `mapValues`
    import KeyValueImplicits._
    val uppercasedWithMapValues: KStream[String, String] = textLines.mapValues(_.toUpperCase())

    val flatMap:KStream[String, String] = uppercasedWithMapValues.flatMapValues(_.split(" ").toList.asJava)

    val filteredMap:KStream[String, String] = flatMap.filter((key,value) => value == "GERMANY")




    //filteredMap.print()

    // flatMap.print()

    // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
    //
    // In this case we can rely on the default serializers for keys and values because their data
    // types did not change, i.e. we only need to provide the name of the output topic.
    // uppercasedWithMapValues.to("UppercasedTextLinesTopic")

    // We are using implicit conversions to convert Scala's `Tuple2` into Kafka Streams' `KeyValue`.
    // This allows us to write streams transformations as, for example:
    //
    //    map((key, value) => (key, value.toUpperCase())
    //
    // instead of the more verbose
    //
    //    map((key, value) => new KeyValue(key, value.toUpperCase())
    //

    // Variant 2: using `map`, modify value only (equivalent to variant 1)
    // val uppercasedWithMap: KStream[Array[Byte], String] = flatMap.map((key, value) => (key, value.toUpperCase()))

    // Variant 3: using `map`, modify both key and value
    //
    // Note: Whether, in general, you should follow this artificial example and store the original
    //       value in the key field is debatable and depends on your use case.  If in doubt, don't
    //       do it.
    val originalAndUppercased: KStream[String, Int] = flatMap.map((key, value) => (value, 1))





    val kTable = originalAndUppercased.countByKey(TimeWindows.of("PageViewCountWindows", 2 * 60 * 1000L),stringSerde)
    kTable.toStream.print()

    // Write the results to a new Kafka topic "OriginalAndUppercasedTopic".
    //
    // In this case we must explicitly set the correct serializers because the default serializers
    // (cf. streaming configuration) do not match the type of this particular KStream instance.

    //originalAndUppercased.print()
    //originalAndUppercased.to(stringSerde, stringSerde, "OriginalAndUppercasedTopic")

    val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
    stream.start()
  }

}