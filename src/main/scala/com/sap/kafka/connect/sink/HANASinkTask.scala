package com.sap.kafka.connect.sink

import java.util

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._


class HANASinkTask extends SinkTask {
    /**
     * Parse the configurations and setup the writer
     * */
    override def start(props: util.Map[String, String]): Unit = {

      println("Start Method of the task called...with the folliwing properties")
      props.asScala.toMap.foreach(println)
    }

    /**
     * Pass the SinkRecords to the writer for Writing
     * */
    override def put(records: util.Collection[SinkRecord]): Unit = {

      println("The put method of the task called")
      records.asScala.toList.foreach(println)


    }


    override def stop(): Unit = {
      println("Stop Method of the Task Called")
    }

    override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) : Unit = {
      //    //while (writer.get.insertCount.get > 0) {
      //      logger.info("Waiting for writes to flush.")
      //      Thread.sleep(flushSleep)
      //    //}
    }

    override def version(): String = getClass.getPackage.getImplementationVersion
}
