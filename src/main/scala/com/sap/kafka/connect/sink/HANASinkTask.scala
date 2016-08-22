package com.sap.kafka.connect.sink

import java.util

import com.sap.kafka.client._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._


class HANASinkTask extends SinkTask {
    /**
     * Parse the configurations and setup the writer
     * */
    private var config: HANAConfiguration = null
    private val log: Logger = LoggerFactory.getLogger(classOf[HANASinkTask])
    private var writer:HANAWriter = null


    override def start(props: util.Map[String, String]): Unit = {
      log.info("Starting task")
      config = HANAConfiguration.prepareConfiguration(props.asScala.toMap)
      writer = new HANAWriter(config)
    }

    /**
     * Pass the SinkRecords to the writer for Writing
     * */
    override def put(records: util.Collection[SinkRecord]): Unit = {
      if (records.isEmpty) {
        return
      }
      val recordsCount: Int = records.size
      log.trace("Received {} records.", recordsCount)
      writer.write(records) // handle Exception
    }


    override def stop(): Unit = {
      log.info("Stopping task")
    }

    override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) : Unit = {
      //    //while (writer.get.insertCount.get > 0) {
      //      logger.info("Waiting for writes to flush.")
      //      Thread.sleep(flushSleep)
      //    //}
    }

    override def version(): String = getClass.getPackage.getImplementationVersion
}
