package com.sap.kafka.connect.sink

import java.sql.SQLException
import java.util

import com.sap.kafka.client.{HANAJdbcClient, HANAConfiguration}
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._


class HANAWriter(config: HANAConfiguration) {

  private var hanaClient: HANAJdbcClient = null


  private def initConnection(): Unit = {
    hanaClient = new HANAJdbcClient(config)
  }


  @throws(classOf[SQLException])
  private[sink] def write(records: util.Collection[SinkRecord]): Unit = {
    initConnection()
    val cacheByTopic = scala.collection.mutable.Map[String, HANASinkRecordsCollector]()

    records.foreach(record => {
      val table: String = record.topic
      val cache: Option[HANASinkRecordsCollector] = cacheByTopic.get(table)
      cache match {
        case None =>
          val cacheRecords = new HANASinkRecordsCollector(table, hanaClient)
          cacheByTopic.put(table, cacheRecords)
          cacheRecords.add(record)
        case Some(bufferRecords) =>
          bufferRecords.add(record)
      }
    })

    for (cache <- cacheByTopic.values) {
      if (cache.tableExistsFlag) {
        cache.flush()
      }
    }
  }
}
