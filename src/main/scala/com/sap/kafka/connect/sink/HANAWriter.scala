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
    val bufferByTable = scala.collection.mutable.Map[String, HANASinkRecordsCollector]()

    records.foreach(record => {
      val table: String = record.topic
      val buffer: Option[HANASinkRecordsCollector] = bufferByTable.get(table)
      buffer match {
        case None =>
          val bufferRecords = new HANASinkRecordsCollector(table, hanaClient)
          bufferByTable.put(table, bufferRecords)
          bufferRecords.add(record)
        case Some(bufferRecords) =>
          bufferRecords.add(record)
      }
    })
  }
}
