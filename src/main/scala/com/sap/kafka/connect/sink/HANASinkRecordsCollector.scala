package com.sap.kafka.connect.sink



import com.sap.kafka.client.{metaSchema, metaAttr, HANAJdbcClient}
import com.sap.kafka.schema.KeyValueSchema
import com.sap.kafka.utils.JdbcTypeConverter
import org.apache.kafka.connect.data.{Field, Schema}
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConversions._


class HANASinkRecordsCollector(var tableName: String, client: HANAJdbcClient) {
  private val log: Logger = LoggerFactory.getLogger(classOf[HANASinkTask])
  private var records: Seq[SinkRecord] = Seq[SinkRecord]()
  private var metaSchema: metaSchema = null
  var tableExistsFlag = true

  private[sink] def add(record: SinkRecord): Unit = {

    records = records :+ record
    val recordSchema = KeyValueSchema(record.keySchema(),record.valueSchema())
    val tableExists = client.tableExists(None,tableName)

    tableExists match
    {
      case true =>
        tableExistsFlag = true
        val tableMetaData = client.getMetaData(tableName,None)
        metaSchema = new metaSchema(tableMetaData, null)
        log.warn("Table already exists.Validate the schema")
        //Compare the Schemas

      case false =>
        tableExistsFlag = false
        metaSchema = new metaSchema(Seq[metaAttr](),Seq[Field]())
        for (field <- recordSchema.valueSchema.fields) {
          val fieldSchema: Schema = field.schema
          val fieldAttr = metaAttr(fieldSchema.name(), JdbcTypeConverter.convertToHANAType(fieldSchema.`type`()),1,0,0,isSigned = false )
          metaSchema.fields = metaSchema.fields :+ fieldAttr
          metaSchema.avroFields = metaSchema.avroFields :+ field
        }
        client.createTable(Some("SYSTEM"), tableName, metaSchema, 1000, columnTable = false)
    }

  }

  private[sink] def flush(): Unit = {
       client.loadData(Some("SYSTEM"),tableName,metaSchema,records,1000)
  }

}
