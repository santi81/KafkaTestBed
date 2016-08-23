package com.sap.kafka.client


import com.sap.kafka.utils.{JdbcTypeConverter, WithCloseables}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.{Logger, LoggerFactory}

trait AbstractHANAPartitionLoader {

  private val log: Logger = LoggerFactory.getLogger(classOf[AbstractHANAPartitionLoader])

  /**
   * Provides a [[HANAJdbcClient]] implementation used for the partitions loading.
   *
   * @param hanaConfiguration The HANA connection configuration
   * @return a [[HANAJdbcClient]] implementation
   */
  def getHANAJdbcClient(hanaConfiguration: HANAConfiguration): HANAJdbcClient

  /**
   * Loads a partition of a DataFrame to the HANA backend. This is done in
   * a single database transaction in order to avoid repeatedly inserting
   * data as much as possible.
   *
   * @param hanaConfiguration The HANA connection configuration
   * @param tableName The name of the table to load
   * @param iterator Iterator over the dataset to load
   * @param metaSchema The RDD schema
   * @param batchSize The batch size
   */
  private[client] def loadPartition(hanaConfiguration: HANAConfiguration,
                                  tableName: String,
                                  iterator: Iterator[SinkRecord],
                                  metaSchema: metaSchema,
                                  batchSize: Int): Unit = {
    var committed = false
    // Here we create a new connection because this piece of code is executed on the worker
    WithCloseables(getHANAJdbcClient(hanaConfiguration).getConnection)({ conn =>
      conn.setAutoCommit(false)

      WithCloseables(conn
        .prepareStatement(prepareInsertIntoStmt(tableName, metaSchema))) { stmt =>
        val fieldsValuesConverters = JdbcTypeConverter.getSinkRowDatatypesSetters(metaSchema.fields,
          stmt)
        for (batchRows <- iterator.grouped(batchSize)) {
          for (row <- batchRows) {
            val data = row.value().asInstanceOf[Struct]

            metaSchema.fields.zipWithIndex.foreach{
              case (field, i) =>
                fieldsValuesConverters(i)(data.get(field.name))
            }
            stmt.addBatch()
            
            /*row.toSeq.zipWithIndex.foreach {
              case (null, i) =>
                stmt.setNull(i + 1, JdbcTypeConverter
                  .convertToHANAType(metaSchema.fields(i).dataType))
              case (value, i) =>
                fieldsValuesConverters(i)(value)
            }
            stmt.addBatch()
            */
          }
          stmt.executeBatch()
          stmt.clearParameters()
        }
      }

      conn.commit()
      committed = true
    }, doClose = { conn => {
      if (!committed) {
        // Partial commit could have happened....rollback
        conn.rollback()
        conn.close()
      } else {
        /**
         * The stage cannot fail now as failure will re-trigger the inserts.
         * Do not propagate exceptions further.
         */
        log.info("Record inserted")
        try {
          conn.close()
        } catch {
          case e: Exception => log.warn("Transaction succeeded, but closing failed", e)
        }
      }
    }
    })
  }

  /**
   * Prepares an INSERT INTO statement for the give parameters.
   *
   * @param fullTableName The fully-qualified name of the table
   * @param metaSchema The Metadata schema
   * @return The prepared INSERT INTO statement as a [[String]]
   */
  private[client] def prepareInsertIntoStmt(fullTableName: String, metaSchema: metaSchema): String = {
    val fields = metaSchema.fields
    val columnNames = fields.map(field => s""""${field.name}"""").mkString(", ")
    val placeHolders = fields.map(field => s"""?""").mkString(", ")
    s"""INSERT INTO $fullTableName ($columnNames) VALUES ($placeHolders)"""
  }

}
