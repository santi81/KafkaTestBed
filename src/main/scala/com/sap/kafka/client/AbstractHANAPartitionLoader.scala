package com.sap.kafka.client

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.sap.kafka.utils.WithCloseables
import org.apache.spark.sql.JdbcTypeConverter
trait AbstractHANAPartitionLoader extends Logging {

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
   * @param rddSchema The RDD schema
   * @param batchSize The batch size
   */
  private[client] def loadPartition(hanaConfiguration: HANAConfiguration,
                                  tableName: String,
                                  iterator: Iterator[Row],
                                  rddSchema: StructType,
                                  batchSize: Int): Unit = {
    var committed = false
    // Here we create a new connection because this piece of code is executed on the worker
    WithCloseables(getHANAJdbcClient(hanaConfiguration).getConnection)({ conn =>
      conn.setAutoCommit(false)

      WithCloseables(conn
        .prepareStatement(prepareInsertIntoStmt(tableName, rddSchema))) { stmt =>
        val fieldsValuesConverters = JdbcTypeConverter.getSparkRowDatatypesSetters(rddSchema.fields,
          stmt)
        for (batchRows <- iterator.grouped(batchSize)) {
          for (row <- batchRows) {
            row.toSeq.zipWithIndex.foreach {
              case (null, i) =>
                stmt.setNull(i + 1, JdbcTypeConverter
                  .convertToHANAType(rddSchema.fields(i).dataType))
              case (value, i) =>
                fieldsValuesConverters(i)(value)
            }
            stmt.addBatch()
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
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
    })
  }

  /**
   * Prepares an INSERT INTO statement for the give parameters.
   *
   * @param fullTableName The fully-qualified name of the table
   * @param rddSchema The RDD schema
   * @return The prepared INSERT INTO statement as a [[String]]
   */
  private[client] def prepareInsertIntoStmt(fullTableName: String, rddSchema: StructType): String = {
    val fields = rddSchema.fields
    val columnNames = fields.map(field => s""""${field.name}"""").mkString(", ")
    val placeHolders = fields.map(field => s"""?""").mkString(", ")
    s"""INSERT INTO $fullTableName ($columnNames) VALUES ($placeHolders)"""
  }

}
