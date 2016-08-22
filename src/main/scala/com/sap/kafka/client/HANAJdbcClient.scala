package com.sap.kafka.client

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util

import com.sap.kafka.connect.sink.HANASinkRecordsCollector
import com.sap.kafka.utils.{SchemaBuilder, WithCloseables}
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

case class HANAJdbcClient(hanaConfiguration: HANAConfiguration)  {
  private val log: Logger = LoggerFactory.getLogger(classOf[HANAJdbcClient])

  protected val driver: String = "com.sap.db.jdbc.Driver"

  private val CONNECTION_FAIL_R = ".*Failed to open connection.*".r

  /**
   * Checks whether the provided exception is a connection opening failure one.
   * This method is marked as protected in order to be overrideable in the tests.
   *
   * @param ex The exception to check
   * @return `true` if the exception is a connection failure one, `false` otherwise
   */
  protected def isFailedToOpenConnectionException(ex: Throwable): Boolean = ex match {
    case e if e.getMessage == null => false
    case _: RuntimeException => CONNECTION_FAIL_R.pattern.matcher(ex.getMessage).matches()
    case _ => false
  }

  /**
   * Returns a fully qualified table name.
   *
   * @param namespace The table namespace
   * @param tableName The table name
   * @return the fully qualified table name
   */
  protected def tableWithNamespace(namespace: Option[String], tableName: String) =
    namespace match {
      case Some(value) => s""""$value"."$tableName""""
      case None => tableName
    }

  /**
   * Returns the connection URL. This method is marked as protected in order to be
   * overrideable in the tests.
   *
   * @return The HANA JDBC connection URL
   */
  protected def jdbcUrl: String = {
    val host = hanaConfiguration.host
    val instance = hanaConfiguration.instance
    val port = hanaConfiguration.port
    if (hanaConfiguration.tenantDB.isDefined) {
      // Connect to a tenant DB
      s"jdbc:sap://$host:3$instance$port/?databaseName=${hanaConfiguration.tenantDB.get}"
    } else {
      s"jdbc:sap://$host:3$instance$port"
    }
  }

  /**
   * Returns a JDBC connection object. This method is marked as protected in order to be
   * overrideable in the tests.
   *
   * @return The created JDBC [[Connection]] object
   */
   def getConnection: Connection = {
    Class.forName(driver)
    Try(DriverManager.getConnection(jdbcUrl, hanaConfiguration.user, hanaConfiguration.password))
    match {
      case Success(conn) => conn
      case Failure(ex) if isFailedToOpenConnectionException(ex) =>
        throw new HANAJdbcConnectionException(hanaConfiguration, s"Opening a connection failed", ex)
      case Failure(ex) =>
        /* Make a distinction between bad state and network failure */
        throw new HANAJdbcBadStateException(hanaConfiguration, "Cannot acquire a connection", ex)
    }
  }

  /**
   * Retrieves table metadata.
   *
   * @param tableName The table name
   * @param namespace The table namespace
   * @return A sequence of [[metaAttr]] objects (JDBC representation of the schema)
   */
   def getMetaData(tableName: String, namespace: Option[String]): Seq[metaAttr] = {
    val fullTableName = tableWithNamespace(namespace, tableName)
    WithCloseables(getConnection) { conn =>
      WithCloseables(conn.createStatement()) { stmt =>
        WithCloseables(stmt.executeQuery(s"SELECT * FROM $fullTableName LIMIT 0")) { rs =>
          val metadata = rs.getMetaData
          val columnCount = metadata.getColumnCount
          (1 to columnCount).map(col => metaAttr(
            metadata.getColumnName(col), metadata.getColumnType(col),
            metadata.isNullable(col), metadata.getPrecision(col),
            metadata.getScale(col), metadata.isSigned(col)))
        }
      }
    }
  }

  /**
   * Creates and loads a table in HANA.
   *
   * @param namespace (optional) The table namespace
   * @param tableName The name of the table.
   * @param tableSchema The schema of the table.
   * @param batchSize Batchsize for inserts
   * @param columnTable Create a columnStore table
   */
  def createTable(namespace: Option[String],
                  tableName: String,
                  tableSchema: metaSchema,
                  batchSize: Int,
                  columnTable: Boolean = false): Unit = {
    val testSchema = SchemaBuilder.avroToHANASchema(tableSchema)
    val fullTableName = tableWithNamespace(namespace, tableName)
    val VARCHAR_STAR_R = "(?i)varchar\\(\\*\\)".r
    // Varchar(*) is not supported by HANA
    val fixedSchema = VARCHAR_STAR_R.replaceAllIn(testSchema, "VARCHAR(1000)")
    val query = if (columnTable) s"""CREATE COLUMN TABLE $fullTableName ($fixedSchema) """
    else s"""CREATE TABLE $fullTableName ($fixedSchema) """
    val connection = getConnection
    val stmt = connection.createStatement()
    Try(stmt.execute(query)) match {
      case Failure(ex) => log.error("Error during table creation", ex)
        stmt.close()
        connection.close()
        throw ex
      case _ =>
        stmt.close()
        connection.close()
    }

  }

  /**
   * Checks if the given table name corresponds to an existent
   * table in the HANA backend within the provided namespace.
   *
   * @param namespace (optional) The table namespace
   * @param tableName The name of the table
   * @return `true`, if the table exists; `false`, otherwise
   */
  def tableExists(namespace: Option[String], tableName: String): Boolean =
    WithCloseables(getConnection) { conn =>
      val dbMetaData = conn.getMetaData
      val tables = dbMetaData.getTables(null, namespace.orNull, tableName, null)
      tables.next()
    }

  /**
   * Returns existing HANA tables which names match the provided pattern and which were created
   * within any schema matching the provided schema pattern.
   *
   * @param dbschemaPattern The schema patters
   * @param tableNamePattern The table name pattern
   * @return [[Success]] with a sequence of the matching tables' names or
   *         [[Failure]] if any error occurred
   */
  def getExistingTables(dbschemaPattern: String, tableNamePattern: String): Try[Seq[String]] =
    WithCloseables(getConnection) { conn =>
      WithCloseables(conn.createStatement()) { stmt =>
        Try {
          val rs = stmt.executeQuery(s"SELECT TABLE_NAME FROM M_TABLES " +
            s"WHERE SCHEMA_NAME LIKE '$dbschemaPattern' AND TABLE_NAME LIKE '$tableNamePattern'")
          val tablesNames = Stream.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
          rs.close()
          tablesNames
        }
      }
    }

  /**
    * Retrieves existing schema information for the given database and table name.
    *
    * @param dbschemaPattern The database schema to filter for.
    * @param tableNamePattern The table name to filter for.
    * @return The [[ColumnRow]]s returned by the query.
    */
  def getExistingSchemas(dbschemaPattern: String, tableNamePattern: String): Seq[ColumnRow] =
    WithCloseables(getConnection) { conn =>
      WithCloseables(conn.createStatement()) { stmt =>
        WithCloseables(stmt.executeQuery(
          s"""SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE_NAME,
              |LENGTH, SCALE, POSITION, IS_NULLABLE
              |FROM TABLE_COLUMNS
              |WHERE SCHEMA_NAME LIKE '$dbschemaPattern' AND TABLE_NAME LIKE '$tableNamePattern'
           """.stripMargin)) { rs =>
          val iterator = new Iterator[ResultSet] {
            def hasNext: Boolean = rs.next()
            def next(): ResultSet = rs
          }
          // scalastyle:off magic.number of the following numbers.
          iterator.map { rs =>
            ColumnRow(
              rs.getString(1),
              rs.getString(2),
              rs.getString(3),
              rs.getString(4),
              rs.getInt(5),
              rs.getInt(6),
              rs.getInt(7),
              rs.getBoolean(8))
          }.toList
          // scalastyle:on magic.number
        }
      }
    }

  /**
   * Drops a table from HANA.
   *
   * @param namespace (optional) The table namespace
   * @param tableName The name of the table to drop
   */
  def dropTable(namespace: Option[String], tableName: String): Unit =
    WithCloseables(getConnection) { conn =>
      WithCloseables(conn.createStatement()) { stmt =>
        val fullTableName = tableWithNamespace(namespace, tableName)
        Try(stmt.execute(s"""DROP TABLE $fullTableName""")) match {
          case Failure(ex) =>
            throw ex
          case _ =>
        }
      }
    }

 /* /**
   * Loads data to a table from a given [[DataFrame]].
   * The [[HANAConfiguration]] must be explicitly passed to this method
   * because it is executed on the workers and the configuration object
   * is provided by the HANA relation.
   *
   * @param hanaConfiguration The HANA connection configuration
   * @param namespace (optional) The table namespace
   * @param tableName The table name to which the [[DataFrame]] is loaded
   * @param data The [[DataFrame]] to load
   * @param batchSize The batch size parameter
   */
   def loadData(hanaConfiguration: HANAConfiguration,
                             namespace: Option[String],
                             tableName: String,
                             data: DataFrame,
                             batchSize: Int): Unit = {
    val schema = data.schema
    val fullTableName = tableWithNamespace(namespace, tableName)
    data.foreachPartition { iterator =>
      HANAPartitionLoader.loadPartition(hanaConfiguration, fullTableName, iterator, schema,
        batchSize)
    }
  }*/

}
