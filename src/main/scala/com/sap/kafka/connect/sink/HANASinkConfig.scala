package com.sap.kafka.connect.sink

import scala.collection.JavaConverters._
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException}

class HANASinkConfig(CONFIG_DEF: ConfigDef, props: java.util.Map[Any, Any]) extends AbstractConfig(CONFIG_DEF, props) {
  val connectionUrl = getString(HANASinkConfig.CONNECTION_URL)
  val connectionUser = getString(HANASinkConfig.CONNECTION_USER)
  val connectionPassword = getString(HANASinkConfig.CONNECTION_PASSWORD)
  val tableNameFormat = getString(HANASinkConfig.TABLE_NAME_FORMAT).trim()
  val batchSize = getInt(HANASinkConfig.BATCH_SIZE)
  val maxRetries = getInt(HANASinkConfig.MAX_RETRIES)
  val retryBackoffMs = getInt(HANASinkConfig.RETRY_BACKOFF_MS)
  val autoCreate = getBoolean(HANASinkConfig.AUTO_CREATE)
  val autoEvolve = getBoolean(HANASinkConfig.AUTO_EVOLVE)
  val insertMode = HANASinkConfig.InsertMode.withName(getString(HANASinkConfig.INSERT_MODE).toUpperCase())
  val pkMode = HANASinkConfig.PrimaryKeyMode.withName(getString(HANASinkConfig.PK_MODE).toUpperCase())
  val pkFields = getList(HANASinkConfig.PK_FIELDS)
}

object HANASinkConfig {
  object InsertMode extends Enumeration {
    type InsertMode = Value
    val INSERT, UPSERT = Value
  }

  object PrimaryKeyMode extends Enumeration {
    type PrimaryKeyMode = Value
    val NONE, RECORD_KEY, RECORD_VALUE = Value
  }

  val CONNECTION_URL = "connection.url"
  val CONNECTION_URL_DOC = "JDBC connection URL."
  val CONNECTION_URL_DISPLAY = "JDBC URL"

  val CONNECTION_USER = "connection.user"
  val CONNECTION_USER_DOC = "JDBC connection user."
  val CONNECTION_USER_DISPLAY = "JDBC user."

  val CONNECTION_PASSWORD = "connection.password"
  val CONNECTION_PASSWORD_DOC = "JDBC connection password."
  val CONNECTION_PASSWORD_DISPLAY = "JDBC Password"

  val TABLE_NAME_FORMAT = "table.name.format"
  val TABLE_NAME_FORMAT_DEFAULT = "${namespace}::${topic}"
  val TABLE_NAME_FORMAT_DOC =
    "A format string for the destination table name, which may be of format '${namespace}::${topic}'.\n"
  val TABLE_NAME_FORMAT_DISPLAY = "Table Name Format"

  val MAX_RETRIES = "max.retries"
  val MAX_RETRIES_DEFAULT = 10 
  val MAX_RETRIES_DOC =
    "The maximum number of times to retry on errors before failing the task."
  val MAX_RETRIES_DISPLAY = "Maximum Retries"

  val RETRY_BACKOFF_MS = "retry.backoff.ms"
  val RETRY_BACKOFF_MS_DEFAULT = 3000
  val RETRY_BACKOFF_MS_DOC =
    "The time in milliseconds to wait following an error before a retry attempt is made."
  val RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)"

  val BATCH_SIZE = "batch.size"
  val BATCH_SIZE_DEFAULT = 3000
  val BATCH_SIZE_DOC =
    "Specifies how many records to attempt to batch together for insertion into the destination table, when possible."
  val BATCH_SIZE_DISPLAY = "Batch Size"

  val AUTO_CREATE = "auto.create"
  val AUTO_CREATE_DEFAULT = "false"
  val AUTO_CREATE_DOC =
    "Whether to automatically create the destination table based on record schema if it is found to be missing by issuing ``CREATE``."
  val AUTO_CREATE_DISPLAY = "Auto-Create"

  val AUTO_EVOLVE = "auto.evolve"
  val AUTO_EVOLVE_DEFAULT = "false"
  val AUTO_EVOLVE_DOC =
    "Whether to automatically dd columns in the table schema when found to be missing relative to the record schema by issuing ``ALTER``."
  val AUTO_EVOLVE_DISPLAY = "Auto-Evolve"

  val INSERT_MODE = "insert.mode"
  val INSERT_MODE_DEFAULT = "insert"
  val INSERT_MODE_DOC =
    "The insertion mode to use. Supported modes are: INSERT & UPSERT"
  val INSERT_MODE_DISPLAY = "Insert Mode"

  val PK_MODE = "pk.mode"
  val PK_MODE_DEFAULT = "none"
  val PK_MODE_DOC =
    "The primary key mode, also refer to ``pk.fields`` documentation for interplay. Supported modes are: none, record_key"
  val PK_MODE_DISPLAY = "Primary Key Mode"

  val PK_FIELDS = "pk.fields"
  val PK_FIELDS_DEFAULT = ""
  val PK_FIELDS_DOC =
    "List of comma-separated primary key field names. The runtime interpretation of this config depends on the ``pk.mode``:\n" +
      "`none`\n" +
      "    Ignored as no fields are used as primary key in this mode.\n" +
      "`record_key`\n" +
      "    If empty, all fields from the key struct will be used, otherwise used to whitelist the desired fields - for primitive key only a single field name must be configured.\n" +
      "`record_value`\n" +
      "    If empty, all fields from the value struct will be used, otherwise used to whitelist the desired fields.\n"
  val PK_FIELDS_DISPLAY = "Primary Key Fields"

  val NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0)

  val CONNECTION_GROUP = "Connection"
  val WRITES_GROUP = "Writes"
  val PK_GROUP = "Primary Keys"
  val DDL_GROUP = "DDL Support"
  val RETRIES_GROUP = "Retries"

  val CONFIG_DEF = new ConfigDef()
    .define(CONNECTION_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
      ConfigDef.Importance.HIGH, CONNECTION_URL_DOC, CONNECTION_GROUP, 1, ConfigDef.Width.LONG, CONNECTION_URL_DISPLAY)
    .define(CONNECTION_USER, ConfigDef.Type.STRING, null,
      ConfigDef.Importance.HIGH, CONNECTION_USER_DOC, CONNECTION_GROUP, 2, ConfigDef.Width.MEDIUM, CONNECTION_USER_DISPLAY)
    .define(CONNECTION_PASSWORD, ConfigDef.Type.PASSWORD, null,
      ConfigDef.Importance.HIGH, CONNECTION_PASSWORD_DOC, CONNECTION_GROUP, 3, ConfigDef.Width.MEDIUM, CONNECTION_USER_DISPLAY)
    .define(TABLE_NAME_FORMAT, ConfigDef.Type.STRING, TABLE_NAME_FORMAT_DEFAULT,
      ConfigDef.Importance.MEDIUM, TABLE_NAME_FORMAT_DOC, WRITES_GROUP, 1, ConfigDef.Width.LONG, TABLE_NAME_FORMAT_DISPLAY)
    .define(INSERT_MODE, ConfigDef.Type.STRING, INSERT_MODE_DEFAULT,
      EnumValidator(Set()).in[InsertMode.InsertMode](InsertMode.values.toList), ConfigDef.Importance.MEDIUM, INSERT_MODE_DOC, WRITES_GROUP, 2,
      ConfigDef.Width.MEDIUM, INSERT_MODE_DISPLAY)
    .define(BATCH_SIZE, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT,
      NON_NEGATIVE_INT_VALIDATOR, ConfigDef.Importance.HIGH, BATCH_SIZE_DOC, WRITES_GROUP, 3,
      ConfigDef.Width.SHORT, BATCH_SIZE_DISPLAY)
    // Primary Keys
    .define(PK_MODE, ConfigDef.Type.STRING, PK_MODE_DEFAULT,
    EnumValidator(Set()).in[PrimaryKeyMode.PrimaryKeyMode](PrimaryKeyMode.values.toList), ConfigDef.Importance.HIGH, PK_MODE_DOC, PK_GROUP, 1,
    ConfigDef.Width.MEDIUM, PK_MODE_DISPLAY)
    .define(PK_FIELDS, ConfigDef.Type.LIST, PK_FIELDS_DEFAULT,
      ConfigDef.Importance.MEDIUM, PK_FIELDS_DOC, PK_GROUP, 2,
      ConfigDef.Width.LONG, PK_FIELDS_DISPLAY)
    // DDL
    .define(AUTO_CREATE, ConfigDef.Type.BOOLEAN, AUTO_CREATE_DEFAULT,
    ConfigDef.Importance.MEDIUM, AUTO_CREATE_DOC, DDL_GROUP, 1,
    ConfigDef.Width.SHORT, AUTO_CREATE_DISPLAY)
    .define(AUTO_EVOLVE, ConfigDef.Type.BOOLEAN, AUTO_EVOLVE_DEFAULT,
      ConfigDef.Importance.MEDIUM, AUTO_EVOLVE_DOC, DDL_GROUP, 2,
      ConfigDef.Width.SHORT, AUTO_EVOLVE_DISPLAY)
    // Retries
    .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
    ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC, RETRIES_GROUP, 1,
    ConfigDef.Width.SHORT, MAX_RETRIES_DISPLAY)
    .define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, RETRY_BACKOFF_MS_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
      ConfigDef.Importance.MEDIUM, RETRY_BACKOFF_MS_DOC, RETRIES_GROUP, 2,
      ConfigDef.Width.SHORT, RETRY_BACKOFF_MS_DISPLAY)

  case class EnumValidator(validValues: Set[String]) extends ConfigDef.Validator {

    def in[E](enumerators: List[E]): EnumValidator = {
      var values = Set[String]()
      enumerators.foreach(e => {
        values = values + e.toString.toUpperCase
        values = values + e.toString.toLowerCase
      })
      EnumValidator(values)
    }

    override def ensureValid(key: String, value: java.lang.Object): Unit = {
      if (!validValues.contains(value.asInstanceOf[String])) {
        throw new ConfigException(key, value, "Invalid enumerator")
      }
    }
  }

  def apply(props: Map[Any, Any]): HANASinkConfig = {
    new HANASinkConfig(CONFIG_DEF, props.asJava)
  }
}