package com.sap.kafka.utils

import java.sql.{PreparedStatement, ResultSet}

import com.sap.kafka.client.metaAttr
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type

/**
 * Provides methods to convert a JDBC [[ResultSet]] to a Avro [[Row]] with the provided types.
 *
 * @param expectedTypes Types expected in the [[ResultSet]]
 */
class JdbcTypeConverter(val expectedTypes: Seq[metaAttr]) {

  /**
   * Given a [[ResultSet]] convert it to [[Row]]. The iterator must be iterated externally.
   *
   * @param rs The [[ResultSet]] object to convert
   * @return a [[Row]] object with the [[ResultSet]] content.
   */
  // def convert(rs: ResultSet): Row = JdbcTypeConverter.convertResultSetEntry(rs, expectedTypes)

}

object JdbcTypeConverter {

  private final val CONST_TYPE_SMALLDECIMAL = 3000

  /**
   * Converts a HANA SQL attribute to the most compatible AVRO [[DataType]].
   *
   * @param attr The attribute to convert
   * @return The converted [[DataType]]
   */
  // scalastyle:off cyclomatic.complexity
  /*
  def convertToSparkType(attr: metaAttr): DataType = {
    attr.dataType match {
      // Datetime types
      case java.sql.Types.DATE => DateType // id:91
      case java.sql.Types.TIME => TimestampType // id:92
      case java.sql.Types.TIMESTAMP => TimestampType // id:93

      // Character string types
      case java.sql.Types.VARCHAR | java.sql.Types.NVARCHAR |
           java.sql.Types.NCHAR | java.sql.Types.CHAR |
           java.sql.Types.NCLOB | java.sql.Types.CLOB => StringType // id:12|-9|-8|1

      // Numeric types
      case java.sql.Types.TINYINT | java.sql.Types.SMALLINT |
           java.sql.Types.INTEGER => IntegerType // id:-6|5|4 ... HANA only supports Signed Intger
      case java.sql.Types.BIGINT => LongType // id:-5 ...HANA only supports Signed BIGINT
      case java.sql.Types.DECIMAL | CONST_TYPE_SMALLDECIMAL =>
        val precision = attr.precision
        val scale = attr.scale
        if (precision != 0 || scale != 0) DecimalType(precision, scale)
        else DecimalType.Unlimited // id:3 : 3000

      case java.sql.Types.REAL => FloatType // id:7 ... REAL is 4 bytes ..can map to Float
      case java.sql.Types.DOUBLE => DoubleType // id:8

      // Boolean Types
      case java.sql.Types.BOOLEAN => BooleanType

      // Binary Types
      case java.sql.Types.VARBINARY | java.sql.Types.BINARY => BinaryType // id : -3|-2

      // Unsupported Types
      case other => sys.error(s"Unsupported JDBC type: $other")
    }
  }
  */
  // scalastyle:on cyclomatic.complexity

  /**
   * Converts a sequence of HANA SQL attributes to the most compatible Spark [[DataType]]s.
   *
   * @param attributes The attributes to convert
   * @return The converted [[DataType]]s
   */
  /*
  def getSchema(attributes: Seq[metaAttr]): StructType =
    StructType(attributes.map(a => StructField(a.name, convertToSparkType(a), a.isNullable == 1)))

  /**
   * Converts an entry from a [[ResultSet]] currently pointed by a cursor to a Spark [[Row]].
   *
   * @param rs The [[ResultSet]] with entries to convert
   * @param expectedTypes The expected Spark types
   * @return A [[Row]] object
   */
   **/
  // scalastyle:off cyclomatic.complexity
  /*
  def convertResultSetEntry(rs: ResultSet, expectedTypes: Seq[DataType]): Row =
    Row.fromSeq(expectedTypes.zipWithIndex.map({ case (t, i) =>
      // Columns in the JDBC [[ResultSet]] start from 1
      val pos = i + 1
      val value = t match {
        case BooleanType => rs.getBoolean(pos)
        case DateType => rs.getDate(pos)
        case DecimalType.Unlimited =>
          val decimalVal = rs.getBigDecimal(pos)
          if (decimalVal == null) {
            null
          } else {
            Decimal(decimalVal)
          }
        case DecimalType.Fixed(p, s) =>
          val decimalVal = rs.getBigDecimal(pos)
          if (decimalVal == null) {
            null
          } else {
            Decimal(decimalVal, p, s)
          }
        case DoubleType => rs.getDouble(pos)
        case FloatType => rs.getFloat(pos)
        case IntegerType => rs.getInt(pos)
        case LongType => rs.getLong(pos)
        case StringType => rs.getString(pos)
        case TimestampType => rs.getTimestamp(pos)
        case BinaryType => rs.getBytes(pos)
        case _ => sys.error(s"Unsupported Spark type: $t")
      }
      if (rs.wasNull()) null else value
    }))
  // scalastyle:on cyclomatic.complexity
*/
  /**
   * Converts a Spark datatype to the most compatible HANA SQL datatype.
   *
   * @param schemaType The Spark type to convert
   * @return The converted HANA datatype as [[java.sql.Types]]
   */
  // scalastyle:off cyclomatic.complexity
  def convertToHANAType(schemaType: Type): Int = schemaType match {
      case Schema.Type.INT8 => java.sql.Types.INTEGER
      case Schema.Type.INT16 => java.sql.Types.INTEGER
      case Schema.Type.INT32 => java.sql.Types.INTEGER
      case Schema.Type.INT64 => java.sql.Types.BIGINT
      case Schema.Type.FLOAT64 => java.sql.Types.DOUBLE
      case Schema.Type.FLOAT32 => java.sql.Types.REAL
      case Schema.Type.BOOLEAN => java.sql.Types.BIT
      case Schema.Type.STRING => java.sql.Types.VARCHAR
      case Schema.Type.BYTES => java.sql.Types.BLOB
      case _ => sys.error(s"Unsupported Avro type: $schemaType")
    }
  // scalastyle:on cyclomatic.complexity

  /**
   * Generates a sequence of setters which set values in the provided [[PreparedStatement]]
   * object using a proper setter method for each value datatype.
   *
   * @param datatypes The datatypes of the values
   * @param stmt The [[PreparedStatement]] object on which the setters are supposed to be called
   * @return A sequence of setter functions which argument is the value to be set
   *         of the type [[Any]]
   */
  // scalastyle:off cyclomatic.complexity
  def getSinkRowDatatypesSetters(datatypes: Seq[metaAttr], stmt: PreparedStatement):
  Seq[(Any) => Unit] = datatypes.zipWithIndex.map({case (t, i) => t.dataType match {
      case java.sql.Types.INTEGER => (value: Any) => stmt.setInt(i + 1, value.asInstanceOf[Int])
      case java.sql.Types.BIT => (value: Any) => stmt.setBoolean(i + 1, value.asInstanceOf[Boolean])
      case java.sql.Types.DOUBLE => (value: Any) => stmt.setDouble(i + 1, value.asInstanceOf[Double])
      case java.sql.Types.VARCHAR => (value: Any) => stmt.setString(i + 1, value.asInstanceOf[String])
      case other =>
          (value: Any) =>
            sys.error(s"Unable to translate the non-null value for the field $i")
    }})
  // scalastyle:on cyclomatic.complexity

}
