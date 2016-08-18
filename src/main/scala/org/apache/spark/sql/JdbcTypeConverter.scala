package org.apache.spark.sql

import java.sql.{PreparedStatement, ResultSet}

import org.apache.spark.sql.types._
import com.sap.kafka.client.metaAttr

/**
 * Provides methods to convert a JDBC [[ResultSet]] to a Spark [[Row]] with the provided types.
 *
 * @param expectedTypes Types expected in the [[ResultSet]]
 */
class JdbcTypeConverter(val expectedTypes: Seq[DataType]) {

  /**
   * Given a [[ResultSet]] convert it to [[Row]]. The iterator must be iterated externally.
   *
   * @param rs The [[ResultSet]] object to convert
   * @return a [[Row]] object with the [[ResultSet]] content.
   */
  def convert(rs: ResultSet): Row = JdbcTypeConverter.convertResultSetEntry(rs, expectedTypes)

}

object JdbcTypeConverter {

  private final val CONST_TYPE_SMALLDECIMAL = 3000

  /**
   * Converts a HANA SQL attribute to the most compatible Spark [[DataType]].
   *
   * @param attr The attribute to convert
   * @return The converted [[DataType]]
   */
  // scalastyle:off cyclomatic.complexity
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
  // scalastyle:on cyclomatic.complexity

  /**
   * Converts a sequence of HANA SQL attributes to the most compatible Spark [[DataType]]s.
   *
   * @param attributes The attributes to convert
   * @return The converted [[DataType]]s
   */
  def getSchema(attributes: Seq[metaAttr]): StructType =
    StructType(attributes.map(a => StructField(a.name, convertToSparkType(a), a.isNullable == 1)))

  /**
   * Converts an entry from a [[ResultSet]] currently pointed by a cursor to a Spark [[Row]].
   *
   * @param rs The [[ResultSet]] with entries to convert
   * @param expectedTypes The expected Spark types
   * @return A [[Row]] object
   */
  // scalastyle:off cyclomatic.complexity
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

  /**
   * Converts a Spark datatype to the most compatible HANA SQL datatype.
   *
   * @param sparkType The Spark type to convert
   * @return The converted HANA datatype as [[java.sql.Types]]
   */
  // scalastyle:off cyclomatic.complexity
  def convertToHANAType(sparkType: DataType): Int = sparkType match {
      case IntegerType => java.sql.Types.INTEGER
      case LongType => java.sql.Types.BIGINT
      case DoubleType => java.sql.Types.DOUBLE
      case FloatType => java.sql.Types.REAL
      case ShortType => java.sql.Types.INTEGER
      case ByteType => java.sql.Types.INTEGER
      case BooleanType => java.sql.Types.BIT
      case StringType => java.sql.Types.CLOB
      case BinaryType => java.sql.Types.BLOB
      case TimestampType => java.sql.Types.TIMESTAMP
      case DateType => java.sql.Types.DATE
      case DecimalType.Unlimited => java.sql.Types.DECIMAL
      case DecimalType.Fixed(p, s) => java.sql.Types.DECIMAL
      case _ => sys.error(s"Unsupported Spark type: $sparkType")
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
  def getSparkRowDatatypesSetters(datatypes: Seq[StructField], stmt: PreparedStatement):
  Seq[(Any) => Unit] = datatypes.zipWithIndex.map({case (t, i) => t.dataType match {
      case LongType => (value: Any) => stmt.setLong(i + 1, value.asInstanceOf[Long])
      case FloatType => (value: Any) => stmt.setFloat(i + 1, value.asInstanceOf[Float])
      case ShortType => (value: Any) => stmt.setInt(i + 1, value.asInstanceOf[Short])
      case ByteType => (value: Any) => stmt.setInt(i + 1, value.asInstanceOf[Byte])
      case IntegerType => (value: Any) => stmt.setInt(i + 1, value.asInstanceOf[Int])
      case BooleanType => (value: Any) => stmt.setBoolean(i + 1, value.asInstanceOf[Boolean])
      case DoubleType => (value: Any) => stmt.setDouble(i + 1, value.asInstanceOf[Double])
      case StringType => (value: Any) => stmt.setString(i + 1, value.asInstanceOf[String])
      case BinaryType => (value: Any) => stmt.setBytes(i + 1, value.asInstanceOf[Array[Byte]])
      case TimestampType =>
        (value: Any) => stmt.setTimestamp(i + 1, value.asInstanceOf[java.sql.Timestamp])
      case DateType => (value: Any) => stmt.setDate(i + 1, value.asInstanceOf[java.sql.Date])
      case DecimalType.Unlimited => (value: Any) => stmt.setBigDecimal(i + 1,
          value.asInstanceOf[java.math.BigDecimal])
      case other =>
        if (other.isInstanceOf[DecimalType]) {
          (value: Any) => stmt.setBigDecimal(i + 1, value.asInstanceOf[java.math.BigDecimal])
        } else {
          (value: Any) =>
            sys.error(s"Unable to translate the non-null value for the field $i")
        }
    }})
  // scalastyle:on cyclomatic.complexity

}
