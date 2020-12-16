package org.apache.spark.sql.itsumma.gpconnector

import java.sql.{Connection, Date, JDBCType, ResultSetMetaData, SQLException, Timestamp}
import java.text.Format
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal._

import com.itsumma.gpconnector.GPClient
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import SparkSchemaUtil.rightPadWithChar
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

//import org.apache.commons.lang.StringUtils.rightPad
import SparkSchemaUtil.{escapeKey, isClass}

object SparkSchemaUtil {

  val ESCAPE_CHARACTER = "\""
  val NAME_SEPARATOR = "."

  def getGreenplumTableColumns(schema: StructType, withTypes: Boolean): String = {
    val columns = new StringBuilder("")
    var i: Int = 0
    schema.foreach(f => {
      if (i > 0) columns.append(", ")
      columns.append(f.name)
      if (withTypes) {
        val dt: String =  f.dataType match {
          case StringType => "TEXT"
          case IntegerType => "INTEGER"
          case LongType => "BIGINT"
          case DoubleType => "DOUBLE PRECISION"
          case FloatType => "REAL"
          case ShortType => "INTEGER"
          case ByteType => "BYTE"
          case BooleanType => "BIT(1)"
          case BinaryType => "BLOB"
          case TimestampType => "TIMESTAMP"
          case DateType => "DATE"
          case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
        }
        columns.append(" ")
        columns.append(dt)
      }
      i += 1
    })
    columns.toString()
  }

  def getGreenplumTableSchema(optionsFactory: GPOptionsFactory,
                              tableOrQuery: String,
                              alwaysNullable: Boolean = false): StructType = {
    using(getConn(optionsFactory)) { conn => {
      JdbcUtils.getSchemaOption(conn, optionsFactory.getJDBCOptions(tableOrQuery)) match {
        case Some(schema) => schema
        case None => {
          var sql: String = tableOrQuery
          if (!tableOrQuery.contains(" ") && GPClient.tableExists(conn, tableOrQuery)) {
            sql = s"select * from $tableOrQuery"
          }
          using(conn.prepareStatement(sql)) {
            stmt => {
              val rsmd = stmt.getMetaData
              val colCount = rsmd.getColumnCount
              val fields: Array[StructField] = new Array[StructField](colCount)
              var i: Int = 0
              val dialect: JdbcDialect = JdbcDialects.get(optionsFactory.getJDBCOptions("--").url)
              while (i < colCount) {
                val columnName = rsmd.getColumnLabel(i + 1)
                val dataType = rsmd.getColumnType(i + 1)
                val typeName = rsmd.getColumnTypeName(i + 1)
                val fieldSize = rsmd.getPrecision(i + 1)
                val fieldScale = rsmd.getScale(i + 1)
                val isSigned = rsmd.isSigned(i + 1)
                val nullable = if (alwaysNullable) {
                  true
                } else {
                  rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
                }
                val metadata = new MetadataBuilder().putLong("scale", fieldScale)
                val columnType =
                  dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
                    getCatalystType(dataType, fieldSize, fieldScale, isSigned))
                fields(i) = StructField(columnName, columnType, nullable)
                i += 1
              }
              new StructType(fields)
            }
          }
        }
      }
    }
    }
  }

  def getConn(optionsFactory: GPOptionsFactory): Connection =
    JdbcUtils.createConnectionFactory(optionsFactory.getJDBCOptions("--"))()

  def using[A, B <: {
    def close(): Unit
  }](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }

  /**
   * Escapes the given argument with {@value #ESCAPE_CHARACTER}
   *
   * @param argument any non null value.
   * @return
   */
  def getEscapedArgument(argument: String): String = {
    if (argument == null)
      new NullPointerException("Argument passed cannot be null")
    ESCAPE_CHARACTER + argument + ESCAPE_CHARACTER
  }

  def getEscapedFullColumnName(fullColumnName: String): String = {
    if (fullColumnName.startsWith(ESCAPE_CHARACTER)) return fullColumnName
    var index = fullColumnName.indexOf(NAME_SEPARATOR)
    if (index < 0) return getEscapedArgument(fullColumnName)
    val columnFamily = fullColumnName.substring(0, index)
    val columnName = fullColumnName.substring(index + 1)
    getEscapedArgument(columnFamily) + NAME_SEPARATOR + getEscapedArgument(columnName)
  }

  def stripChars(s: String, ch: String) = s filterNot (ch contains _)

  // Helper function to escape column key to work with SQL queries
  private def escapeKey(key: String): String = getEscapedFullColumnName(key)

  private def isClass(obj: Any, className: String) = {
    className.equals(obj.getClass().getName())
  }

  /**
   * Maps a JDBC type of some generic database to a Catalyst type.  This function is called only when
   * the JdbcDialect class corresponding to Postgres database driver returns null.
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
  private def getCatalystType(
                               sqlType: Int,
                               precision: Int,
                               scale: Int,
                               signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.DECIMAL       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType.bounded(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.REF_CURSOR    => null
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType
      case java.sql.Types.STRUCT        => StringType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE
      => null
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      => null
      case java.sql.Types.TINYINT       => IntegerType
      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
      case _                            =>
        throw new SQLException("Unrecognized SQL type " + sqlType)
      // scalastyle:on
    }

    if (answer == null) {
      throw new SQLException("Unsupported type " + JDBCType.valueOf(sqlType).getName)
    }
    answer
  }

  def repeatChar(c: Char, n: Int): String = c.toString * n

  def rightPadWithChar(str: String, num: Int, c: Char): String = {
    val len = num - str.length
    if (len <= 0)
      return str
    str + repeatChar(c, len)
  }

}

case class SparkSchemaUtil(dbTimeZoneName: String = java.time.ZoneId.systemDefault.toString) {
  val DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSS]"
  val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
  val GP_TIMESTAMP_FORMAT = "yyyy-mm-dd hh24:mi:ss.us"
  val GP_DATE_FORMAT = "yyyy-mm-dd"
  val dbTimeZoneId: ZoneId = ZoneId.of(dbTimeZoneName) // e.g. "Asia/Irkutsk"
  val timestampParseFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_TIME_FORMAT)
  val timestampParseFormatTz: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_TIME_FORMAT).withZone(dbTimeZoneId)
  val dateFormatter: Format = FastDateFormat.getInstance(DEFAULT_DATE_FORMAT)
  val dateParseFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT)

  private def getTimestampString(timestampValue: java.sql.Timestamp): String = {
    "TO_TIMESTAMP('%s', '%s')".format(
      //timeformatter.format(timestampValue),
      timestampParseFormat.format(timestampValue.toLocalDateTime),
      GP_TIMESTAMP_FORMAT)
  }

  private def getDateString(dateValue: java.sql.Date): String = {
    "TO_DATE('%s', '%s')".format(
      dateFormatter.format(dateValue),
      //dateParseFormat.format(dateValue.toLocalDate),
      GP_DATE_FORMAT)
  }

  // Helper function to escape string values in SQL queries
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${StringEscapeUtils.escapeSql(stringValue)}'"

    case timestampValue: Timestamp => getTimestampString(timestampValue)

    case dateValue: Date => getDateString(dateValue)

    case utf if (isClass(utf, "org.apache.spark.sql.types.UTF8String")) => s"'${StringEscapeUtils.escapeSql(utf.toString)}'"
    // Spark 1.5
    case utf if (isClass(utf, "org.apache.spark.unsafe.types.UTF8String")) => s"'${StringEscapeUtils.escapeSql(utf.toString)}'"

    // Pass through anything else
    case _ => value
  }

  /**
   * Attempt to create Postgres-accepted WHERE clause from Spark filters,
   *
   * @return tuple representing where clause (derived from supported filters),
   *         array of unsupported filters and array of supported filters
   */
  def pushFilters(filters: Array[Filter]): (String, Array[Filter], Array[Filter]) = {
    if (filters.isEmpty) {
      return ("", Array[Filter](), Array[Filter]())
    }

    val filter = new StringBuilder("")
    val unsupportedFilters = Array[Filter]();
    var i = 0

    filters.foreach(f => {
      // Assume conjunction for multiple filters, unless otherwise specified
      if (i > 0) {
        filter.append(" AND")
      }

      f match {
        // Spark 1.3.1+ supported filters
        case And(leftFilter, rightFilter) => {
          val (whereClause, currUnsupportedFilters, _) = pushFilters(Array(leftFilter, rightFilter))
          if (currUnsupportedFilters.isEmpty)
            filter.append(whereClause)
          else
            unsupportedFilters :+ f
        }
        case Or(leftFilter, rightFilter) => {
          val (whereLeftClause, leftUnsupportedFilters, _) = pushFilters(Array(leftFilter))
          val (whereRightClause, rightUnsupportedFilters, _) = pushFilters(Array(rightFilter))
          if (leftUnsupportedFilters.isEmpty && rightUnsupportedFilters.isEmpty) {
            filter.append(whereLeftClause + " OR " + whereRightClause)
          }
          else {
            unsupportedFilters :+ f
          }
        }
        case Not(aFilter) => {
          val (whereClause, currUnsupportedFilters, _) = pushFilters(Array(aFilter))
          if (currUnsupportedFilters.isEmpty)
            filter.append(" NOT " + whereClause)
          else
            unsupportedFilters :+ f
        }
        case EqualTo(attr, value) => filter.append(s" ${escapeKey(attr)} = ${compileValue(value)}")
        case GreaterThan(attr, value) => filter.append(s" ${escapeKey(attr)} > ${compileValue(value)}")
        case GreaterThanOrEqual(attr, value) => filter.append(s" ${escapeKey(attr)} >= ${compileValue(value)}")
        case LessThan(attr, value) => filter.append(s" ${escapeKey(attr)} < ${compileValue(value)}")
        case LessThanOrEqual(attr, value) => filter.append(s" ${escapeKey(attr)} <= ${compileValue(value)}")
        case IsNull(attr) => filter.append(s" ${escapeKey(attr)} IS NULL")
        case IsNotNull(attr) => filter.append(s" ${escapeKey(attr)} IS NOT NULL")
        case In(attr, values) => filter.append(s" ${escapeKey(attr)} IN ${values.map(compileValue).mkString("(", ",", ")")}")
        case StringStartsWith(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue(value + "%")}")
        case StringEndsWith(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue("%" + value)}")
        case StringContains(attr, value) => filter.append(s" ${escapeKey(attr)} LIKE ${compileValue("%" + value + "%")}")
        case _ => unsupportedFilters :+ f
      }

      i = i + 1
    })

    (filter.toString(), unsupportedFilters, filters diff unsupportedFilters)
  }

  ///TODO: Add more datatypes, e.g. ArrayType(et, _),
  // see https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala
  def textToInternalRow(schema: StructType, fields: Array[String]): InternalRow = {
    if (schema.fields.length != fields.length)
      throw new SQLException(s"schema.size=${schema.fields.length}, but ${fields.length} data columns received")
    val row = new SpecificInternalRow(schema.fields.map(x => x.dataType))
    fields.zipWithIndex.foreach{ case(txt ,i) => {
      val isNull = txt.toLowerCase.equals("null") || txt.length == 0
      schema.fields(i).dataType match {
        case StringType => if (!isNull) row.update(i, UTF8String.fromString(txt)) else row.update(i, UTF8String.fromString(""))
        case DoubleType => if (!isNull) row.setDouble(i, txt.toDouble) else row.setDouble(i, 0.0)
        case FloatType => if (!isNull) row.setFloat(i, txt.toFloat) else row.setFloat(i, 0.0f)
        case IntegerType => if (!isNull) row.setInt(i, txt.toInt) else row.setInt(i, 0)
        case LongType => if (!isNull) row.setLong(i, txt.toLong) else row.setLong(i, 0L)
        case ShortType => if (!isNull) row.setShort(i, txt.toShort) else row.setShort(i, 0)
        case ByteType => if (!isNull) row.update(i, txt.toByte) else row.update(i, 0)
        case TimestampType => {
          if (isNull) {
            row.setLong(i, 0)
          } else {
            try {
              var data = txt
              if (txt.length > 19)
                data = rightPadWithChar(txt, 26, '0')
              val zdt = timestampParseFormatTz.parse(data)
              val epochTime = zdt.getLong(ChronoField.INSTANT_SECONDS) * 1000000L + zdt.getLong(ChronoField.MICRO_OF_SECOND)
              row.setLong(i, epochTime)
            } catch {
              case e: java.time.format.DateTimeParseException =>
                throw new SQLException(s"col=${i.toString}: invalid timestamp string ${fields.mkString("(", "|", ")")}")
            }
          }
        }
        case DateType => if (!isNull) row.setInt(i, txt.toInt) else row.setInt(i, 0)
          if (isNull) {
            row.setInt(i, 0)
          } else {
            val ld = dateParseFormat.parse(txt)
            val epochDay = ld.getLong(ChronoField.EPOCH_DAY)
            row.setInt(i, epochDay.toInt)
          }
        case BooleanType => if (!isNull) row.setBoolean(i, txt.toBoolean) else row.setBoolean(i,false)
        case DecimalType.Fixed(p, s) =>
          if (isNull) {
            row.update(i, BigDecimal.valueOf(0L))
          } else {
            val decimal = BigDecimal.apply(txt)
            row.update(i, Decimal(decimal, p, s))
          }
        /*
        case dt: DecimalType =>
          if (isNull) {
            row.update(i, BigDecimal.valueOf(0L))
          } else {
            val decimal = BigDecimal.apply(txt)
            row.update(i, Decimal(decimal, dt.precision, dt.scale))
          }
        */
        case _ => throw new IllegalArgumentException(s"Unsupported type ${schema.fields(i).dataType.catalogString}")
      }
      if (isNull)
        row.setNullAt(i)
    }}
    row
  }
}
