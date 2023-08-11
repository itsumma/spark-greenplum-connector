package org.apache.spark.sql.itsumma.gpconnector

import java.sql.{Connection, Date, JDBCType, ResultSetMetaData, SQLException, Timestamp}
import java.text.Format
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal._
import com.itsumma.gpconnector.GPClient
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import SparkSchemaUtil.rightPadWithChar
import org.apache.spark.SparkContext
import org.apache.spark.sql.itsumma.gpconnector.GpTableTypes.GpTableType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.{HashMap => MutableHashMap}

//import org.apache.commons.lang.StringUtils.rightPad
import SparkSchemaUtil.{escapeKey, isClass}

case class GPColumnMeta(sqlTypeId: Int, dbTypeName: String, nullable: Int, colSize: Integer, decimalDigits: Integer) {}

object GpTableTypes extends Enumeration {
  /**
   * Denotes a type of Greenplum table, one of:
   * <p><b>None</b> - means "Unspecified"
   * <p><b>Target</b> - a table that can hold final source or destination data
   * <p><b>ExternalReadable</b> - GP readable external table for GPFDIST protocol
   * <p><b>ExternalWritable</b> - GP writable external table for GPFDIST protocol
   */
  type GpTableType = Value
  val None, Target, ExternalReadable, ExternalWritable = Value
}

object SparkSchemaUtil {

  val ESCAPE_CHARACTER = "\""
  val NAME_SEPARATOR = "."

  /**
   * Extracts Greenplum table columns metadata as a Map[column name String, {@link GPColumnMeta}]
   * @param connection: GP database JDBC connection
   * @param dbSchemaName: name of DB schema where target table resides
   * @param tableName: name of the target table
   * @return
   */
  def getGreenplumTableMeta(connection: Connection, dbSchemaName: String, tableName: String): Map[String, GPColumnMeta] = {
    val columnsMeta: MutableHashMap[String, GPColumnMeta] = MutableHashMap()
    val schemaPattern: String = if (dbSchemaName == null || dbSchemaName.isEmpty) null else dbSchemaName
    using(connection.getMetaData.getColumns(null, schemaPattern, tableName, null)) {
      rs => {
        while (rs.next()) {
          val colName = rs.getString("COLUMN_NAME")
          var precision: Integer = rs.getInt("COLUMN_SIZE")
          if (rs.wasNull())
            precision = null
          var scale: Integer = rs.getInt("DECIMAL_DIGITS")
          if (rs.wasNull())
            scale = null
          val colMeta = GPColumnMeta(rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME"),
            rs.getInt("NULLABLE"), precision, scale)
          columnsMeta.put(colName, colMeta)
        }
      }
    }
    columnsMeta.toMap
  }

  /**
   * Generates column list claus for
   * <p>   CREATE TABLE SQL operator - when <b>forCreateTable</b> is one of (Target, ExternalReadable, ExternalWritable),
   * <p>   or for
   * <p>   INSERT SQL operator - when <b>forCreateTable</b> is None
   * <p> In the case of CREATE TABLE operation, result contains corresponding column data type specifiers derived from
   * the spark data source schema given by the <b>schema</b> parameter. An optional <b>dbTableMeta</b>,
   *  parameter extracted from the Greenplum database object, if given, can be used to override data type of (some) columns
   *  matching by column name with accordance to DB preferences, thus providing a mean for data type mapping
   *  between Spark and GP data sources.
   * @param schema: {@link StructType} - Spark data source schema
   * @param forCreateTable: target table type ({@link GpTableType}): .None, ExternalReadable, ExternalWritable
   * @param dbTableMeta: optional GP source metadata, can contain subset of target columns that require type mapping
   * @return column list string
   */
  def getGreenplumTableColumns(schema: StructType, forCreateTable: GpTableType, dbTableMeta: Map[String, GPColumnMeta] = null
                              ): String = {
    val columns = new StringBuilder("")
    var i: Int = 0
    schema.foreach(f => {
      if (i > 0) columns.append(", ")
      columns.append(f.name)
      if (forCreateTable != GpTableTypes.None) {
        var dt: String =  f.dataType match {
          case StringType => "TEXT"
          case IntegerType => "INTEGER"
          case LongType => "BIGINT"
          case DoubleType => "DOUBLE PRECISION"
          case FloatType => "REAL"
          case ShortType => "INTEGER"
          case ByteType => "INTEGER" // type BYTE doesn't exists
          case BooleanType => "BOOLEAN"
          case BinaryType => {
            forCreateTable match {
              case GpTableTypes.ExternalReadable => "TEXT"
              case GpTableTypes.ExternalWritable => "TEXT"
              case _ => "BYTEA"
            }
          }
          case TimestampType => "TIMESTAMP"
          case DateType => "DATE"
          case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
        }
        // External table column types overriding corresponding to the existing target table
        if ((dbTableMeta != null) && dbTableMeta.contains(f.name)
          && ((dt == "TEXT") || (f.dataType == BooleanType) || (f.dataType == BinaryType))) {
          if (forCreateTable == GpTableTypes.ExternalReadable) {
            dbTableMeta.get(f.name) match {
              case Some(colMeta: GPColumnMeta) => {
                colMeta.dbTypeName.toUpperCase match {
                  case "UUID" => dt = "UUID"
                  case "VARCHAR" | "CHARACTER VARYING" | "CHARACTER" | "CHAR" => if (colMeta.colSize != null) dt = s"VARCHAR(${colMeta.colSize})"
                  case "INTEGER" => dt = "INTEGER"
                  case "BIGINT" => dt = "BIGINT"
                  case "DOUBLE PRECISION" => dt = "DOUBLE PRECISION"
                  case "REAL" => dt = "REAL"
                  // case "BYTE" => dt = "BYTE"
                  case "BIT" | "BIT VARYING" | "VARBIT" => {
                    dt = "VARCHAR(256)"
                    /*
                                      if (colMeta.colSize != null) {
                                        dt = s"BIT(${colMeta.colSize})"
                                      } else {
                                        dt = "BIT(1)"
                                      }
                    */
                  }
                  case "BOOLEAN" => dt = "BOOLEAN"
                  case "DECIMAL" => {
                    if ((colMeta.colSize != null) && (colMeta.decimalDigits != null))
                      dt = s"DECIMAL(${colMeta.colSize}.${colMeta.decimalDigits})"
                  }
                  case "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
                    if (colMeta.colSize != null) {
                      dt = s"TIMESTAMPTZ(${colMeta.colSize})"
                    } else {
                      dt = "TIMESTAMPTZ"
                    }
                  }
                  case "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
                    if (colMeta.colSize != null) {
                      dt = s"TIMESTAMP(${colMeta.colSize})"
                    } else {
                      dt = "TIMESTAMP"
                    }
                  }
                  case "TIME WITH TIME ZONE" => {
                    if (colMeta.colSize != null) {
                      dt = s"TIME (${colMeta.colSize}) WITH TIME ZONE"
                    } else {
                      dt = "TIME WITH TIME ZONE"
                    }
                  }
                  case "TIME" | "TIME WITHOUT TIME ZONE" => {
                    if (colMeta.colSize != null) {
                      dt = s"TIME(${colMeta.colSize})"
                    } else {
                      dt = "TIME"
                    }
                  }
                  case "DATE" => dt = "DATE"
                  case "GEOMETRY" => dt = "GEOMETRY"
                  case _ =>
                }
              }
              case None =>
            }
          }
        }
        columns.append(" ")
        columns.append(dt)
      }
      i += 1
    })
    columns.toString()
  }

  /**
   * Generates column list claus for
   * <p>   SELECT SQL operator - where <b>fromTableOfType</b> parameter can be one of (Target, ExternalReadable),
   * <p> with every column from
   * the spark data source schema given by the <b>schema</b> parameter.
   * <p> An optional <b>dbTableMeta</b>,
   * parameter extracted from the Greenplum database object, if given, can be used to override
   * a data type of (some) columns
   * matching by column name with accordance to DB preferences, thus providing a mean for data type mapping
   * between Spark and GP data sources.
   *
   * @param schema         : {@link StructType} - Spark data source schema
   * @param fromTableOfType : target table type ({@link GpTableType}), one of: None, ExternalReadable
   * @param dbTableMeta    : optional GP source metadata, can contain subset of target columns that require type mapping
   * @return column list string
   */
  def getGreenplumSelectColumns(schema: StructType, fromTableOfType: GpTableType, dbTableMeta: Map[String, GPColumnMeta] = null): String = {
    val columns = new StringBuilder("")
    var i: Int = 0
    schema.foreach(f => {
      if (i > 0)
        columns.append(", ")
      var dt: String = f.name
      if (fromTableOfType == GpTableTypes.ExternalReadable) {
        f.dataType match {
          case BinaryType => dt = s"decode(${f.name}, 'base64')"
          case BooleanType => {
            if ((dbTableMeta != null) && dbTableMeta.contains(f.name)) {
              dbTableMeta.get(f.name) match {
                case Some(colMeta) => {
                  if ((colMeta.dbTypeName.toUpperCase == "BIT") && (colMeta.colSize != null))
                    dt = s"${f.name}::bit(${colMeta.colSize})"
                }
                case None =>
              }
            }
          }
          case StringType => {
            if ((dbTableMeta != null) && dbTableMeta.contains(f.name)) {
              dbTableMeta.get(f.name) match {
                case Some(colMeta) => {
                  if ((colMeta.dbTypeName.toUpperCase == "BIT")
                  || (colMeta.dbTypeName.toUpperCase == "BIT VARYING")
                    || (colMeta.dbTypeName.toUpperCase == "VARBIT")) {
                    dt = s"${f.name}::varbit"
/*
                    if ((colMeta.colSize != null) && (colMeta.colSize > 0)) {
                      dt = s"${f.name}::bit(${colMeta.colSize})"
                    } else {
                      dt = s"${f.name}::varbit"
                    }
*/
                  } else if (colMeta.dbTypeName.toUpperCase == "JSON") {
                    dt = s"${f.name}::json"
                  } else if (colMeta.dbTypeName.toUpperCase == "JSONB") {
                    dt = s"${f.name}::jsonb"
                  }
                }
                case None =>
              }
            }
          }
          case _ =>
        }
      } else if (fromTableOfType == GpTableTypes.Target) {
        f.dataType match {
          case BinaryType => dt = s"encode(${f.name}, 'base64')"
          case _ =>
        }
      }
      columns.append(s"$dt as ${f.name}")
      i += 1
    })
    columns.toString()
  }

  /**
   * Produce dummy fake data scheme required for the count of rows operation.
   * @param optionsFactory
   * @return
   */
  def getGreenplumPlaceholderSchema(optionsFactory: GPOptionsFactory): StructType = {
    val fields: Array[StructField] = new Array[StructField](1)
    val dialect: JdbcDialect = JdbcDialects.get(optionsFactory.getJDBCOptions("--").url)
    val metadata = new MetadataBuilder().putLong("scale", 0)
    val columnType =
      dialect.getCatalystType(java.sql.Types.CHAR, "CHAR", 20, metadata).getOrElse(
        getCatalystType(java.sql.Types.CHAR, 20, 0, false))
    fields(0) = StructField("dummy", columnType, true)
    new StructType(fields)
  }

  /**
   * Extracts columns metadata list as {@link StructType} instance (i.e. Spark-native data schema representation)
   * from the GP data source given by <b>tableOrQuery</b> parameter,
   * <p>which can be a table name or SQL SELECT operator.
   * <p>No actual data result set is generated, the statement for <b>tableOrQuery</b> is only prepared.
   * <p> <b>Note</b>: this method will commit previous transaction (if any) on the <b>conn</b>
   * @param optionsFactory - can pass usual JDBC options influencing the statement prepare stage.
   * @param conn: GP DB connection
   * @param tableOrQuery: Greenplum table name or SELECT operator that can produce a result set
   * @param alwaysNullable optional: enforce all columns in the result to be marked as nullable
   * @return {@link StructType} instance with column list metadata
   */
  def getGreenplumTableSchema(optionsFactory: GPOptionsFactory,
                              conn: Connection,
                              tableOrQuery: String,
                              alwaysNullable: Boolean = false): StructType = {
    if (!conn.getAutoCommit)
      conn.commit()
    JdbcUtils.getSchemaOption(conn, optionsFactory.getJDBCOptions(tableOrQuery)) match {
      case Some(schema) => schema
      case None => {
        val targetType: GPTarget = GPTarget(tableOrQuery)
        var sql: String = tableOrQuery
        if (!targetType.isQuery) {
          if (GPClient.tableExists(conn, tableOrQuery)) sql = s"select * from $tableOrQuery"
          else return new StructType()
        }
        if (!conn.getAutoCommit)
          conn.commit()
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

/*
  private def getConn(optionsFactory: GPOptionsFactory): Connection =
    JdbcUtils.createConnectionFactory(optionsFactory.getJDBCOptions("--"))()
*/

  private def using[A, B <: {
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

  def stripChars(s: String, ch: String): String = s filterNot (ch contains _)

  // Helper function to escape column key to work with SQL queries
  private def escapeKey(key: String): String = getEscapedFullColumnName(key)

  private def isClass(obj: Any, className: String) = {
    className.equals(obj.getClass.getName)
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
      => StringType //null
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      => StringType //null
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

  def guessMaxParallelTasks(): Int = {
    val sparkContext = SparkContext.getOrCreate
    var guess: Int = -1
    while ((guess <= 0) && !Thread.currentThread().isInterrupted) {
      guess = sparkContext.getExecutorMemoryStatus.keys.size - 1
      if (sparkContext.deployMode == "cluster")
        guess -= 1
    }
    guess
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

  def internalRowToText(schema: StructType, row: InternalRow, fieldDelimiter: Char = '\t'): String = {
    if (schema.fields.length != row.numFields)
      throw new SQLException(s"internalRowToText: schema.size=${schema.fields.length}, but ${row.numFields} data columns received")
    val ret: StringBuilder = new StringBuilder("")
    schema.fields.zipWithIndex.foreach{
      case (field, i) => {
        var txt = "NULL"
        if (!row.isNullAt(i)) {
          field.dataType match {
            case StringType => {
              txt = StringEscapeUtils.escapeJava(row.getString(i)).replace("\\\"", "\"")
              if (fieldDelimiter != '\t')
                txt = txt.replace(s"${fieldDelimiter}", s"\\${fieldDelimiter}")
            }
            case DecimalType.Fixed(p, s) => {
              val decVal = row.getDecimal(i, p, s)
              txt = decVal.toString()
            }
            case DoubleType => txt = row.getDouble(i).toString
            case FloatType => txt = row.getFloat(i).toString
            case IntegerType => txt = row.getInt(i).toString
            case LongType => txt = row.getLong(i).toString
            case ShortType => txt = row.getShort(i).toString
            case ByteType => txt = row.getByte(i).toString
            case BooleanType => {
              txt = if (row.getBoolean(i)) "1" else "0"
            }
            case TimestampType => {
              val epochTime = row.getLong(i)/1000
              val offsetDt: OffsetDateTime = Instant.ofEpochMilli(epochTime).atZone(ZoneId.of("UTC")).toOffsetDateTime
              txt = timestampParseFormatTz.format(offsetDt)
            }
            case DateType => {
              val epochDays = row.getInt(i)
              val offsetDate: OffsetDateTime = Instant.ofEpochSecond(epochDays * 3600 * 24).atZone(ZoneId.of("UTC")).toOffsetDateTime
              txt = dateParseFormat.format(offsetDate)
            }
            case BinaryType => {
              //txt = "decode('" + new String(java.util.Base64.getEncoder.encode(row.getBinary(i))) + "','base64')"
              txt = new String(java.util.Base64.getEncoder.encode(row.getBinary(i)))
            }
            case _ => throw new IllegalArgumentException(s"Unsupported type ${field.dataType.catalogString}")
          }
        }
        if (i > 0)
          ret.append(fieldDelimiter)
        ret.append(txt)
      }
    }
    ret.toString()
  }

  ///TODO: Add more datatypes, e.g. ArrayType(et, _),
  // see https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala
  def textToInternalRow(schema: StructType, fields: Array[String]): InternalRow = {
    if (schema.fields.length != fields.length)
      throw new SQLException(s"textToInternalRow: schema.size=${schema.fields.length}, but ${fields.length} data columns received")
    val row = new SpecificInternalRow(schema.fields.map(x => x.dataType))
    fields.zipWithIndex.foreach{ case(txt ,i) => {
      val isNull = txt.toLowerCase.equals("null") || txt.isEmpty
      schema.fields(i).dataType match {
        case StringType => {
          if (!isNull) row.update(i, UTF8String.fromString(StringEscapeUtils.unescapeJava(txt)))
          else row.update(i, UTF8String.fromString(""))
        }
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
        case BooleanType => {
          if (!isNull) {
            //row.setBoolean(i, txt.toBoolean)
            row.setBoolean(i, List("true", "t", "1", "y", "yes").contains(txt.toLowerCase))
          } else {
            row.setBoolean(i, false)
          }
        }
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
        case BinaryType => {
          if (!isNull) {
            row.update(i, java.util.Base64.getDecoder.decode(txt))
            // row.update(i, UTF8String.fromString(txt))
          }
        }
        case _ => throw new IllegalArgumentException(s"Unsupported type ${schema.fields(i).dataType.catalogString}")
      }
      if (isNull)
        row.setNullAt(i)
    }}
    row
  }
}
