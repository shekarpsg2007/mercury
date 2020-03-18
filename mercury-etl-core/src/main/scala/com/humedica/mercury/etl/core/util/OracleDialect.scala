package com.humedica.mercury.etl.core.util

import java.sql.Types
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

class OracleDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.NUMERIC) {
      val scale = if (null != md) md.build().getLong("scale") else 0L
      size match {
        case 0 => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
        case _ if scale == -127L => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
        case 1 => Option(BooleanType)
        case 3 | 5 | 10 => Option(IntegerType)
        case 19 if scale == 0L => Option(LongType)
        case 19 if scale == 4L => Option(FloatType)
        case _ => None
      }
    } else { None }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.BOOLEAN))
    case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
    case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
    case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
    case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
    case StringType => Some(JdbcType("VARCHAR2(1024)", java.sql.Types.VARCHAR))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}

