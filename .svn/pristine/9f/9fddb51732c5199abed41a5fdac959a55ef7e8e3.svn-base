package com.humedica.mercury.toolkit.impl

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.{Logger, LoggerFactory}
import com.humedica.mercury.etl.core.engine.ShowDf._

object SketchDiff {

  val log: Logger = LoggerFactory.getLogger(getClass)
  
  def rowMatchPct(df0: DataFrame, df1: DataFrame, sess: SparkSession): Unit =
    matchPct(concatCols(df0), concatCols(df1), sess)

  def matchPct(df0Raw: DataFrame, df1Raw: DataFrame, sess: SparkSession): Unit = {
    import sess.sqlContext.implicits._
    val commonCols = df0Raw.columns.intersect(df1Raw.columns)
    val df0Fill = coalesceToIntegral(df0Raw).na.fill("null")
    val df1Fill = coalesceToIntegral(df1Raw).na.fill("null")
    val df0Len = df0Fill.count()
    val df1Len = df1Fill.count()
    val df0Bf = bloomFilter(df0Fill, df0Len, commonCols)
    val df1Bf = bloomFilter(df1Fill, df1Len, commonCols)
    val df0Exists = df0Fill.map(r0 => exists(r0, df1Bf))
    val df1Exists = df1Fill.map(r0 => exists(r0, df0Bf))
    val df0ExtSum = df0Exists.reduce((s0, s1) => s0.zip(s1).map { case (val0, val1) => val0 + val1 })
    val df1ExtSum = df1Exists.reduce((s0, s1) => s0.zip(s1).map { case (val0, val1) => val0 + val1 })
    val df0Pct = df0ExtSum.map(val0 => val0.toDouble / df0Len.toDouble)
    val df1Pct = df1ExtSum.map(val0 => val0.toDouble / df1Len.toDouble)
    log.info("X column match percentage against Y:")
    log.info(df0Pct.toString())
    log.info("Y column match percentage against X:")
    log.info(df1Pct.toString())

    val df0Freq = df0Fill.stat.freqItems(df0Fill.columns, 0.9)
    val df1Freq = df1Fill.stat.freqItems(df1Fill.columns, 0.9)
    log.info(show(df0Freq, false))
    log.info(show(df1Freq, false))
  }

  private def exists(r0: Row, bfs: Seq[BloomFilter]): Seq[Long] = {
    r0.toSeq.zip(bfs).map {
      case (value, filter) =>
        if (value != null && filter.mightContain(value)) 1L else 0L
    }
  }

  private def bloomFilter(df: DataFrame, dfLen: Long, cols: Seq[String]): Seq[BloomFilter] =
    cols.map(cn => df.stat.bloomFilter(col(cn), dfLen, 0.01))

  private def concatCols(df0: DataFrame): DataFrame =
    df0.select(concat_ws("|", df0.columns.map(c => col(c)): _*))

  private def coalesceToIntegral(df: DataFrame): DataFrame = {
    val cols0 = df.columns.zip(df.schema)
      .map { case (cName, cField) => (cName, cField.dataType) }
      .map {
        case (cName, _: TimestampType) => col(cName).cast(LongType)
        case (cName, _: DecimalType) => col(cName).cast(StringType)
        case (cName, _) => col(cName)
      }
    val df0 = df.select(cols0: _*)
    df0
  }
}
