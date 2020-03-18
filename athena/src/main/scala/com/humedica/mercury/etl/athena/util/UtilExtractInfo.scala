package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.functions.first

class UtilExtractInfo (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID","CONTEXT_ID","FILENAME", "FROMDATE", "TODATE", "ASOFDATE")

  tables = List("extractinfo")

  columnSelect = Map(
    "extractinfo" -> List("FILEID","CONTEXT_ID","HUM_PARAMETER","HUM_VALUE")
  )

  beforeJoin = Map(
    "extractinfo"-> ((df: DataFrame) => {
      val backFillId = df.filter("HUM_PARAMETER = 'FILENAME' AND UPPER(HUM_VALUE) LIKE '%BACKFILL%'")
        .select("FILEID")
      val backFill = df.join(backFillId, Seq("FILEID"), "inner")
        .withColumn("BACKFILL_FILENAME", regexp_extract(df("HUM_PARAMETER"), "(17\\.3|4\\.0|3\\.11)_[0-9]{14}_[0-9]{1,5}", 0))
        .select("FILEID", "BACKFILL_FILENAME")
        .distinct()
        .filter("BACKFILL_FILENAME IS NOT NULL AND BACKFILL_FILENAME <> ''")

      val dfpivot = df.groupBy("FILEID","CONTEXT_ID")
        .pivot("HUM_PARAMETER", Seq("FILENAME", "FROMDATE", "TODATE", "ASOFDATE"))
        .agg(min("HUM_VALUE"))

      val dfOut = dfpivot.join(backFill, Seq("FILEID"), "left_outer")
      dfOut.withColumn("FILENAME_final", coalesce(dfOut("BACKFILL_FILENAME"), dfOut("FILENAME")))
        .select("FILEID", "CONTEXT_ID", "FILENAME_final", "FROMDATE", "TODATE", "ASOFDATE")
        .withColumnRenamed("FILENAME_final","FILENAME")
        .distinct()
    })
  )
}

// test
//  val a = new UtilExtractInfo(cfg); val o = build(a); o.count

