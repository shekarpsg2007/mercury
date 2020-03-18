package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.functions.first

class UtilFileIdDates (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID","FILEDATE")

  tables = List("load_log","extractInfo:athena.util.UtilExtractInfo")

  columnSelect = Map(
    "load_log" -> List("FILEID", "FILE_NAME"),
    "extractInfo" -> List("CONTEXT_ID", "FILENAME", "FROMDATE", "TODATE", "ASOFDATE")
  )

  beforeJoin = Map(
    "load_log"-> ((df: DataFrame) => {
      df.withColumn("FILENAME_ll", regexp_extract(df("FILE_NAME"), "(17\\.3|4\\.0|3\\.11)_[0-9]{14}_[0-9]{1,5}", 0))
        .select("FILEID","FILENAME_ll")
    }),
    "extractInfo"-> ((df: DataFrame) => {
      val dfOut = df.withColumn("FILENAME_ei", regexp_extract(df("FILENAME"), "(17\\.3|4\\.0|3\\.11)_[0-9]{14}_[0-9]{1,5}", 0))

      val df1 = safe_to_date(dfOut, "FROMDATE", "FROMDATE", "MM/dd/yyyy")
      val df2 = safe_to_date(df1, "TODATE", "TODATE", "MM/dd/yyyy")
      safe_to_date(df2, "ASOFDATE", "ASOFDATE", "MM/dd/yyyy")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("load_log")
      .join(dfs("extractInfo"), dfs("extractInfo")("FILENAME_ei") === dfs("load_log")("FILENAME_ll"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val df1 = df.withColumn("FILE_DATE", when(df("FROMDATE") >= to_date(lit("2000-01-01")), df("FROMDATE"))
      .when(df("TODATE") < date_add(current_date(), 30), df("TODATE"))
      .otherwise(df("ASOFDATE")))

    df1.groupBy("FILEID")
      .agg(max("FILE_DATE").as("FILEDATE"))
  }
}


// test
//  val a = new UtilFileIdDates(cfg); val o1 = build(a); o1.show ; o1.printSchema; o1.count;

