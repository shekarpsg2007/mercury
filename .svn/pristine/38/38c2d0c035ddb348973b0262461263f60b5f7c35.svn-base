package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedChart (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("CHART_ID", "CHART_SHARING_GROUP_ID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID",
    "CREATED_BY", "CREATED_DATETIME", "DELETED_BY", "DELETED_DATETIME", "ENTERPRISE_ID", "NEW_CHART_ID")

  tables = List("chart",
    "fileExtractDates:athena.util.UtilFileIdDates",
    "pat:athena.util.UtilSplitPatient")

  columnSelect = Map(
    "chart" -> List("CHART_ID", "CHART_SHARING_GROUP_ID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID",
      "CREATED_BY", "CREATED_DATETIME", "DELETED_BY", "DELETED_DATETIME", "ENTERPRISE_ID", "NEW_CHART_ID", "FILEID"),
    "fileExtractDates" -> List("FILEID","FILEDATE"),
    "pat" -> List("PATIENT_ID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    val patJoinType = new UtilSplitTable(config).patprovJoinType
    dfs("chart")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
      .join(dfs("pat"), dfs("chart")("ENTERPRISE_ID") === dfs("pat")("PATIENT_ID"), patJoinType)
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("CHART_ID"), df("CONTEXT_ID"))
      .orderBy(df("FILEDATE").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1")
  }

}
