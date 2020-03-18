package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedAppointmentnote (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("APPOINTMENT_ID", "APPOINTMENT_NOTE_ID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID",
    "CREATED_BY", "CREATED_DATETIME", "DELETED_BY", "DELETED_DATETIME", "FILEID", "HUM_TYPE", "NOTE")

  tables = List("appointmentnote","fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "appointmentnote" -> List("APPOINTMENT_ID", "APPOINTMENT_NOTE_ID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID",
      "CREATED_BY", "CREATED_DATETIME", "DELETED_BY", "DELETED_DATETIME", "FILEID", "HUM_TYPE", "NOTE"),
    "fileExtractDates" -> List("FILEID","FILEDATE")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("appointmentnote")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("APPOINTMENT_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1")
  }


}
