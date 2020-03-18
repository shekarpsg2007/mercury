package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedMedication (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "MEDICATION_ID", "MEDICATION_NAME",
    "FDB_MED_ID", "MED_NAME_ID", "RXNORM", "NDC", "HIC3_CODE", "HIC3_DESCRIPTION", "HIC1_CODE", "HIC1_DESCRIPTION",
    "GCN_CLINICAL_FORUMULATION_ID", "HIC2_PHARMACOLOGICAL_CLASS", "HIC4_INGREDIENT_BASE", "BRAND_OR_GENERIC_INDICATOR",
    "DEA_SCHEDULE")

  tables = List("medication", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "medication" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "MEDICATION_ID",
      "MEDICATION_NAME", "FDB_MED_ID", "MED_NAME_ID", "RXNORM", "NDC", "HIC3_CODE", "HIC3_DESCRIPTION", "HIC1_CODE",
      "HIC1_DESCRIPTION", "GCN_CLINICAL_FORUMULATION_ID", "HIC2_PHARMACOLOGICAL_CLASS", "HIC4_INGREDIENT_BASE",
      "BRAND_OR_GENERIC_INDICATOR", "DEA_SCHEDULE"),
    "fileExtractDates" -> List("FILEID","FILEDATE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("medication")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("MEDICATION_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1")
  }


}

// test
//  val a = new UtilDedupedMedication(cfg); val o = build(a); o.count

