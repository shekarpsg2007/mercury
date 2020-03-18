package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedClinicalResultObservation (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "CLINICAL_OBSERVATION_ID",
    "CLINICAL_RESULT_ID", "LOINC_ID", "HUM_RESULT", "OBSERVATION_VALUE_TYPE", "REFERENCE_RANGE", "OBSERVATION_UNITS",
    "OBSERVATION_IDENTIFIER", "OBSERVATION_IDENTIFIER_TEXT", "OBSERVATION_ABNORMAL_FLAG_ID", "PERFORMING_LAB_KEY",
    "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "TEMPLATE_ANALYTE_NAME")

  tables = List("clinicalresultobservation", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "clinicalresultobservation" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID",
      "CLINICAL_OBSERVATION_ID", "CLINICAL_RESULT_ID", "LOINC_ID", "HUM_RESULT", "OBSERVATION_VALUE_TYPE",
      "REFERENCE_RANGE", "OBSERVATION_UNITS", "OBSERVATION_IDENTIFIER", "OBSERVATION_IDENTIFIER_TEXT",
      "OBSERVATION_ABNORMAL_FLAG_ID", "PERFORMING_LAB_KEY", "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME",
      "DELETED_BY", "TEMPLATE_ANALYTE_NAME"),
    "fileExtractDates" -> List("FILEID","FILEDATE")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinicalresultobservation")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("CLINICAL_OBSERVATION_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1 and DELETED_DATETIME is null")
  }


}

// test
//  val a = new UtilDedupedClinicalResultObservation(cfg); val o = build(a); o.count

