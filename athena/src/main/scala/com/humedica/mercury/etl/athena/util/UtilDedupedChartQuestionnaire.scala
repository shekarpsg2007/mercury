package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import java.lang.Character


class UtilDedupedChartQuestionnaire (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "CHART_QUESTIONNAIRE_ID", "CHART_ID",
    "DOCUMENT_ID", "CLINICAL_ENCOUNTER_ID", "QUESTIONNAIRE_TEMPLATE_NAME", "HUM_STATE", "DEFAULT_STATE", "STATELABEL",
    "SCORE", "MAXIMUM_SCORE", "SCORING_STATUS", "SCORED_DATETIME", "SCORED_BY", "NOTE", "GUIDELINES", "CREATED_DATETIME",
    "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "OBSCODE", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS")

  tables = List("chartquestionnaire", "cdr.zcm_obstype_code", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "chartquestionnaire" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID",
      "CHART_QUESTIONNAIRE_ID", "CHART_ID", "DOCUMENT_ID", "CLINICAL_ENCOUNTER_ID", "QUESTIONNAIRE_TEMPLATE_NAME",
      "HUM_STATE", "DEFAULT_STATE", "STATELABEL", "SCORE", "MAXIMUM_SCORE", "SCORING_STATUS", "SCORED_DATETIME",
      "SCORED_BY", "NOTE", "GUIDELINES", "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS"),
    "fileExtractDates" -> List("FILEID","FILEDATE")
  )


  beforeJoin = Map(
    "chartquestionnaire" -> ((df: DataFrame) => {
      df.withColumn("OBSCODE", concat(coalesce(df("MAXIMUM_SCORE"), df("HUM_STATE")), lit("_") , df("QUESTIONNAIRE_TEMPLATE_NAME")))
    }),
    "cdr.zcm_obstype_code" -> includeIf("GROUPID='" + config(GROUP) + "' AND DATASRC = 'chartquestionnaire'")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("chartquestionnaire")
      .join(dfs("cdr.zcm_obstype_code"), Seq("OBSCODE"), "inner")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("CLINICAL_ENCOUNTER_ID"), df("OBSCODE"), df("CHART_QUESTIONNAIRE_ID"), df("SCORED_DATETIME"), df("OBSTYPE"))
      .orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row=1 and DELETED_DATETIME is null and SCORING_STATUS = 'BEENSCORED' and SCORE is not null")
  }
}

// test
//  val a = new UtilDedupedChartQuestionnaire(cfg); val o = build(a); o.count

