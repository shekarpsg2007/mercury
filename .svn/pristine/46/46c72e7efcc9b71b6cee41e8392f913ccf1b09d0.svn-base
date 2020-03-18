package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

class ObservationGeneralordersdesc(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
  "generalorders",
  "zh_clarity_eap",
  "cdr.zcm_obstype_code")

  columnSelect = Map(
    "generalorders" -> List("ORDER_STATUS_C", "ORDER_DESCRIPTION", "PROC_CODE", "PAT_ID", "PAT_ENC_CSN_ID", "UPDATE_DATE", "PROC_ID", "SPECIMN_TAKEN_TIME",
      "PROC_START_TIME", "PROC_BGN_TIME", "ORDER_TIME", "ORDER_DESCRIPTION"),
    "zh_clarity_eap" -> List("PROC_CODE", "PROC_ID")
  )

  beforeJoin = Map(
    "generalorders" -> ((df: DataFrame) => {
      val fil = df.filter("ORDER_STATUS_C in ('5','3','-1') AND ORDER_DESCRIPTION is not null AND PROC_CODE is not null AND PAT_ID is not null AND PAT_ENC_CSN_ID is not null")
        .withColumn("LOCALCODE_go", when(isnull(df("PROC_CODE")), null).otherwise(concat(lit(config(CLIENT_DS_ID)+"."), df("PROC_CODE"))))
      val groups = Window.partitionBy(fil("PAT_ENC_CSN_ID"),fil("PROC_CODE")).orderBy(fil("UPDATE_DATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
      }),
    "zh_clarity_eap" -> ((df: DataFrame) => {
      df.withColumnRenamed("PROC_CODE","LOCALCODE_zh")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'generalorders_desc' and GROUPID='"+config(GROUP)+"'").drop("GROUPID", "CLIENT_DS_ID")
    })      
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("generalorders")
      .join(dfs("zh_clarity_eap"), Seq("PROC_ID"), "left_outer")
      .join(dfs("cdr.zcm_obstype_code"), coalesce(dfs("generalorders")("LOCALCODE_go"),dfs("zh_clarity_eap")("LOCALCODE_zh"))
        === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("generalorders_desc"),
    "OBSDATE" -> cascadeFrom(Seq("SPECIMN_TAKEN_TIME", "PROC_START_TIME", "PROC_BGN_TIME", "ORDER_TIME")),
    "PATIENTID" -> mapFrom("PAT_ID", nullIf = Seq("-1")),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID", nullIf = Seq("-1")),
    "LOCALRESULT" -> mapFrom("ORDER_DESCRIPTION"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "LOCALCODE" -> cascadeFrom(Seq("LOCALCODE_go","LOCALCODE_zh"))
  )

  afterMap = (df: DataFrame) => {
    df.filter("PATIENTID IS NOT NULL AND ENCOUNTERID IS NOT NULL AND LOCALCODE IS NOT NULL AND OBSDATE IS NOT NULL")
  }

}