package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by bhenriksen on 1/25/17.
 */
class ProcedureEdieveventinfo(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  tables = List(
     "ed_iev_event_info",
    "cdr.map_custom_proc",
     "ed_iev_pat_info")

  columnSelect = Map(
    "ed_iev_event_info" -> List("EVENT_TYPE", "EVENT_TIME", "EVENT_DISPLAY_NAME","EVENT_ID"),
    "ed_iev_pat_info" -> List("PAT_ID", "PAT_ENC_CSN_ID","EVENT_ID"),
    "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID","MAPPEDVALUE", "CODETYPE")
  )

  beforeJoin = Map(
    "ed_iev_event_info" -> ((df: DataFrame) => {
      df.filter("EVENT_TYPE is not null and EVENT_TIME is not null")
    }),
    "ed_iev_pat_info" -> ((df: DataFrame) => {
      df.filter("PAT_ID is not null").withColumnRenamed("EVENT_ID", "EVENT_ID_eipi")
    }),
    "cdr.map_custom_proc"-> ((df: DataFrame) => {
      val fil = df.filter("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'ed_iev_event_info'")
      fil.withColumnRenamed("GROUPID", "GROUPID_mcp").withColumnRenamed("DATASRC", "DATASRC_mcp")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("ed_iev_event_info")
      .join(dfs("cdr.map_custom_proc"), concat(lit(config(CLIENT_DS_ID) + "."), dfs("ed_iev_event_info")("EVENT_TYPE")) === dfs("cdr.map_custom_proc")("LOCALCODE"), "inner")
      .join(dfs("ed_iev_pat_info"), dfs("ed_iev_event_info")("EVENT_ID") === dfs("ed_iev_pat_info")("EVENT_ID_eipi"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("EVENT_ID"),df("EVENT_TYPE")).orderBy(df("EVENT_TIME").desc)
      df.withColumn("rw", row_number.over(groups))
      .filter("rw =1")
  }

  map = Map(
    "DATASRC" -> literal("ed_iev_event_info"),
    "LOCALCODE" -> mapFrom("EVENT_TYPE", prefix=config(CLIENT_DS_ID)+"."),
    "PROCEDUREDATE" -> mapFrom("EVENT_TIME"),
    "HOSP_PX_FLAG" -> literal("Y"),
    "LOCALNAME" -> mapFrom("EVENT_DISPLAY_NAME"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> mapFrom("CODETYPE")
  )

}

// test
// val p = new ProcedureEdieveventinfo(cfg) ; val pr = build(p) ; pr.show ; pr.count ; pr.select("ENCOUNTERID").distinct.count