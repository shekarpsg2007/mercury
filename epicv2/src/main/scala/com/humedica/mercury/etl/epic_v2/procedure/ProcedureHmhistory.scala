package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ProcedureHmhistory(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_clarityhmtopic",
                   "hm_history",
                   "hm_override",
                   "cdr.map_custom_proc", "cdr.map_predicate_values")

  columnSelect = Map(
        "zh_clarityhmtopic" -> List("NAME","HM_TOPIC_ID"),
        "hm_history" -> List("PAT_ID","HM_HX_DATE","HM_TOPIC_ID", "HM_TYPE_C"),
        "hm_override" -> List("PAT_ID","OVERRIDE_DATE","HM_TOPIC_ID","HM_TYPE_C"),
        "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID","MAPPEDVALUE")
  )

  beforeJoin = Map(
  "cdr.map_custom_proc"-> ((df: DataFrame) => {
    df.filter("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'HM_HISTORY'").drop("GROUPID").drop("DATASRC")
  }),
  "hm_history"-> ((df: DataFrame) => {
    val or = table("hm_override")
    df.union(or).distinct
  })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hm_history")
      .join(dfs("cdr.map_custom_proc"),
        concat(lit(config(CLIENT_DS_ID)+"."), dfs("hm_history")("HM_TOPIC_ID")) === dfs("cdr.map_custom_proc")("LOCALCODE"), "inner")
      .join(dfs("zh_clarityhmtopic"), Seq("HM_TOPIC_ID"), "left_outer")

  }


  afterJoin = (df:DataFrame) => {
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "HM_HISTORY", "PROCEDUREDO", "HM_HISTORY",  "HM_TYPE_C")
    df.filter("HM_HX_DATE is not null and PAT_ID is not null and (HM_TYPE_C in (" + list_coding_status_c + ") or 'NO_MPV_MATCHES' in (" + list_coding_status_c + "))")

  }



  map = Map(
        "DATASRC" -> literal("hm_history"),
        "LOCALCODE" ->  mapFrom("HM_TOPIC_ID",nullIf=Seq(null),prefix=config(CLIENT_DS_ID)+"."),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "PROCEDUREDATE" -> mapFrom("HM_HX_DATE"),
        "LOCALNAME" -> mapFrom("NAME"),
        "ACTUALPROCDATE" -> mapFrom("HM_HX_DATE"),
        "CODETYPE" -> literal("CUSTOM"),
        "MAPPEDCODE"-> mapFrom("MAPPEDVALUE")
  )


  afterMap = (df1:DataFrame) => {
    val df=df1.repartition(1000)
    df.dropDuplicates("LOCALCODE", "PATIENTID", "PROCEDUREDATE", "LOCALNAME", "MAPPEDCODE")
  }
 }