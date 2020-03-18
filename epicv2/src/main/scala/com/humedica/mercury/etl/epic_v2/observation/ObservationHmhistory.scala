package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ObservationHmhistory(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hm_history",
                "zh_hm_type", "cdr.map_predicate_values",
                "cdr.zcm_obstype_code")


  columnSelect = Map(
    "hm_history" -> List("HM_TYPE_C", "HM_TOPIC_ID", "HM_HX_DATE", "PAT_ID"),
    "zh_hm_type" -> List("HM_TYPE_C", "NAME"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "GROUPID", "OBSTYPE", "OBSCODE", "LOCALUNIT")
  )

  beforeJoin = Map(
    "hm_history" -> ((df: DataFrame) => {
      val list_hm_type_c = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "HM_HISTORY", "OBSERVATION", "HM_HISTORY", "HM_TYPE_C")
      df.withColumn("LOCALCODE", when(df("HM_TYPE_C") isin(list_hm_type_c:_*),
        concat(lit(config(CLIENT_DS_ID)+".DECLINED."),df("HM_TOPIC_ID")))
        .otherwise(when(isnull(df("HM_TOPIC_ID")),null).otherwise(concat(lit(config(CLIENT_DS_ID)+"."),df("HM_TOPIC_ID")))))
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'hm_history' and GROUPID = '"+config(GROUP)+"'").drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hm_history")
      .join(dfs("zh_hm_type"), Seq("HM_TYPE_C"),"left_outer")
      .join(dfs("cdr.zcm_obstype_code"), dfs("hm_history")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("obscode"), "inner")
  }


  map = Map(
    "DATASRC" -> literal("hm_history"),
    "OBSDATE" -> mapFrom("HM_HX_DATE"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "STATUSCODE" -> mapFrom("NAME"),
    "LOCALRESULT" -> ((col:String, df:DataFrame) => df.withColumn(col,
          when(isnull(df("HM_TYPE_C")), null).otherwise(concat(lit(config(CLIENT_DS_ID)+"."),df("HM_TYPE_C")))))
  )

  afterMap = (df: DataFrame) => {
    df.filter("OBSDATE is not null and PATIENTID is not null and LOCALCODE is not null").distinct()
  }

 }