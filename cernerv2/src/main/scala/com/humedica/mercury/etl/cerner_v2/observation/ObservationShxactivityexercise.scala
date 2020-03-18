package com.humedica.mercury.etl.cerner_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class ObservationShxactivityexercise(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("shx_activity", "zh_nomenclature", "shx_response", "clinical_event", "shx_alpha_response",
    "cdr.zcm_obstype_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "shx_activity" -> List("PERSON_ID", "SHX_ACTIVITY_ID"),
    "zh_nomenclature" -> List("NOMENCLATURE_ID", "SOURCE_STRING"),
    "shx_response" -> List("SHX_RESPONSE_ID", "SHX_ACTIVITY_ID", "TASK_ASSAY_CD", "BEG_EFFECTIVE_DT_TM"),
    "shx_alpha_response" -> List("SHX_RESPONSE_ID", "NOMENCLATURE_ID", "UPDT_DT_TM"),
    "clinical_event" -> List("EVENT_CD", "PERSON_ID", "EVENT_END_DT_TM"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_incl_event = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SHX_ACT", "OBSERVATION", "CLINICAL_EVENT", "EVENT_CD")
      df.filter("event_cd in (" + list_incl_event + ")")
        .withColumnRenamed("PERSON_ID", "PERSON_ID_ce")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and datasrc = 'shx_act' and obstype = 'EXERCISE'")
        .drop("GROUPID", "DATASRC")
    }),
    "shx_response" -> ((df: DataFrame) => {
      df.filter("beg_effective_dt_tm is not null")
        .withColumn("LOCALCODE", concat_ws(".", lit(config(CLIENT_DS_ID)), df("TASK_ASSAY_CD")))
    }),
    "shx_activity" -> ((df: DataFrame) => {
      df.filter("person_id is not null")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    val list_incl_assay = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SHX_ACT", "OBSERVATION", "SHX_RESPONSE", "TASK_ASSAY_CD")
    dfs("shx_activity")
      .join(dfs("shx_response"), Seq("SHX_ACTIVITY_ID"), "inner")
      .join(dfs("shx_alpha_response"), Seq("SHX_RESPONSE_ID"), "inner")
      .join(dfs("zh_nomenclature"), Seq("NOMENCLATURE_ID"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), dfs("shx_response")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
      .join(dfs("clinical_event"),
        dfs("shx_response")("TASK_ASSAY_CD").isin(list_incl_assay: _*)
          && dfs("shx_activity")("PERSON_ID") === dfs("clinical_event")("PERSON_ID_ce")
          && from_unixtime(unix_timestamp(dfs("clinical_event")("EVENT_END_DT_TM")), "yyyy-MM-dd") >= from_unixtime(unix_timestamp(dfs("shx_alpha_response")("UPDT_DT_TM")), "yyyy-MM-dd")
        , "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("shx_act"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "OBSDATE" -> cascadeFrom(Seq("EVENT_END_DT_TM", "BEG_EFFECTIVE_DT_TM")),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "LOCALRESULT" -> mapFrom("SOURCE_STRING"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("LOCALCODE"), df("OBSDATE"), df("LOCALRESULT"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and localresult is not null")
      .drop("rn")
  }

}
