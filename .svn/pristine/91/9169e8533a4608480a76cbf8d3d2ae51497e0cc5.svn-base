package com.humedica.mercury.etl.cerner_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 08/09/2018
 */


class ObservationClinicaleventexcl(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "zh_code_value", "cdr.zcm_obstype_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "clinical_event" -> List("EVENT_CD", "RESULT_VAL", "EVENT_END_DT_TM", "PERSON_ID", "ENCNTR_ID", "RESULT_STATUS_CD", "UPDT_DT_TM"),
    "zh_code_value" -> List("CODE_VALUE", "DESCRIPTION"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_result_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "OBSERVATION", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      df.filter("event_cd is not null and result_val is not null and event_end_dt_tm is not null and person_id is not null " +
        "and result_status_cd is not null and result_status_cd not in (" + list_result_status + ")")
        .withColumn("LOCALCODE", concat(lit(config(CLIENT_DS_ID) + "."), df("EVENT_CD"), lit("_"), df("RESULT_VAL")))
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and lower(datasrc) = 'clinical_event_excl' and obstype <> 'LABRESULT'")
        .drop("GROUPID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinical_event")
      .join(dfs("cdr.zcm_obstype_code"), lower(dfs("clinical_event")("LOCALCODE")) === lower(dfs("cdr.zcm_obstype_code")("OBSCODE")), "inner")
      .join(dfs("zh_code_value"), dfs("clinical_event")("EVENT_CD") === dfs("zh_code_value")("CODE_VALUE"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PERSON_ID"), df("ENCNTR_ID"), df("EVENT_CD"), df("RESULT_VAL"), df("OBSTYPE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

  map = Map(
    "DATASRC" -> literal("clinical_event_excl"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "OBSDATE" -> mapFrom("EVENT_END_DT_TM"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALRESULT" -> concatFrom(Seq("DESCRIPTION", "RESULT_VAL"), delim = "."),
    "OBSTYPE" -> mapFrom("OBSTYPE")
  )

}