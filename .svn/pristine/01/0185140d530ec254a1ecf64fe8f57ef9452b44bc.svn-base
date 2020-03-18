package com.humedica.mercury.etl.cerner_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by mschlomka on 8/14/2018.
  */

class ObservationEncounter(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("encounter", "zh_code_value", "cdr.zcm_obstype_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "encounter" -> List("ENCNTR_ID", "UPDT_DT_TM", "LOCATION_CD", "ARRIVE_DT_TM", "DISCH_DT_TM", "PERSON_ID", "ACTIVE_STATUS_CD",
      "REASON_FOR_VISIT", "ENCNTR_TYPE_CD", "ACTIVE_IND"),
    "zh_code_value" -> List("CODE_VALUE", "DESCRIPTION"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "encounter" -> ((df: DataFrame) => {
      val list_excl_encntr = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTER", "OBSERVATION", "ENCOUNTER", "ENCNTR_TYPE_CD")
      val df2 = df.filter("person_id is not null")
      val groups = Window.partitionBy(df2("ENCNTR_ID")).orderBy(df2("UPDT_DT_TM").desc_nulls_last)
      df2.withColumn("rn", row_number.over(groups))
        .withColumn("LOCALCODE", concat_ws(".", lit(config(CLIENT_DS_ID)), df2("LOCATION_CD")))
        .filter("rn = 1 and reason_for_visit not in ('CANCELLED', 'NO SHOW') and coalesce(active_ind, 'X') <> '0' " +
          "and arrive_dt_tm is not null and encntr_type_cd not in (" + list_excl_encntr + ") and location_cd is not null")
        .drop("rn")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and lower(datasrc) = 'encounter' and obstype is not null")
        .drop("GROUPID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("encounter")
      .join(dfs("zh_code_value"), dfs("encounter")("LOCATION_CD") === dfs("zh_code_value")("CODE_VALUE"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), dfs("encounter")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("encounter"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "OBSDATE" -> cascadeFrom(Seq("ARRIVE_DT_TM", "DISCH_DT_TM")),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "STATUSCODE" -> mapFrom("ACTIVE_STATUS_CD"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALRESULT" -> mapFrom("DESCRIPTION"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

}