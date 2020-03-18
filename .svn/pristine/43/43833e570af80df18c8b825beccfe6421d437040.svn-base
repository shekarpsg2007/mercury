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


class ObservationClinicaleventdef(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "cdr.zcm_obstype_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "clinical_event" -> List("EVENT_CD", "EVENT_END_DT_TM", "PERSON_ID", "RESULT_STATUS_CD", "ENCNTR_ID", "RESULT_VAL",
      "UPDT_DT_TM"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val zcm = table("cdr.zcm_obstype_code")
        .filter("groupid = '" + config(GROUP) + "' and lower(datasrc) = 'clinical_event_def' and obstype <> 'LABRESULT'")
        .drop("GROUPID", "DATASRC")
      val joined = df1.join(zcm, lower(concat_ws(".", lit(config(CLIENT_DS_ID)), df1("EVENT_CD"))) === lower(zcm("OBSCODE")), "inner")
      joined.filter("person_id is not null and event_end_dt_tm is not null and result_val is not null")
    })
  )

  map = Map(
    "DATASRC" -> literal("clinical_event_def"),
    "LOCALCODE" -> mapFrom("OBSCODE"),
    "OBSDATE" -> mapFrom("EVENT_END_DT_TM"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "STATUSCODE" -> mapFrom("RESULT_STATUS_CD"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALRESULT" -> mapFrom("RESULT_VAL"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val list_clidef_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT_DEF", "OBSERVATION", "CLINICAL_EVENT_DEF", "RESULT_STATUS_CD")
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"), df("OBSDATE"), df("OBSTYPE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and statuscode in (" + list_clidef_status + ")")
  }

}