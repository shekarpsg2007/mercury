package com.humedica.mercury.etl.cerner_v2.treatmentadmin

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 08/09/2018
  */


class TreatmentadminClinicalevent(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "cdr.zcm_treatment_type_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "clinical_event" -> List("EVENT_ID", "EVENT_END_DT_TM", "EVENT_CD", "PERSON_ID", "PERFORMED_PRSNL_ID",
      "ORDER_ID", "ENCNTR_ID", "RESULT_VAL", "RESULT_STATUS_CD", "UPDT_DT_TM"),
    "cdr.zcm_treatment_type_code" -> List("GROUPID", "LOCAL_CODE", "TREATMENT_TYPE_CUI", "LOCAL_UNIT", "TREATMENT_TYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      df.filter("person_id is not null and event_end_dt_tm is not null and event_cd is not null and event_id is not null")
        .withColumn("LOCAL_CODE", concat_ws(".", lit(config(CLIENT_DS_ID)), df("EVENT_CD")))
    }),
    "cdr.zcm_treatment_type_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "'")
        .drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinical_event")
      .join(dfs("cdr.zcm_treatment_type_code"), Seq("LOCAL_CODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("clinical_event"),
    "ADMINISTERED_ID" -> mapFrom("EVENT_ID"),
    "ADMINISTERED_DATE" -> mapFrom("EVENT_END_DT_TM"),
    "LOCALCODE" -> mapFrom("LOCAL_CODE"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ADMINISTERED_PROV_ID" -> mapFrom("PERFORMED_PRSNL_ID"),
    "ORDER_ID" -> mapFrom("ORDER_ID", nullIf = Seq("0")),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "ADMINISTERED_QUANTITY" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, regexp_extract(df("RESULT_VAL"), "([0-9]+)", 1))
    }),
    "CUI" -> mapFrom("TREATMENT_TYPE_CUI"),
    "STD_UNIT_CUI" -> mapFrom("TREATMENT_TYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val list_result_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "TREATMENT_ADMIN", "CLINICAL_EVENT", "RESULT_STATUS_CD")
    val groups = Window.partitionBy(df("ADMINISTERED_ID"))
      .orderBy(df("ADMINISTERED_DATE").desc_nulls_last, df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and result_status_cd not in (" + list_result_status + ") and administered_quantity is not null")
  }

}