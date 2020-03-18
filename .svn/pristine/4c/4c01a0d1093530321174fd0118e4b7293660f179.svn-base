package com.humedica.mercury.etl.cerner_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 08/09/2018
  */


class ProcedureCebolus(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("orders", "clinical_event", "cdr.map_custom_proc", "cdr.map_predicate_values")

  columnSelect = Map(
    "orders" -> List("ORDER_ID", "ORDERED_AS_MNEMONIC"),
    "clinical_event" -> List("PERSON_ID", "EVENT_END_DT_TM", "PERFORMED_DT_TM", "ENCNTR_ID", "PERFORMED_PRSNL_ID",
      "ENCNTR_ID", "UPDT_DT_TM", "RESULT_STATUS_CD", "EVENT_CLASS_CD", "ORDER_ID"),
    "cdr.map_custom_proc" -> List("GROUPID", "DATASRC", "LOCALCODE", "MAPPEDVALUE")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_result_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CE_BOLUS", "PROCEDURE", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      val list_event_class = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CE_BOLUS", "PROCEDURE", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      df.filter("(result_status_cd is null or result_status_cd not in (" + list_result_status + ")) " +
        "and coalesce(event_class_cd, ' ') in (" + list_event_class + ")")
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and lower(datasrc) = 'ce_bolus'")
        .select("LOCALCODE", "MAPPEDVALUE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinical_event")
      .join(dfs("orders"), Seq("ORDER_ID"), "inner")
      .join(dfs("cdr.map_custom_proc"), concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("orders")("ORDERED_AS_MNEMONIC")) === dfs("cdr.map_custom_proc")("LOCALCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("ce_bolus"),
    "LOCALCODE" -> mapFrom("ORDERED_AS_MNEMONIC", prefix = config(CLIENT_DS_ID) + "."),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "PROCEDUREDATE" -> cascadeFrom(Seq("EVENT_END_DT_TM", "PERFORMED_DT_TM")),
    "LOCALNAME" -> mapFrom("ORDERED_AS_MNEMONIC"),
    "ACTUALPROCDATE" -> cascadeFrom(Seq("EVENT_END_DT_TM", "PERFORMED_DT_TM")),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "PERFORMINGPROVIDERID" -> mapFrom("PERFORMED_PRSNL_ID", nullIf = Seq("0")),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"), df("PROCEDUREDATE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and proceduredate is not null and patientid is not null")
      .drop("rn")
  }

}