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


class ProcedureClinicalevent(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "zh_code_value", "cdr.map_custom_proc", "cdr.map_predicate_values")

  columnSelect = Map(
    "clinical_event" -> List("EVENT_CD", "CATALOG_CD", "PERSON_ID", "RESULT_VAL", "ENCNTR_ID", "PERFORMED_PRSNL_ID",
      "RESULT_UNITS_CD", "UPDT_DT_TM", "RESULT_STATUS_CD", "EVENT_CLASS_CD", "PERFORMED_DT_TM", "EVENT_START_DT_TM"),
    "zh_code_value" -> List("CODE_VALUE", "DESCRIPTION"),
    "cdr.map_custom_proc" -> List("GROUPID", "DATASRC", "LOCALCODE", "MAPPEDVALUE")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_result_concat = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "PROCEDURE", "CLINICAL_EVENT", "RESULT_CONCAT")
      df.withColumn("LOCALCODE",
        when(df("EVENT_CD").isin(list_result_concat: _*),
          concat(lit(config(CLIENT_DS_ID) + "."), concat_ws("_", df("CATALOG_CD"), df("EVENT_CD"), df("RESULT_UNITS_CD"), df("RESULT_VAL"))))
        .otherwise(concat(lit(config(CLIENT_DS_ID) + "."), concat_ws("_", df("CATALOG_CD"), df("EVENT_CD"), df("RESULT_UNITS_CD")))))
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and datasrc = 'clinical_event'")
        .select("LOCALCODE", "MAPPEDVALUE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinical_event")
      .join(dfs("zh_code_value"), dfs("clinical_event")("EVENT_CD") === dfs("zh_code_value")("CODE_VALUE"), "left_outer")
      .join(dfs("cdr.map_custom_proc"), Seq("LOCALCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("clinical_event"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "PROCEDUREDATE" -> ((col: String, df: DataFrame) => {
      val list_procdt = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROCEDUREDATE", "PROCEDURE", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      val list_performed_dt = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "PROCEDURE", "CLINICAL_EVENT", "PERFORMED_DT_TM")
      df.withColumn(col, when(df("EVENT_CLASS_CD").isin(list_procdt: _*), from_unixtime(unix_timestamp(substring(df("RESULT_VAL"), 3, 12), "yyyyMMddHHmm")))
        .when(df("MAPPEDVALUE").isin(list_performed_dt: _*), coalesce(df("EVENT_START_DT_TM"), df("PERFORMED_DT_TM")))
        .otherwise(coalesce(df("PERFORMED_DT_TM"), df("EVENT_START_DT_TM"))))
    }),
    "LOCALNAME" -> mapFrom("DESCRIPTION"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "PERFORMINGPROVIDERID" -> mapFrom("PERFORMED_PRSNL_ID", nullIf = Seq("0")),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> literal("CUSTOM")
  )

  afterMap = (df: DataFrame) => {
    val addColumn = df.withColumn("ACTUALPROCDATE",
      when(from_unixtime(unix_timestamp(df("PROCEDUREDATE"))) > current_timestamp() ||
        from_unixtime(unix_timestamp(df("PROCEDUREDATE"))) < to_date(lit("1901-01-01"))
        , null)
      .otherwise(df("PROCEDUREDATE")))

    val list_result_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "PROCEDURE", "CLINICAL_EVENT", "RESULT_STATUS_CD")
    val list_event_class = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "PROCEDURE", "CLINICAL_EVENT", "EVENT_CLASS_CD")
    val groups = Window.partitionBy(addColumn("ENCOUNTERID"), addColumn("LOCALCODE"), addColumn("PROCEDUREDATE"), addColumn("MAPPEDVALUE"))
      .orderBy(addColumn("UPDT_DT_TM").desc_nulls_last)
    addColumn.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and (result_status_cd is null or result_status_cd not in (" + list_result_status + ")) " +
        "and not coalesce(event_class_cd, ' ') in (" + list_event_class + ") and patientid is not null and proceduredate is not null")
      .drop("rn")
  }

}