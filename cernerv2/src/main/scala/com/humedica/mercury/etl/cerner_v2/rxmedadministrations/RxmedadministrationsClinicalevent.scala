package com.humedica.mercury.etl.cerner_v2.rxmedadministrations

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class RxmedadministrationsClinicalevent(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "orders", "zh_code_value", "cdr.map_predicate_values",
    "rxorder:cerner_v2.rxordersandprescriptions.RxordersandprescriptionsOrders",
    "tempmed:cerner_v2.rxordersandprescriptions.RxordersandprescriptionsTempmed")

  columnSelect = Map(
    "clinical_event" -> List("PERSON_ID", "CLINICAL_EVENT_ID", "ORDER_ID", "RESULT_UNITS_CD", "RESULT_VAL",
      "PERFORMED_PRSNL_ID", "EVENT_END_DT_TM", "ENCNTR_ID", "PERFORMED_DT_TM", "CATALOG_CD", "EVENT_CLASS_CD",
      "RESULT_STATUS_CD", "UPDT_DT_TM"),
    "zh_code_value" -> List("DISPLAY", "CODE_VALUE"),
    "rxorder" -> List("RXID", "LOCALROUTE"),
    "orders" -> List("ORDER_ID", "CATALOG_CD"),
    "tempmed" -> List("CATALOG_CD", "LOCALGENERICDESC", "LOCALNDC")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_event_class_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "RXADMIN", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      val list_result_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "RXADMIN", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      val groups = Window.partitionBy(df("CLINICAL_EVENT_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1 and event_class_cd in (" + list_event_class_cd + ") and " +
          "(result_status_cd is null or result_status_cd not in (" + list_result_status_cd + "))")
        .drop("rn")
    }),
    "orders" -> ((df: DataFrame) => {
      val rx = table("rxorder")
      val joined = df.join(rx, df("ORDER_ID") === rx("RXID"), "left_outer")
      joined.groupBy("ORDER_ID", "CATALOG_CD")
        .agg(max(joined("LOCALROUTE")).as("LOCALROUTE"))
        .withColumnRenamed("CATALOG_CD", "CATALOG_CD_ord")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinical_event")
      .join(dfs("orders"), Seq("ORDER_ID"), "left_outer")
      .join(dfs("tempmed"), Seq("CATALOG_CD"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("clinical_event"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "RXADMINISTRATIONID" -> mapFrom("CLINICAL_EVENT_ID"),
    "RXORDERID" -> mapFrom("ORDER_ID"),
    "LOCALMEDCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("CATALOG_CD") =!= lit("0"), df("CATALOG_CD"))
        .when(df("CATALOG_CD_ord") =!= lit("0"), df("CATALOG_CD_ord"))
        .otherwise(null))
    }),
    "LOCALSTRENGTHUNIT" -> mapFrom("RESULT_UNITS_CD"),
    "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("RESULT_VAL"), 1, 255))
    }),
    "LOCALPROVIDERID" -> mapFrom("PERFORMED_PRSNL_ID"),
    "ADMINISTRATIONTIME" -> cascadeFrom(Seq("EVENT_END_DT_TM", "PERFORMED_DT_TM")),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID")
  )

  afterMap = (df: DataFrame) => {
    val zh = table("zh_code_value")
    val joined = df.join(zh, df("LOCALMEDCODE") === zh("CODE_VALUE"), "left_outer")
    joined.withColumn("LOCALDRUGDESCRIPTION", joined("DISPLAY"))
  }

}