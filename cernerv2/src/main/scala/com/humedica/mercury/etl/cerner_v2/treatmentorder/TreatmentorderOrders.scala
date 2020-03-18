package com.humedica.mercury.etl.cerner_v2.treatmentorder

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 08/09/2018
  */


class TreatmentorderOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("orders", "order_action", "order_detail", "cdr.map_predicate_values", "cdr.zcm_treatment_type_code")

  columnSelect = Map(
    "orders" -> List("CATALOG_CD", "PERSON_ID", "ORDER_ID", "ENCNTR_ID", "ORDER_STATUS_CD", "ACTIVE_STATUS_CD", "UPDT_DT_TM"),
    "order_action" -> List("ORDER_DT_TM", "ORDER_PROVIDER_ID", "ORDER_ID", "UPDT_DT_TM", "ACTION_TYPE_CD"),
    "order_detail" -> List("ORDER_ID", "OE_FIELD_MEANING_ID", "OE_FIELD_DISPLAY_VALUE", "OE_FIELD_ID", "UPDT_DT_TM"),
    "cdr.zcm_treatment_type_code" -> List("GROUPID", "LOCAL_CODE", "TREATMENT_TYPE_CUI", "LOCAL_UNIT", "TREATMENT_TYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "orders" -> ((df: DataFrame) => {
      val list_order_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "TREATMENT_ORDER", "ORDERS", "ORDER_STATUS_CD")
      val list_active_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "TREATMENT_ORDER", "ORDERS", "ACTIVE_STATUS_CD")

      val fil = df.filter("person_id is not null and catalog_cd is not null and order_status_cd not in (" + list_order_status + ") " +
        "and active_status_cd in (" + list_active_status + ")")
      val groups = Window.partitionBy(fil("ORDER_ID")).orderBy(fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .withColumn("LOCAL_CODE", concat_ws(".", lit(config(CLIENT_DS_ID)), fil("CATALOG_CD")))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM")
    }),
    "order_action" -> ((df: DataFrame) => {
      val list_action_type = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "NEW_ORDER_CD", "TREATMENT_ORDER", "ORDERS", "ORDER_STATUS_CD")
      val fil = df.filter("action_type_cd in (" + list_action_type + ")")
      val groups_min = Window.partitionBy(fil("ORDER_ID"))
      val groups_rn = Window.partitionBy(fil("ORDER_ID")).orderBy(fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("OA_ORDER_DATE", min(fil("ORDER_DT_TM")).over(groups_min))
        .withColumn("rn", row_number.over(groups_rn))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM")
    }),
    "order_detail" -> ((df: DataFrame) => {
      val list_oe_field_meaning = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "TREATMENT_ORDER", "ORDERS", "OE_FIELD_MEANING_ID")
      val list_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "TREATMENT_ORDER", "ORDERS", "OE_FIELD_ID")
      val addColumn = df.withColumn("ORDERED_QUANTITY",
        when(df("OE_FIELD_MEANING_ID").isin(list_oe_field_meaning: _*), df("OE_FIELD_DISPLAY_VALUE"))
          .when(df("OE_FIELD_ID").isin(list_oe_field_id: _*), regexp_extract(df("OE_FIELD_DISPLAY_VALUE"), "([0-9]+)", 1))
          .otherwise(null))
      val fil = addColumn.filter("ordered_quantity is not null")
      val groups = Window.partitionBy(fil("ORDER_ID")).orderBy(fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .select("ORDER_ID", "ORDERED_QUANTITY")
    }),
    "cdr.zcm_treatment_type_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "'")
        .drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("orders")
      .join(dfs("order_action"), Seq("ORDER_ID"), "left_outer")
      .join(dfs("order_detail"), Seq("ORDER_ID"), "left_outer")
      .join(dfs("cdr.zcm_treatment_type_code"), Seq("LOCAL_CODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("orders"),
    "LOCALCODE" -> mapFrom("LOCAL_CODE"),
    "ORDER_DATE" -> mapFrom("OA_ORDER_DATE"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ORDER_ID" -> mapFrom("ORDER_ID"),
    "ORDER_PROV_ID" -> mapFrom("ORDER_PROVIDER_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "ORDERED_QUANTITY" -> mapFrom("ORDERED_QUANTITY"),
    "CUI" -> mapFrom("TREATMENT_TYPE_CUI"),
    "STD_UNIT_CUI" -> mapFrom("TREATMENT_TYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ORDER_ID")).orderBy(lit("1"))
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}