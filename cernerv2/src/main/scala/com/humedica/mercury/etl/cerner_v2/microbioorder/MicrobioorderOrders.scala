package com.humedica.mercury.etl.cerner_v2.microbioorder

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class MicrobioorderOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("orders", "ce_specimen_coll", "mic_task_log", "zh_code_value", "clinical_event", "cdr.map_predicate_values")

  columnSelect = Map(
    "orders" -> List("CATALOG_CD", "ORDER_ID", "PERSON_ID", "ORIG_ORDER_DT_TM", "ENCNTR_ID", "DEPT_STATUS_CD",
      "ACTIVITY_TYPE_CD", "UPDT_DT_TM"),
    "ce_specimen_coll" -> List("EVENT_ID", "SOURCE_TYPE_CD", "COLLECT_DT_TM", "UPDT_DT_TM"),
    "mic_task_log" -> List("ORDER_ID", "TASK_LOG_ID", "TASK_DT_TM"),
    "zh_code_value" -> List("CODE_VALUE", "CODE_SET", "DESCRIPTION", "DEFINITION"),
    "clinical_event" -> List("ORDER_ID", "EVENT_ID", "UPDT_DT_TM", "RESULT_STATUS_CD", "EVENT_CLASS_CD")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_result_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "MBORDER", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      val list_event_class_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "MBORDER", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      val fil = df.filter("order_id <> '0' and (result_status_cd is null or result_status_cd not in (" + list_result_status_cd + ")) " +
        "and event_class_cd in (" + list_event_class_cd + ")")
      val groups = Window.partitionBy(fil("ORDER_ID"), fil("EVENT_ID"))
        .orderBy(fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .select("ORDER_ID", "EVENT_ID")
    }),
    "orders" -> ((df: DataFrame) => {
      val list_activity_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "MBORDER", "ORDERS", "ACTIVITY_TYPE_CD")
      df.filter("activity_type_cd in (" + list_activity_type_cd + ")")
    }),
    "ce_specimen_coll" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("EVENT_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM")
    }),
    "mic_task_log" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ORDER_ID")).orderBy(df("TASK_LOG_ID").asc)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .select("TASK_DT_TM", "ORDER_ID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    val zh = table("zh_code_value")
      .filter("code_set = '2052'")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_source")
      .withColumnRenamed("DEFINITION", "DEFINITION_source")
      .select("CODE_VALUE_source", "DEFINITION_source")
    val zh1 = table("zh_code_value")
      .filter("code_set = '14281'")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_dept")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_dept")
      .select("CODE_VALUE_dept", "DESCRIPTION_dept")
    val zh2 = table("zh_code_value")
      .filter("code_set = '200'")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_catalog")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_catalog")
      .select("CODE_VALUE_catalog", "DESCRIPTION_catalog")

    dfs("orders")
      .join(dfs("clinical_event"), Seq("ORDER_ID"), "left_outer")
      .join(dfs("ce_specimen_coll"), Seq("EVENT_ID"), "left_outer")
      .join(dfs("mic_task_log"), Seq("ORDER_ID"), "left_outer")
      .join(zh, dfs("ce_specimen_coll")("SOURCE_TYPE_CD") === zh("CODE_VALUE_source"), "left_outer")
      .join(zh1, dfs("orders")("DEPT_STATUS_CD") === zh1("CODE_VALUE_dept"), "left_outer")
      .join(zh2, dfs("orders")("CATALOG_CD") === zh2("CODE_VALUE_catalog"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("orders"),
    "LOCALORDERCODE" -> mapFrom("CATALOG_CD", prefix=config(CLIENT_DS_ID) + "."),
    "MBPROCORDERID" -> mapFrom("ORDER_ID"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "LOCALORDERDESC" -> mapFrom("DESCRIPTION_catalog"),
    "LOCALORDERSTATUS" -> mapFrom("DESCRIPTION_dept"),
    "LOCALSPECIMENNAME" -> mapFrom("DEFINITION_source"),
    "LOCALSPECIMENSOURCE" -> mapFrom("SOURCE_TYPE_CD", prefix=config(CLIENT_DS_ID) + "."),
    "DATECOLLECTED" -> mapFrom("COLLECT_DT_TM"),
    "DATEORDERED" -> mapFrom("ORIG_ORDER_DT_TM"),
    "DATERECEIVED" -> mapFrom("TASK_DT_TM"),
    "MBORDER_DATE" -> cascadeFrom(Seq("COLLECT_DT_TM", "TASK_DT_TM", "ORIG_ORDER_DT_TM")),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("MBPROCORDERID"), df("PATIENTID"), df("ENCOUNTERID"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}