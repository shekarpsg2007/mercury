package com.humedica.mercury.etl.asent.observation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, _}
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "as_orders",
    "cdr.zcm_obstype_code",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "as_orders" -> List("ORDER_ITEM_ID", "CLINICAL_DATETIME", "PATIENT_MRN", "ORDER_STATUS_ID", "ENCOUNTER_ID", "ORDER_ITEM_ID"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "DATATYPE")
  )


  beforeJoin = Map(
    "as_orders" -> ((df: DataFrame) => {
      val excl_status_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EXCLUSION_STATUS", "OBSERVATION", "AS_ORDERS", "ORDER_STATUS_ID")
      val groups = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("ORDER_ITEM_ID")).orderBy(df("CLINICAL_DATETIME").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1 AND ORDER_STATUS_ID  NOT IN (" + excl_status_id + ") AND PATIENT_MRN IS NOT NULL AND CLINICAL_DATETIME IS NOT NULL").drop("rn")
    }
      ))


  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_orders")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("obscode") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_orders")("order_item_id")) && dfs("cdr.zcm_obstype_code")("groupid") === lit(config(GROUP)) && dfs("cdr.zcm_obstype_code")("datasrc") === lit("orders"), "inner")
  }


  afterJoin = (df: DataFrame) => {
    val groups2 = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("ORDER_ITEM_ID"), df("OBSTYPE")).orderBy((df("CLINICAL_DATETIME").desc_nulls_last))
    df.withColumn("rw", row_number.over(groups2))
      .filter("rw=1").drop("rw")
      .withColumn("localresult", when(df("DATATYPE") === lit("CV"), concat_ws(".", lit(config(CLIENT_DS_ID)), df("ORDER_ITEM_ID"))).otherwise(df("ORDER_ITEM_ID")))
      .distinct()
  }


  map = Map(
    "DATASRC" -> literal("orders"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "OBSDATE" -> mapFrom("CLINICAL_DATETIME"),
    "LOCALCODE" -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("ORDER_ITEM_ID")))),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "STATUSCODE" -> mapFrom("ORDER_STATUS_ID"),
    "LOCALRESULT" -> mapFrom("localresult")
  )
}
