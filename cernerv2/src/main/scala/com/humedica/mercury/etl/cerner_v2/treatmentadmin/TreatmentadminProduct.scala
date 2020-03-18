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


class TreatmentadminProduct(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("product", "product_event", "cdr.zcm_treatment_type_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "product" -> List("PRODUCT_ID", "PRODUCT_CD", "BARCODE_NBR", "UPDT_DT_TM"),
    "product_event" -> List("EVENT_DT_TM", "PRODUCT_ID", "PERSON_ID", "EVENT_PRSNL_ID", "ORDER_ID", "ENCNTR_ID", "UPDT_DT_TM"),
    "cdr.zcm_treatment_type_code" -> List("GROUPID", "LOCAL_CODE", "TREATMENT_TYPE_CUI", "LOCAL_UNIT", "TREATMENT_TYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "product" -> ((df: DataFrame) => {
      val df2 = df.filter("product_cd is not null")
        .withColumn("LOCALCODE", concat_ws(".", lit(config(CLIENT_DS_ID)), df("PRODUCT_CD")))
      val groups = Window.partitionBy(df2("PRODUCT_ID"), df2("BARCODE_NBR"))
        .orderBy(df2("UPDT_DT_TM").desc_nulls_last)
      df2.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM")
    }),
    "product_event" -> ((df: DataFrame) => {
      val list_event_type = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PRODUCT", "TREATMENT_ADMIN", "PRODUCT_EVENT", "EVENT_TYPE_CD")
      val df2 = df.filter("event_dt_tm is not null and person_id is not null and event_Type_cd in (" + list_event_type + ")")
      val groups = Window.partitionBy(df2("PRODUCT_ID")).orderBy(df2("UPDT_DT_TM").desc_nulls_last)
      df2.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM")
    }),
    "cdr.zcm_treatment_type_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "'")
        .drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("product")
      .join(dfs("product_event"), Seq("PRODUCT_ID"), "inner")
      .join(dfs("cdr.zcm_treatment_type_code"), dfs("product")("LOCALCODE") === dfs("cdr.zcm_treatment_type_code")("LOCAL_CODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("product"),
    "ADMINISTERED_ID" -> mapFrom("PRODUCT_ID"),
    "ADMINISTERED_DATE" -> mapFrom("EVENT_DT_TM"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ADMINISTERED_PROV_ID" -> mapFrom("EVENT_PRSNL_ID", nullIf = Seq("1")),
    "ORDER_ID" -> mapFrom("ORDER_ID", nullIf = Seq("0")),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "CUI" -> mapFrom("TREATMENT_TYPE_CUI"),
    "STD_UNIT_CUI" -> mapFrom("TREATMENT_TYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val groups_cnt = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"))
    val groups_rn = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"))
      .orderBy(df("ADMINISTERED_DATE").desc_nulls_last)
    df.withColumn("ADMINISTERED_QUANTITY", count("*").over(groups_cnt))
      .withColumn("rn", row_number.over(groups_rn))
      .filter("rn = 1")
      .drop("rn")
  }

}