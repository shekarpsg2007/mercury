package com.humedica.mercury.etl.cerner_v2.immunization

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 08/09/2018
 */


class ImmunizationCemedresult(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "ce_med_result", "zh_med_identifier", "zh_code_value", "zh_order_catalog_item_r",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "clinical_event" -> List("PERSON_ID", "ENCNTR_ID", "EVENT_CD", "EVENT_ID", "EVENT_CLASS_CD", "CATALOG_CD"),
    "ce_med_result" -> List("ADMIN_START_DT_TM", "EVENT_ID"),
    "zh_med_identifier" -> List("VALUE_KEY", "ITEM_ID", "ACTIVE_IND", "UPDT_DT_TM"),
    "zh_code_value" -> List("CODE_VALUE", "DESCRIPTION"),
    "zh_order_catalog_item_r" -> List("CATALOG_CD", "ITEM_ID")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_event_class_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "IMMUNIZATION_MODIFIER", "IMMUNIZATION", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      df.filter("event_cd is not null and person_id is not null and event_class_cd in (" + list_event_class_cd + ")")
    }),
    "zh_med_identifier" -> ((df: DataFrame) => {
      val fil = df.filter("med_identifier_type_cd = '3104'")
      val groups = Window.partitionBy(fil("ITEM_ID"))
        .orderBy(fil("ACTIVE_IND").desc_nulls_last, fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("ce_med_result")
      .join(dfs("clinical_event"), Seq("EVENT_ID"), "inner")
      .join(dfs("zh_code_value"), dfs("clinical_event")("EVENT_CD") === dfs("zh_code_value")("CODE_VALUE"), "left_outer")
      .join(dfs("zh_order_catalog_item_r"), Seq("CATALOG_CD"), "left_outer")
      .join(dfs("zh_med_identifier"), Seq("ITEM_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("ce_med_result"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "ADMINDATE" -> mapFrom("ADMIN_START_DT_TM"),
    "DOCUMENTEDDATE" -> mapFrom("ADMIN_START_DT_TM"),
    "LOCALIMMUNIZATIONCD" -> mapFrom("EVENT_CD"),
    "LOCALIMMUNIZATIONDESC" -> mapFrom("DESCRIPTION"),
    "LOCALNDC" -> mapFrom("VALUE_KEY")
  )
  
  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("LOCALIMMUNIZATIONCD"), df("ADMINDATE"))
      .orderBy(df("ADMINDATE").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}