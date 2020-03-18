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


class ImmunizationImmunizationmodifier(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "immunization_modifier", "zh_code_value", "cdr.map_predicate_values",
    "temp_med:cerner_v2.rxordersandprescriptions.RxordersandprescriptionsTempmed")

  columnSelect = Map(
    "clinical_event" -> List("ENCNTR_ID", "EVENT_ID", "CATALOG_CD", "EVENT_CLASS_CD", "UPDT_DT_TM"),
    "immunization_modifier" -> List("VIS_DT_TM", "PERSON_ID", "VIS_PROVIDED_ON_DT_TM", "BEG_EFFECTIVE_DT_TM", "EVENT_CD",
      "PERSON_ID", "UPDT_DT_TM", "EVENT_ID"),
    "zh_code_value" -> List("CODE_VALUE", "DESCRIPTION"),
    "temp_med" -> List("CATALOG_CD", "LOCALNDC")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_event_class_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "IMMUNIZATION_MODIFIER", "IMMUNIZATION", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      val fil = df.filter("event_class_cd in (" + list_event_class_cd + ")")
      val groups = Window.partitionBy(fil("EVENT_ID")).orderBy(fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn")
        .withColumnRenamed("UPDT_DT_TM", "UPDT_DT_TM_ce")
    }),
    "immunization_modifier" -> ((df: DataFrame) => {
      df.filter("coalesce(person_id, '0') <> '0' and coalesce(event_cd, '0') <> '0'")
        .withColumnRenamed("UPDT_DT_TM", "UPDT_DT_TM_im")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("immunization_modifier")
      .join(dfs("clinical_event"), Seq("EVENT_ID"), "inner")
      .join(dfs("zh_code_value"), dfs("immunization_modifier")("EVENT_CD") === dfs("zh_code_value")("CODE_VALUE"), "left_outer")
      .join(dfs("temp_med"), Seq("CATALOG_CD"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("immunization_modifier"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "ADMINDATE" -> cascadeFrom(Seq("VIS_PROVIDED_ON_DT_TM", "BEG_EFFECTIVE_DT_TM")),
    "DOCUMENTEDDATE" -> cascadeFrom(Seq("VIS_PROVIDED_ON_DT_TM", "BEG_EFFECTIVE_DT_TM")),
    "LOCALIMMUNIZATIONCD" -> mapFrom("EVENT_CD"),
    "LOCALIMMUNIZATIONDESC" -> mapFrom("DESCRIPTION"),
    "LOCALNDC" -> mapFrom("LOCALNDC")
  )
  
  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("LOCALIMMUNIZATIONCD"), df("ADMINDATE"))
      .orderBy(df("UPDT_DT_TM_im").desc_nulls_last, df("UPDT_DT_TM_ce").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

  mapExceptions = Map(
    ("H984197_CR2", "ADMINDATE") -> cascadeFrom(Seq("VIS_DT_TM", "BEG_EFFECTIVE_DT_TM")),
    ("H984442_CR2", "ADMINDATE") -> cascadeFrom(Seq("VIS_DT_TM", "BEG_EFFECTIVE_DT_TM"))
  )

}