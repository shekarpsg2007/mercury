package com.humedica.mercury.etl.cerner_v2.allergies

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class AllergiesAllergy(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("allergy", "zh_nomenclature")

  columnSelect = Map(
    "allergy" -> List("SUBSTANCE_TYPE_CD", "SUBSTANCE_NOM_ID", "ONSET_DT_TM", "PERSON_ID", "ENCNTR_ID",
      "REACTION_STATUS_CD", "REACTION_STATUS_DT_TM", "SUBSTANCE_FTDESC", "CANCEL_REASON_CD", "UPDT_DT_TM", "ACTIVE_IND"),
    "zh_nomenclature" -> List("SOURCE_STRING_KEYCAP", "NOMENCLATURE_ID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("allergy")
      .join(dfs("zh_nomenclature"), dfs("allergy")("SUBSTANCE_NOM_ID") === dfs("zh_nomenclature")("NOMENCLATURE_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("allergy"),
    "LOCALALLERGENTYPE" -> mapFrom("SUBSTANCE_TYPE_CD", prefix=config(CLIENT_DS_ID)+".", nullIf=Seq("0")),
    "LOCALALLERGENCD" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("SUBSTANCE_NOM_ID") === lit("0") || df("SUBSTANCE_NOM_ID").isNull, substring(df("SUBSTANCE_FTDESC"), 1, 249))
        .otherwise(substring(df("SUBSTANCE_NOM_ID"), 1, 249)))
    }),
    "ONSETDATE" -> cascadeFrom(Seq("ONSET_DT_TM", "REACTION_STATUS_DT_TM")),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID", nullIf=Seq("0")),
    "LOCALSTATUS" -> mapFrom("REACTION_STATUS_CD"),
    "LOCALALLERGENDESC" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(coalesce(df("SOURCE_STRING_KEYCAP"), df("SUBSTANCE_FTDESC")), 1, 249))
    })
  )
      
  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("LOCALALLERGENCD"), df("ONSETDATE"))
      .orderBy(df("ACTIVE_IND").desc_nulls_last, df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and patientid is not null and localallergencd is not null and onsetdate is not null and cancel_reason_cd = '0'")
      .drop("rn")
  }

}