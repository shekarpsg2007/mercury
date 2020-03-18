package com.humedica.mercury.etl.asent.allergies

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window


class AllergiesAllergies(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_allergen_de",
    "as_allergies",
    "as_zc_medication_de",
    "as_zc_medication_allergy_de")

  columnSelect = Map(
    "as_zc_allergen_de" -> List("ID", "ENTRYNAME"),
    "as_allergies" -> List("MEDICATION_ALLERGY_ID", "REACTION_DATE_TIME", "PATIENT_MRN", "ENCOUNTER_ID", "ALLERGY_STATUS_ID", "NON_MEDICATION_ALLERGY_ID", "RECORDED_DATE_TIME", "LAST_UPDATED_DATE"),
    "as_zc_medication_de" -> List("GPI_TC3", "NDC", "ID"),
    "as_zc_medication_allergy_de" -> List("ENTRYNAME", "ID", "MEDDICTDE")
  )

  beforeJoin = Map(
    "as_allergies" -> ((df: DataFrame) => {
      val df1 = df.withColumn("MEDICATION_ALLERGY_ID_new", when(df("MEDICATION_ALLERGY_ID") === "0", null).otherwise(df("MEDICATION_ALLERGY_ID")))
        .withColumn("NON_MEDICATION_ALLERGY_ID_new", when(df("NON_MEDICATION_ALLERGY_ID") === "0", null).otherwise(df("NON_MEDICATION_ALLERGY_ID")))
      val groups = Window.partitionBy(df1("PATIENT_MRN"), coalesce(df1("REACTION_DATE_TIME"), df1("RECORDED_DATE_TIME")), df1("ENCOUNTER_ID"), df1("MEDICATION_ALLERGY_ID_new"), df1("NON_MEDICATION_ALLERGY_ID_new"))
        .orderBy(df1("LAST_UPDATED_DATE").desc)
      df1.withColumn("rn", row_number.over(groups))
        .filter("rn=1 and ALLERGY_STATUS_ID != '4'")
        .select("MEDICATION_ALLERGY_ID_new", "REACTION_DATE_TIME", "PATIENT_MRN", "ENCOUNTER_ID", "ALLERGY_STATUS_ID", "NON_MEDICATION_ALLERGY_ID_new", "RECORDED_DATE_TIME")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_allergies")
      .join(dfs("as_zc_allergen_de"), dfs("as_zc_allergen_de")("id") === dfs("as_allergies")("NON_MEDICATION_ALLERGY_ID_new"), "left_outer")
      .join(dfs("as_zc_medication_allergy_de")
        .join(dfs("as_zc_medication_de"), dfs("as_zc_medication_de")("id") === dfs("as_zc_medication_allergy_de")("meddictde"), "left_outer")
        , dfs("as_zc_medication_allergy_de")("id") === dfs("as_allergies")("MEDICATION_ALLERGY_ID_new"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("allergies"),
    "LOCALALLERGENTYPE" -> ((col, df) => df.withColumn(col, when(df("MEDICATION_ALLERGY_ID_new").isNotNull, lit("Med")).otherwise(lit("Non-Med")))),
    "LOCALALLERGENCD" -> ((col, df) => df.withColumn(col, when(df("MEDICATION_ALLERGY_ID_new").isNotNull, concat_ws(".", df("CLIENT_DS_ID"), df("MEDICATION_ALLERGY_ID_new"))).otherwise(concat(lit("n."), df("NON_MEDICATION_ALLERGY_ID_new"))))),
    "ONSETDATE" -> cascadeFrom(Seq("REACTION_DATE_TIME", "RECORDED_DATE_TIME"), nullIf = Seq("0")),
    "PATIENTID" -> mapFrom("PATIENT_MRN", nullIf = Seq("0")),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID", nullIf = Seq("0")),
    "LOCALGPI" -> mapFrom("GPI_TC3", nullIf = Seq("0")),
    "LOCALSTATUS" -> mapFrom("ALLERGY_STATUS_ID", nullIf = Seq("0")),
    "LOCALALLERGENDESC" -> ((col, df) => df.withColumn(col, when(df("MEDICATION_ALLERGY_ID_new").isNotNull, df("AS_ZC_MEDICATION_ALLERGY_DE.ENTRYNAME")).otherwise(df("AS_ZC_ALLERGEN_DE.ENTRYNAME")))),
    "LOCALNDC" -> mapFrom("NDC", nullIf = Seq("0"))
  )
}