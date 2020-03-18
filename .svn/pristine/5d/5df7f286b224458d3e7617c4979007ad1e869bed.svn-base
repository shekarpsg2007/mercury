package com.humedica.mercury.etl.hl7_v2.allergies

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._


/**
 * Created by bhenriksen on 1/19/17.
 */
class AllergiesAllergies(config: Map[String,String]) extends EntitySource(config: Map[String,String])  {

  tables = List(
    "allergies",
    "zh_cl_elg_ndc_list",
    "zh_cl_elg"
    )

  beforeJoin = Map(
    "allergies" -> ((df:DataFrame) => {
      val deduped = bestRowPerGroup(List("PAT_ID", "ALLERGEN_ID"), "UPDATE_DATE")(df)
      includeIf(
        "coalesce(Date_Noted ,Alrgy_Entered_Dttm ) IS NOT NULL " +
        "AND STATUS != 2 AND STATUS is not null " +
        "AND ALLERGEN_ID is not null " +
        "AND PAT_ID is not null")(deduped)
    }))


  join = (dfs: Map[String,DataFrame]) =>
    dfs("allergies")
      .join(dfs("zh_cl_elg")
        .join(dfs("zh_cl_elg_ndc_list"), Seq("ALLERGEN_ID"), "left_outer")
      ,Seq("ALLERGEN_ID"),"left_outer")




//join = leftOuterJoin("ALLERGEN_ID")


  map = Map(
    "DATASRC" -> literal("Allergies"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ONSETDATE" -> cascadeFrom(Seq("DATE_NOTED", "ALRGY_ENTERED_DTTM")),
    "LOCALALLERGENCD" -> mapFrom("ALLERGEN_ID"),
    "LOCALALLERGENDESC" -> mapFrom("DESCRIPTION"),
    "LOCALSTATUS" -> mapFrom("STATUS", nullIf = Seq(null),prefix=config(CLIENT_DS_ID)+"."),      //prefix with '[client_ds_id].'
    "LOCALALLERGENTYPE" -> ((col, df) => df.withColumn(col,concat(lit((config(CLIENT_DS_ID) + ".")), (if (df("ALLERGEN_TYPE_C") == null) lit("Unknown")
      else df("ALLERGEN_TYPE_C"))))), //prefix with '[client_ds_id].' and swap null value for literal 'Unknown'

    "LOCALNDC" -> mapFrom("MED_INTRCT_NDC")
  )

  mapExceptions = Map(
    ("H477171", "LOCALALLERGENCD") -> todo("ALLERGEN_ID"),      //TODO - to be coded
    ("H477171", "LOCALALLERGENCD") -> todo("ALLERGEN_ID"),      //TODO - to be coded
    ("H458934", "LOCALSTATUS") -> todo("STATUS"),      //TODO - to be coded
    ("H477171", "LOCALALLERGENCD") -> todo("ALLERGEN_ID")      //TODO - to be coded
  )
}
