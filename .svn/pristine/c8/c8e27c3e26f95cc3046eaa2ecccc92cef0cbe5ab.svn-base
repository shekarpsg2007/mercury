package com.humedica.mercury.etl.athena.encounterprovider

import com.humedica.mercury.etl.athena.util.UtilSplitTable
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class EncounterproviderClinicalencounter(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List(
    "clinicalencounter:athena.util.UtilDedupedClinicalEncounter"
    , "transclaim:athena.util.UtilDedupedTransactionClaim"
    , "claim:athena.util.UtilDedupedClaim")

  columnSelect = Map(
    "clinicalencounter" -> List("ENCOUNTER_DATE", "PATIENT_ID", "DEPARTMENT_ID", "CLINICAL_ENCOUNTER_ID", "APPOINTMENT_ID")
    , "transclaim" -> List("CLAIM_ID", "CLAIM_APPOINTMENT_ID", "RENDERING_PROVIDER_ID", "SUPERVISING_PROVIDER_ID", "CLAIM_SCHEDULING_PROVIDER_ID")
    , "claim" -> List("CLAIM_ID", "CLAIM_APPOINTMENT_ID", "RENDERING_PROVIDER_ID", "SUPERVISING_PROVIDER_ID", "CLAIM_SCHEDULING_PROVIDER_ID")
  )

  beforeJoin = Map(
    "transclaim" -> ((df: DataFrame) => {
      val clm = table("claim")
        .withColumnRenamed("CLAIM_APPOINTMENT_ID", "clm_CLAIM_APPOINTMENT_ID")
        .withColumnRenamed("RENDERING_PROVIDER_ID", "clm_RENDERING_PROVIDER_ID")
        .withColumnRenamed("SUPERVISING_PROVIDER_ID", "clm_SUPERVISING_PROVIDER_ID")
        .withColumnRenamed("CLAIM_SCHEDULING_PROVIDER_ID", "clm_CLAIM_SCHEDULING_PROVIDER_ID")
      val trnClmDf =
        df.join(clm, Seq("CLAIM_ID"), "left_outer")
          .withColumn("APPOINTMENT_ID", coalesce(df("CLAIM_APPOINTMENT_ID"), clm("clm_CLAIM_APPOINTMENT_ID")))
          .withColumn("RENDERING_PROVIDER", coalesce(df("RENDERING_PROVIDER_ID"), clm("clm_RENDERING_PROVIDER_ID")))
          .withColumn("SUPERVISING_PROVIDER", coalesce(df("SUPERVISING_PROVIDER_ID"), clm("clm_SUPERVISING_PROVIDER_ID")))
          .withColumn("CLAIM_SCHEDULING_PROVIDER", coalesce(df("CLAIM_SCHEDULING_PROVIDER_ID"), clm("clm_CLAIM_SCHEDULING_PROVIDER_ID")))
          .select("APPOINTMENT_ID", "RENDERING_PROVIDER", "SUPERVISING_PROVIDER", "CLAIM_SCHEDULING_PROVIDER")
      val pivottrnClm = unpivot(
        Seq("RENDERING_PROVIDER", "SUPERVISING_PROVIDER", "CLAIM_SCHEDULING_PROVIDER"),
        Seq("Rendering Provider", "Supervising Provider", "Scheduling Provider"), typeColumnName = "PROVIDERROLE")
      pivottrnClm("PROVIDERID", trnClmDf)
    }))


  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinicalencounter")
      .join(dfs("transclaim"), Seq("APPOINTMENT_ID"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    df.filter("PATIENT_ID is not null and PROVIDERID is not null and ENCOUNTER_DATE is not null")
  }

  afterJoinExceptions = Map(
    "H542284_ATH_DWF4" -> ((df: DataFrame) => {
      df.filter("PATIENT_ID is not null and PROVIDERID is not null and ENCOUNTER_DATE is not null and providerrole <> 'Scheduling Provider'")
    })
  )

  map = Map(
    "DATASRC" -> literal("clinicalencounter"),
    "ENCOUNTERTIME" -> mapFrom("ENCOUNTER_DATE"),
    "PROVIDERID" -> mapFrom("PROVIDERID"),
    "PROVIDERROLE" -> mapFrom("PROVIDERROLE"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "FACILITYID" -> mapFrom("DEPARTMENT_ID"),
    "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID")
  )

}


// test
//  val epc = new EncounterproviderClinicalencounter(cfg); val ep = build(epc,true); ep.show() ; ep.count;
