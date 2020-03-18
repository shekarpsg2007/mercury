package com.humedica.mercury.etl.athena.rxordersandprescriptions

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class RxordersandprescriptionsPatientmedicationcp(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinicalencounter:athena.util.UtilDedupedClinicalEncounter",
    "medication:athena.util.UtilDedupedMedication",
    "document:athena.util.UtilDedupedDocument",
    "patientmedication:athena.util.UtilDedupedPatientMedication")

  columnSelect = Map(
    "clinicalencounter" -> List("PROVIDER_ID", "CLINICAL_ENCOUNTER_ID"),
    "medication" -> List("RXNORM", "MEDICATION_NAME", "NDC"),
    "document" -> List("ORDER_DATETIME", "PATIENT_ID", "CLINICAL_ENCOUNTER_ID", "FBD_MED_ID", "CLINICAL_ORDER_TYPE",
      "STATUS", "CREATED_DATETIME", "DEACTIVATED_DATETIME", "VACCINE_ROUTE", "DOCUMENT_ID"),
    "patientmedication" -> List("MED_ADMINISTERED_DATETIME", "PATIENT_MEDICATION_ID", "LENGTH_OF_COURSE", "STOP_DATE",
      "DEACTIVATION_REASON", "DEACTIVATION_DATETIME", "DISPLAY_DOSAGE_UNITS", "LENGTH_OF_COURSE", "DOSAGE_FORM",
      "DOSAGE_QUANTITY", "DOSAGE_ROUTE", "DOSAGE_STRENGTH", "DOSAGE_STRENGTH_UNITS", "NUMBER_OF_REFILLS_PRESCRIBED",
      "MED_ADMINISTERED_DATETIME", "PRESCRIPTION_FILL_QUANTITY", "SIG", "DOCUMENT_ID", "PATIENT_ID", "MEDICATION_NAME",
      "DOCUMENT_DESCRIPTION", "STATUS", "RXNORM")
  )

  beforeJoin = Map(
    "medication" -> ((df: DataFrame) => {
      df.withColumnRenamed("RXNORM", "RXNORM_med")
        .withColumnRenamed("MEDICATION_NAME", "MEDICATION_NAME_med")
    }),
    "document" -> ((df: DataFrame) => {
      df.filter("denied_datetime is null")
        .withColumnRenamed("PATIENT_ID", "PATIENT_ID_doc")
        .withColumnRenamed("STATUS", "STATUS_doc")
    }),
    "patientmedication" -> ((df: DataFrame) => {
      df.filter("denied_datetime is null and medication_type = 'CLINICALPRESCRIPTION'")
        .withColumnRenamed("PATIENT_ID", "PATIENT_ID_pm")
        .withColumnRenamed("RXNORM", "RXNORM_pm")
        .withColumnRenamed("MEDICATION_NAME", "MEDICATION_NAME_pm")
        .withColumnRenamed("STATUS", "STATUS_pm")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("document")
      .join(dfs("medication"), Seq("FBD_MED_ID"), "inner")
      .join(dfs("patientmedication"), Seq("DOCUMENT_ID"), "inner")
      .join(dfs("clinicalencounter"), Seq("CLINICAL_ENCOUNTER_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("patientmedication_cp"),
    "ISSUEDATE" -> cascadeFrom(Seq("ORDER_DATETIME", "CREATED_DATETIME")),
    "ORDERVSPRESCRIPTION" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("MED_ADMINISTERED_DATETIME").isNotNull, lit("O")).otherwise(lit("P")))
    }),
    "PATIENTID" -> cascadeFrom(Seq("PATIENT_ID_pm", "PATIENT_ID_doc")),
    "RXID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat(df("PATIENT_MEDICATION_ID"), lit("_1")))
    }),
    "ALTMEDCODE" -> mapFrom("RXNORM_med"),
    "LOCALDAYSUPPLIED" -> mapFrom("LENGTH_OF_COURSE"),
    "DISCONTINUEDATE" -> cascadeFrom(Seq("STOP_DATE", "DEACTIVATION_DATETIME", "DEACTIVATED_DATETIME")),
    "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
    "LOCALDOSEUNIT" -> cascadeFrom(Seq("DISPLAY_DOSAGE_UNITS", "DOSAGE_FORM")),
    "LOCALDESCRIPTION" -> cascadeFrom(Seq("MEDICATION_NAME_med", "MEDICATION_NAME_pm", "DOCUMENT_DESCRIPTION")),
    "LOCALDURATION" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("LENGTH_OF_COURSE").isNull, regexp_extract(lower(df("SIG")), "(x|for)\\s*([0-9]+)\\s*days*", 2))
        .otherwise(null))
    }),
    "LOCALFORM" -> mapFrom("DOSAGE_FORM"),
    "LOCALGENERICDESC" -> cascadeFrom(Seq("MEDICATION_NAME_med", "MEDICATION_NAME_pm")),
    "LOCALMEDCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(coalesce(df("FBD_MED_ID"), df("CLINICAL_ORDER_TYPE")), 1, 100))
    }),
    "LOCALNDC" -> mapFrom("NDC"),
    "LOCALPROVIDERID" -> mapFrom("PROVIDER_ID"),
    "LOCALQTYOFDOSEUNIT" -> mapFrom("DOSAGE_QUANTITY"),
    "LOCALROUTE" -> cascadeFrom(Seq("DOSAGE_ROUTE", "VACCINE_ROUTE")),
    "LOCALSTRENGTHPERDOSEUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, lower(trim(coalesce(df("DOSAGE_STRENGTH_UNITS"), regexp_extract(lower(df("MEDICATION_NAME")), "([0-9]*\\.*[0-9]+)\\s*((m*c*g|ml|meq|milligram))", 1)))))
    }),
    "LOCALSTRENGTHUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, lower(trim(coalesce(df("DOSAGE_STRENGTH_UNITS"), regexp_extract(lower(df("MEDICATION_NAME")), "([0-9]*\\.*[0-9]+)\\s*((m*c*g|ml|meq|milligram))", 2)))))
    }),
    "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, df("DOSAGE_QUANTITY").multiply(df("DOSAGE_STRENGTH")))
    }),
    "FILLNUM" -> mapFrom("NUMBER_OF_REFILLS_PRESCRIBED"),
    "ORDERSTATUS" -> cascadeFrom(Seq("STATUS_doc", "STATUS_pm")),
    "ORDERTYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("MED_ADMINISTERED_DATETIME").isNotNull, lit("CH002045")).otherwise(lit("CH002047")))
    }),
    "QUANTITYPERFILL" -> mapFrom("PRESCRIPTION_FILL_QUANTITY"),
    "SIGNATURE" -> mapFrom("SIG"),
    "VENUE" -> literal("1"),
    "RXNORM_CODE" -> cascadeFrom(Seq("RXNORM_med", "RXNORM_pm"))
  )

  afterMap = (df: DataFrame) => {
    df.filter("patientid is not null")
  }

}