package com.humedica.mercury.etl.athena.rxmedadministrations

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class RxmedadministrationsPatientmedication(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinicalencounter:athena.util.UtilDedupedClinicalEncounter",
    "medication:athena.util.UtilDedupedMedication",
    "document:athena.util.UtilDedupedDocument",
    "patientmedication:athena.util.UtilDedupedPatientMedication")

  columnSelect = Map(
    "clinicalencounter" -> List("PROVIDER_ID", "CLINICAL_ENCOUNTER_ID"),
    "medication" -> List("MEDICATION_NAME", "NDC", "RXNORM", "FBD_MED_ID"),
    "document" -> List("PATIENT_ID", "FBD_MED_ID", "DOCUMENT_ID", "CLINICAL_ENCOUNTER_ID", "VACCINE_ROUTE"),
    "patientmedication" -> List("CLINICAL_ORDER_TYPE", "PATIENT_MEDICATION_ID", "DISPLAY_DOSAGE_UNITS", "NDC",
      "DOSAGE_QUANTITY", "DOSAGE_ROUTE", "RXNORM", "MED_ADMINISTERED_DATETIME", "CLINICAL_ENCOUNTER_ID", "DOSAGE_FORM",
      "PATIENT_ID", "DOCUMENT_DESCRIPTION", "DOCUMENT_ID", "MEDICATION_TYPE")
  )

  beforeJoin = Map(
    "medication" -> renameColumns(List(("NDC", "NDC_med"), ("RXNORM", "RXNORM_med"))),
    "document" -> renameColumn("PATIENT_ID", "PATIENT_ID_doc"),
    "patientmedication" -> ((df: DataFrame) => {
      df.filter("medication_type = 'CLINICALPRESCRIPTION' and med_administered_datetime is not null")
        .withColumnRenamed("RXNORM", "RXNORM_pm")
        .withColumnRenamed("NDC", "NDC_pm")
        .withColumnRenamed("CLINICAL_ENCOUNTER_ID", "CLINICAL_ENCOUNTER_ID_pm")
        .withColumnRenamed("PATIENT_ID", "PATIENT_ID_pm")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patientmedication")
      .join(dfs("document"), Seq("DOCUMENT_ID"), "left_outer")
      .join(dfs("clinicalencounter"), Seq("CLINICAL_ENCOUNTER_ID"), "left_outer")
      .join(dfs("medication"), Seq("FBD_MED_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("patientmedication"),
    "PATIENTID" -> cascadeFrom(Seq("PATIENT_ID_pm", "PATIENT_ID_doc")),
    "RXADMINISTRATIONID" -> mapFrom("PATIENT_MEDICATION_ID"),
    "RXORDERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat(df("PATIENT_MEDICATION_ID"), lit("_1")))
    }),
    "LOCALDOSEUNIT" -> mapFrom("DISPLAY_DOSAGE_UNITS"),
    "LOCALDRUGDESCRIPTION" -> cascadeFrom(Seq("MEDICATION_NAME", "DOCUMENT_DESCRIPTION")),
    "LOCALGENERICDESC" -> mapFrom("MEDICATION_NAME"),
    "LOCALMEDCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("FBD_MED_ID"), when(df("RXNORM_pm").isNotNull, concat(lit("r."), df("RXNORM_pm")))))
    }),
    "LOCALNDC" -> cascadeFrom(Seq("NDC_med", "NDC_pm")),
    "LOCALQTYOFDOSEUNIT" -> mapFrom("DOSAGE_QUANTITY"),
    "LOCALROUTE" -> cascadeFrom(Seq("DOSAGE_ROUTE", "VACCINE_ROUTE")),
    "LOCALSTRENGTHPERDOSEUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, regexp_extract(lower(df("MEDICATION_NAME")), "([0-9]*\\.*[0-9]+)\\s*((m*c*g|ml|meq|milligram))", 1))
    }),
    "LOCALSTRENGTHUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, regexp_extract(lower(df("MEDICATION_NAME")), "([0-9]*\\.*[0-9]+)\\s*((m*c*g|ml|meq|milligram))", 2))
    }),
    "LOCALPROVIDERID" -> mapFrom("PROVIDER_ID"),
    "ADMINISTRATIONTIME" -> mapFrom("MED_ADMINISTERED_DATETIME"),
    "ENCOUNTERID" -> cascadeFrom(Seq("CLINICAL_ENCOUNTER_ID", "CLINICAL_ENCOUNTER_ID_pm")),
    "LOCALFORM" -> mapFrom("DOSAGE_FORM"),
    "RXNORM_CODE" -> cascadeFrom(Seq("RXNORM_pm", "RXNORM_med"))
  )

  afterMap = (df: DataFrame) => {
    df.filter("patientid is not null and localmedcode is not null")
  }

}