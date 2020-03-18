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


class RxordersandprescriptionsPatientmedicationpm(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("medication:athena.util.UtilDedupedMedication",
    "patientmedication:athena.util.UtilDedupedPatientMedication",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "medication" -> List("RXNORM", "MEDICATION_NAME", "NDC", "MEDICATION_ID"),
    "patientmedication" -> List("MEDICATION_ID", "RXNORM", "DOCUMENT_DESCRIPTION", "DOCUMENT_DESCRIPTION", "START_DATE",
      "PATIENT_ID", "PATIENT_MEDICATION_ID", "LENGTH_OF_COURSE", "STOP_DATE", "DEACTIVATION_REASON",
      "DISPLAY_DOSAGE_UNITS", "DOSAGE_FORM", "MEDICATION_NAME", "LENGTH_OF_COURSE", "DOSAGE_FORM",
      "MEDICATION_ID", "MEDICATION_NAME", "DOCUMENT_DESCRIPTION", "NDC", "PRESCRIBER_NPI",
      "SIG", "DOSAGE_ROUTE", "NUMBER_OF_REFILLS_PRESCRIBED", "PRESCRIPTION_FILL_QUANTITY", "RXNORM",
      "SIG")
  )

  beforeJoin = Map(
    "medication" -> ((df: DataFrame) => {
      df.withColumnRenamed("RXNORM", "RXNORM_med")
        .withColumnRenamed("MEDICATION_NAME", "MEDICATION_NAME_med")
        .withColumnRenamed("NDC", "NDC_med")
    }),
    "patient_medication" -> ((df: DataFrame) => {
      df.filter("deleted_datetime is null and coalesce(deactivation_reason, 'X') <> 'entered in error' and medication_type = 'PATIENTMEDICATION'")
        .withColumnRenamed("RXNORM", "RXNORM_pm")
        .withColumnRenamed("MEDICATION_NAME", "MEDICATION_NAME_pm")
        .withColumnRenamed("NDC", "NDC_pm")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patientmedication")
      .join(dfs("medication"), Seq("MEDICATION_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("patientmedication_pm"),
    "ISSUEDATE" -> cascadeFrom(Seq("FILL_DATE", "START_DATE", "CREATED_DATETIME")),
    "ORDERVSPRESCRIPTION" -> literal("P"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "RXID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat(df("PATIENT_MEDICATION_ID"), lit("_2")))
    }),
    "ALTMEDCODE" -> mapFrom("RXNORM_med"),
    "LOCALDAYSUPPLIED" -> mapFrom("LENGTH_OF_COURSE"),
    "DISCONTINUEDATE" -> cascadeFrom(Seq("STOP_DATE", "DEACTIVATION_DATETIME")),
    "DISCONTINUEREASON" -> mapFrom("DEACTIVATION_REASON"),
    "LOCALDOSEUNIT" -> cascadeFrom(Seq("DISPLAY_DOSAGE_UNITS", "DOSAGE_FORM")),
    "LOCALDESCRIPTION" -> cascadeFrom(Seq("MEDICATION_NAME_pm", "MEDICATION_NAME_med", "DOCUMENT_DESCRIPTION")),
    "LOCALDURATION" -> mapFrom("LENGTH_OF_COURSE"),
    "LOCALFORM" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("DOSAGE_FORM"),
        regexp_extract(lower(df("SIG")), "([0-9]+\\.*[0-9]*|one|two|three|four|five)+\\s*((tab|unit|caps|spray|patch|puff|t\\s|ts\\s|tea\\s|drop))", 2)))
    }),
    "LOCALGENERICDESC" -> cascadeFrom(Seq("MEDICATION_NAME_med", "MEDICATION_NAME_pm")),
    "LOCALMEDCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(coalesce(df("MEDICATION_ID"), df("MEDICATION_NAME_pm"), df("DOCUMENT_DESCRIPTION")),1, 100))
    }),
    "LOCALNDC" -> cascadeFrom(Seq("NDC_med", "NDC_pm")),
    "LOCALPROVIDERID" -> mapFrom("PRESCRIBER_NPI", prefix="npi."),
    "LOCALQTYOFDOSEUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, regexp_extract(lower(df("SIG")), "([0-9]+\\.*[0-9]*|one|two|three|four|five)+\\s*((tab|unit|caps|spray|patch|puff|t\\s|ts\\s|tea\\s|drop))", 1))
    }),
    "LOCALROUTE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("DOSAGE_ROUTE"), regexp_extract(lower(df("SIG")), "(\\w*?\\s*route|by\\smouth|\\spo\\s|\\siv\\s)", 1)))
    }),
    "LOCALSTRENGTHPERDOSEUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, trim(regexp_extract(lower(df("MEDICATION_NAME_med")), "([0-9]*\\.*[0-9]+)\\s*((m*c*g|ml|meq|milligram))", 1)))
    }),
    "LOCALSTRENGTHUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, trim(regexp_extract(lower(df("MEDICATION_NAME_med")), "([0-9]*\\.*[0-9]+)\\s*((m*c*g|ml|meq|milligram))", 2)))
    }),
    "FILLNUM" -> mapFrom("NUMBER_OF_REFILLS_PRESCRIBED"),
    "QUANTITYPERFILL" -> mapFrom("PRESCRIPTION_FILL_QUANTITY"),
    "SIGNATURE" -> mapFrom("SIG"),
    "VENUE" -> literal("1"),
    "ORDERSTATUS" -> mapFrom("STATUS")
  )

  afterMap = (df: DataFrame) => {
    val athena_start_dt = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENTMEDICATION_PM", "RXORDER", "PATIENTMEDICATION", "ISSUEDATE")
    df.withColumn("LOCALTOTALDOSE", df("LOCALSTRENGTHPERDOSEUNIT").multiply(df("LOCALQTYOFDOSEUNIT")))
      .withColumn("ATH_START_DT", to_date(when(lit("'NO_MPV_MATCHES'").isin(athena_start_dt: _*), lit("19010101")).otherwise(lit(athena_start_dt.head))))
      .filter("patientid is not null and issuedate >= ath_start_dt")
  }

}