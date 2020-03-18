package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedProcPatientMedication (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  columns=List("FILEID", "APPROVED_BY", "APPROVED_DATETIME", "CLINICAL_ENCOUNTER_ID", "CLINICAL_ORDER_TYPE",
    "CLINICAL_ORDER_TYPE_GROUP", "CLINICAL_PROVIDER_ID", "DENIED_DATETIME", "DENIEDBY", "DOCUMENT_CLASS",
    "DOCUMENT_DESCRIPTION", "DOCUMENT_SUBCLASS", "IMAGE_EXISTS_YN", "NDC", "PROVIDER_USERNAME", "RXNORM",
    "STATUS", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "PATIENT_MEDICATION_ID", "MEDICATION_TYPE",
    "PATIENT_ID", "CHART_ID", "DOCUMENT_ID", "MEDICATION_ID", "FROM_PATIENT_MEDICATION_ID", "SIG", "MEDICATION_NAME",
    "DOSAGE_FORM", "DOSAGE_ACTION", "DOSAGE_STRENGTH", "DOSAGE_STRENGTH_UNITS", "DOSAGE_QUANTITY", "DISPLAY_DOSAGE_UNITS",
    "DOSAGE_ROUTE", "FREQUENCY", "LENGTH_OF_COURSE", "PRESCRIPTION_FILL_QUANTITY", "NUMBER_OF_REFILLS_PRESCRIBED",
    "DEACTIVATION_REASON", "SOURCE_CODE", "SOURCE_CODE_TYPE", "FILL_DATE", "PHARMACY_NAME", "MED_ADMINISTERED_DATETIME",
    "CREATED_DATETIME", "CREATED_BY", "START_DATE", "STOP_DATE", "DEACTIVATION_DATETIME", "DEACTIVATED_BY",
    "DELETED_DATETIME", "DELETED_BY", "NOTE", "PRESCRIBER", "PRESCRIBER_NPI", "PRESCRIBED_YN", "ADMINISTERED_YN",
    "DISPENSED_YN", "ADMINISTERED_EXPIRATION_DATE", "DISPENSED_EXPIRATION_DATE", "PROC_DATE")

  tables = List("patientmedication", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "patientmedication" -> List("FILEID", "APPROVED_BY", "APPROVED_DATETIME", "CLINICAL_ENCOUNTER_ID",
      "CLINICAL_ORDER_TYPE", "CLINICAL_ORDER_TYPE_GROUP", "CLINICAL_PROVIDER_ID", "DENIED_DATETIME", "DENIEDBY",
      "DOCUMENT_CLASS", "DOCUMENT_DESCRIPTION", "DOCUMENT_SUBCLASS", "IMAGE_EXISTS_YN", "NDC", "PROVIDER_USERNAME",
      "RXNORM", "STATUS", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "PATIENT_MEDICATION_ID",
      "MEDICATION_TYPE", "PATIENT_ID", "CHART_ID", "DOCUMENT_ID", "MEDICATION_ID", "FROM_PATIENT_MEDICATION_ID", "SIG",
      "MEDICATION_NAME", "DOSAGE_FORM", "DOSAGE_ACTION", "DOSAGE_STRENGTH", "DOSAGE_STRENGTH_UNITS", "DOSAGE_QUANTITY",
      "DISPLAY_DOSAGE_UNITS", "DOSAGE_ROUTE", "FREQUENCY", "LENGTH_OF_COURSE", "PRESCRIPTION_FILL_QUANTITY",
      "NUMBER_OF_REFILLS_PRESCRIBED", "DEACTIVATION_REASON", "SOURCE_CODE", "SOURCE_CODE_TYPE", "FILL_DATE",
      "PHARMACY_NAME", "MED_ADMINISTERED_DATETIME", "CREATED_DATETIME", "CREATED_BY", "START_DATE", "STOP_DATE",
      "DEACTIVATION_DATETIME", "DEACTIVATED_BY", "DELETED_DATETIME", "DELETED_BY", "NOTE", "PRESCRIBER",
      "PRESCRIBER_NPI", "PRESCRIBED_YN", "ADMINISTERED_YN", "DISPENSED_YN", "ADMINISTERED_EXPIRATION_DATE",
      "DISPENSED_EXPIRATION_DATE"),
    "fileExtractDates" -> List("FILEID","FILEDATE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("patientmedication")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val dfOut = df.withColumn("PROC_DATE", expr("from_unixtime(unix_timestamp(NOTE,\"MM/dd/yyyy\"))").cast("Date"))
    val groups = Window.partitionBy(dfOut("PATIENT_ID"), dfOut("CLINICAL_ENCOUNTER_ID"), dfOut("PROC_DATE"), dfOut("MEDICATION_TYPE"))
      .orderBy(dfOut("FILEDATE").desc_nulls_last, dfOut("FILEID").desc_nulls_last)
    dfOut.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1 and DELETED_DATETIME is null")
  }


}

// test
//  val a = new UtilDedupedProcPatientMedication(cfg); val o = build(a); o.count

