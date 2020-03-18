package com.humedica.mercury.etl.athena.patientcontact

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Auto-generated on 09/21/2018
  */


class PatientcontactPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient:athena.util.UtilDedupedPatient")

  columnSelect = Map(
    "patient" -> List("PATIENT_ID", "TEST_PATIENT_YN", "REGISTRATION_DATE", "PATIENT_LAST_SEEN_DATE", "MOBILE_PHONE",
      "PATIENT_HOME_PHONE", "WORK_PHONE", "EMAIL", "FILEDATE")
  )

  beforeJoin = Map(
    "patient" -> excludeIf("coalesce(test_patient_YN ,'X') = 'Y'")
  )

  map = Map(
    "DATASRC" -> literal("patient"),
    "UPDATE_DT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("FILEDATE"), df("PATIENT_LAST_SEEN_DATE"), df("REGISTRATION_DATE"), current_timestamp()))
    }),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "CELL_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("MOBILE_PHONE"), 1, 50))
    }),
    "HOME_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("PATIENT_HOME_PHONE"), 1, 50))
    }),
    "PERSONAL_EMAIL" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("EMAIL"), 1, 200))
    }),
    "WORK_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("WORK_PHONE"), 1, 50))
    })
  )

  afterMap = includeIf("update_dt is not null and coalesce(cell_phone, home_phone, personal_email, work_phone) is not null")

}