package com.humedica.mercury.etl.athena.patient

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.athena.util.UtilSplitTable

/**
  * Auto-generated on 09/21/2018
  */


class PatientPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient:athena.util.UtilDedupedPatient",
    "cdr.map_predicate_values"
  )

  columnSelect = Map(
    "patient" -> List("PATIENT_ID", "TEST_PATIENT_YN", "ENTERPRISE_ID", "DOB", "DECEASED_DATE", "NEW_PATIENT_ID",
      "PATIENT_STATUS")
  )

  beforeJoin = Map(
    "patient" -> excludeIf("coalesce(test_patient_yn ,'X') = 'Y'")
  )

  map = Map(
    "DATASRC" -> literal("patient"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "MEDICALRECORDNUMBER" -> mapFrom("ENTERPRISE_ID"),
    "DATEOFBIRTH" -> mapFrom("DOB"),
    "DATEOFDEATH" -> mapFrom("DECEASED_DATE"),
    "INACTIVE_FLAG" -> ((col: String, df: DataFrame) => {
      val pat_inact_flag = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PATIENT", "INACTIVATE_PT_STATUS_D")
      df.withColumn(col,
        when(lower(df("PATIENT_STATUS")) === lit("i"), "Y")
          .when(lit(pat_inact_flag.head) === "Y" &&
            lower(df("PATIENT_STATUS")) === "d" &&
            coalesce(df("NEW_PATIENT_ID"), df("PATIENT_ID")) != df("PATIENT_ID"), lit("Y"))
          .otherwise(null)
      )
    })
  )

}
