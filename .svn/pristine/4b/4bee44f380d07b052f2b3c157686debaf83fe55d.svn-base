package com.humedica.mercury.etl.athena.patientidentifier

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.athena.util.UtilSplitTable


/**
  * Auto-generated on 09/21/2018
  */


class PatientidentifierPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient",
    "temp_patient:athena.util.UtilSplitPatient",
    "temp_fileid_dates:athena.util.UtilFileIdDates"
  )

  columnSelect = Map(
    "patient" -> List("PATIENT_ID", "TEST_PATIENT_YN", "PATIENT_SSN", "FILEID"),
    "temp_patient" -> List("PATIENT_ID"),
    "temp_fileid_dates" -> List("FILEID", "FILEDATE")
  )

  beforeJoin = Map(
    "patient" -> excludeIf("coalesce(test_patient_YN ,'X') = 'Y'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    val splitjointype = new UtilSplitTable(config).patprovJoinType
    dfs("patient")
      .join(dfs("temp_patient"), Seq("PATIENT_ID"), splitjointype)
      .join(dfs("temp_fileid_dates"), Seq("FILEID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENT_ID"), df("PATIENT_SSN"))
      .orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("Ssn_Row", row_number.over(groups))
      .filter("Ssn_Row = 1")
  }

  map = Map(
    "DATASRC" -> literal("patient"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "IDTYPE" -> literal("SSN"),
    "IDVALUE" -> standardizeSSN("PATIENT_SSN")
  )

  afterMap = includeIf("IDVALUE is not null")

}