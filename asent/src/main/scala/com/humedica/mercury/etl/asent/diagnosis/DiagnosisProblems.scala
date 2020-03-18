package com.humedica.mercury.etl.asent.diagnosis


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._


class DiagnosisProblems(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_problems", "cdr.map_predicate_values")

  columnSelect = Map(
    "as_problems" -> List("PROBLEM_PARENT_CATEGORY", "PROBLEM_RESOLVED_DATE", "PROBLEM_ONSET_DATE", "PATIENT_MRN",
      "PROBLEM_STATUS_ID", "PROBLEM_CATEGORY_ID", "ENCOUNTER_ID",
      "PROBLEM_RESOLVED_DATE", "ICD10DIAGNOSISCODE", "LAST_UPDATED_DATE", "RECORDED_DATE", "ICD9DIAGNOSISCODE", "ICD9_CODE", "CURRENT_PROBLEM_ID")
  )

  beforeJoin = Map(
    "as_problems" -> ((df1: DataFrame) => {
      val prob_stat_excp = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROBLEMS", "DIAGNOSIS", "AS_PROBLEMS", "PROBLEM_STATUS_ID")
      val df = df1.repartition(1000)
        .filter("PROBLEM_STATUS_ID NOT IN  (" + prob_stat_excp + ") AND (PROBLEM_CATEGORY_ID IS NULL OR PROBLEM_CATEGORY_ID IN ('0','9','10','19','20','21','22','23','28','36','37','38','42','43','55','67', '5','7','40','30','25','39'))")
      val pivcodetype = unpivot(
        Seq("ICD9_CODE", "ICD9DIAGNOSISCODE", "ICD10DIAGNOSISCODE"),
        Seq("ICD9", "ICD9", "ICD10"), typeColumnName = "ICD_CODE_TYPE")
      val pivcode = pivcodetype("ICD_CODE_VALUE", df)

      val pivdxdate = unpivot(
        Seq("PROBLEM_ONSET_DATE", "LAST_UPDATED_DATE", "RECORDED_DATE"),
        Seq("PROBLEM_ONSET_DATE", "LAST_UPDATED_DATE", "RECORDED_DATE"), typeColumnName = "DT_TYPE")

      val pivdx = pivdxdate("DX_DATE", pivcode)
      val groups = Window.partitionBy(pivdx("CURRENT_PROBLEM_ID"), pivdx("ICD_CODE_TYPE"), pivdx("DX_DATE")).orderBy(pivdx("LAST_UPDATED_DATE").desc_nulls_last)
      pivdx.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
    })
  )


  map = Map(
    "DATASRC" -> literal("problems"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "LOCALDIAGNOSIS" -> mapFrom("ICD_CODE_VALUE"),
    "PRIMARYDIAGNOSIS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROBLEM_CATEGORY_ID") === 20, 1).otherwise(null))
    }),
    "MAPPEDDIAGNOSIS" -> mapFrom("ICD_CODE_VALUE"),
    "LOCALDIAGNOSISSTATUS" -> mapFrom("PROBLEM_CATEGORY_ID"),
    "LOCALACTIVEIND" -> mapFrom("PROBLEM_STATUS_ID"),
    "RESOLUTIONDATE" -> mapFromDate("PROBLEM_RESOLVED_DATE"),
    "LOCALADMITFLG" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROBLEM_CATEGORY_ID") === 19, df("PROBLEM_CATEGORY_ID")).otherwise(null))
    }),
    "LOCALDISCHARGEFLG" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROBLEM_CATEGORY_ID") === 22, df("PROBLEM_CATEGORY_ID")).otherwise(null))
    }),
    "DX_TIMESTAMP" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DX_DATE").contains("1900-01-01"), null).otherwise(df("DX_DATE")))
    }),
    "CODETYPE" -> mapFrom("ICD_CODE_TYPE")

  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("DX_TIMESTAMP"), df("ENCOUNTERID"), df("LOCALDIAGNOSIS")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last, df("RESOLUTIONDATE").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 AND DX_TIMESTAMP IS NOT NULL AND PATIENTID IS NOT NULL")
  }
}