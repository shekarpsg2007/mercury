package com.humedica.mercury.etl.asent.diagnosis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._


class DiagnosisEncounters(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_problem_de", "as_problems", "as_encounters", "as_charges", "cdr.map_predicate_values")

  columnSelect = Map(
    "as_zc_problem_de" -> List("ICD9DIAGNOSISCODE", "ICD10DIAGNOSISCODE", "ID"),
    "as_problems" -> List("PROBLEM_STATUS_ID", "PROBLEM_CATEGORY_ID", "PROBLEM_RESOLVED_DATE", "CURRENT_PROBLEM_ID", "ICD9DIAGNOSISCODE", "ICD10DIAGNOSISCODE", "LAST_UPDATED_DATE"),
    "as_encounters" -> List("ENCOUNTER_DATE_TIME", "APPOINTMENT_LOCATION_ID", "APPOINTMENT_STATUS_ID", "PROVIDER_ID", "ENCOUNTER_ID", "PROBLEM_DE", "PROBLEMCATEGORYDE", "PROBLEM_ID", "LAST_UPDATED_DATE", "PATIENT_MRN"),
    "as_charges" -> List("ENCOUNTER_ID", "BILLING_STATUS")
  )

  beforeJoin = Map(
    "as_encounters" -> ((df: DataFrame) => {
      val fil = df.repartition(1000)
        .filter("PATIENT_MRN IS NOT NULL AND ENCOUNTER_DATE_TIME IS NOT NULL AND PROBLEMCATEGORYDE IN ('0', '9', '10', '19', '20', '21', '22', '23', '28', '36', '37', '38', '42', '43', '55', '67', '5', '7', '40', '30', '25', '39')")
        .withColumnRenamed("ENCOUNTER_ID", "ENCOUNTER_ID_en")
        .withColumnRenamed("LAST_UPDATED_DATE", "LAST_UPDATED_DATE_en")
        .select("ENCOUNTER_ID_en", "APPOINTMENT_LOCATION_ID", "PATIENT_MRN", "ENCOUNTER_DATE_TIME", "PROVIDER_ID", "LAST_UPDATED_DATE_en", "PROBLEM_DE", "PROBLEM_ID", "APPOINTMENT_STATUS_ID")
        .distinct()
      val groups = Window.partitionBy(fil("ENCOUNTER_ID_en"), fil("PROBLEM_DE")).orderBy(fil("LAST_UPDATED_DATE_en").desc_nulls_last)
      fil.withColumn("RN_AE", row_number.over(groups))
    }),
    "as_problems" -> ((df: DataFrame) => {
      val prob_stat_excp = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROBLEMS", "DIAGNOSIS", "AS_PROBLEMS", "PROBLEM_STATUS_ID")
      //val H908filter = if(config(GROUP) == lit("H908583")) lit(" OR PROBLEM_RESOLVED_DATE IS NOT NULL ") else lit(" and 1=1 ")
      val unpivoted = df.select(df("CURRENT_PROBLEM_ID"), df("LAST_UPDATED_DATE"), df("PROBLEM_STATUS_ID"), df("PROBLEM_CATEGORY_ID"), expr("stack(2, ICD9DIAGNOSISCODE, 'ICD9', ICD10DIAGNOSISCODE, 'ICD10') as (COL_VAL, COL_TYPE)"))
      val fil = unpivoted.filter("COL_VAL is not null")
      val groups = Window.partitionBy(fil("CURRENT_PROBLEM_ID"), fil("COL_TYPE")).orderBy(fil("LAST_UPDATED_DATE").desc_nulls_last)
      fil.withColumn("rwp", row_number.over(groups))
        .filter("rwp=1 and NOT (PROBLEM_STATUS_ID NOT IN  (" + prob_stat_excp + ") AND PROBLEM_CATEGORY_ID IN ('0','9','10','19','20','21','22','23','28','36','37','38','42','43','55','67', '5','7','40','30','25','39'))")
        .select("CURRENT_PROBLEM_ID", "PROBLEM_STATUS_ID")
        .distinct()
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_encounters")
      .join(dfs("as_charges"), dfs("as_encounters")("ENCOUNTER_ID_en") === dfs("as_charges")("ENCOUNTER_ID"), "left_outer")
      .join(dfs("as_zc_problem_de"), dfs("as_zc_problem_de")("ID") === dfs("as_encounters")("PROBLEM_DE"), "left_outer")
      .join(dfs("as_problems"), dfs("as_problems")("CURRENT_PROBLEM_ID") === dfs("as_encounters")("PROBLEM_ID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val fil = df.filter("RN_AE=1 and CURRENT_PROBLEM_ID IS NULL AND (APPOINTMENT_STATUS_ID = '2' OR (APPOINTMENT_STATUS_ID IS NULL AND BILLING_STATUS NOT IN ('R','C')))")
    fil.select(fil("ENCOUNTER_ID_en"), fil("APPOINTMENT_LOCATION_ID"), fil("PATIENT_MRN"), fil("ENCOUNTER_DATE_TIME"),
      fil("PROVIDER_ID"), fil("PROBLEM_STATUS_ID"), fil("LAST_UPDATED_DATE_en"), expr("stack(2, ICD9DIAGNOSISCODE, 'ICD9', ICD10DIAGNOSISCODE, 'ICD10') as (CODE_VALUE,CODETYPE)"))

  }

  map = Map(
    "DATASRC" -> literal("encounters"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID_en"),
    "FACILITYID" -> mapFrom("APPOINTMENT_LOCATION_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "DX_TIMESTAMP" -> mapFromDate("ENCOUNTER_DATE_TIME"),
    "LOCALDIAGNOSIS" -> mapFrom("CODE_VALUE"),
    "MAPPEDDIAGNOSIS" -> mapFrom("CODE_VALUE"),
    "LOCALDIAGNOSISPROVIDERID" -> mapFrom("PROVIDER_ID"),
    "LOCALACTIVEIND" -> mapFrom("PROBLEM_STATUS_ID")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("DX_TIMESTAMP"), df("ENCOUNTERID"), df("LOCALDIAGNOSIS")).orderBy(df("LAST_UPDATED_DATE_en").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw = 1 and DX_TIMESTAMP is not null AND LOCALDIAGNOSIS IS NOT NULL")
  }

}