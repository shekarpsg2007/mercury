package com.humedica.mercury.etl.asent.diagnosis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._


class DiagnosisCharges(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_charges", "as_zc_icd9_diagnosis_de")

  columnSelect = Map(
    "as_charges" -> List("BILLING_LOCATION_ID", "ENCOUNTER_DATE_TIME", "LINKED_ICD9_ID", "PATIENT_MRN",
      "CHARGE_ID", "ENCOUNTER_ID", "ICD9DIAGNOSISCODE", "ICD10DIAGNOSISCODE", "ICD10_CODE", "LAST_UPDATED_DATE"),
    "as_zc_icd9_diagnosis_de" -> List("ENTRYCODE", "ID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_charges")
      .join(dfs("as_zc_icd9_diagnosis_de"), dfs("as_zc_icd9_diagnosis_de")("ID") === dfs("as_charges")("LINKED_ICD9_ID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val addColumn = df.withColumn("ICD9", coalesce(when(df("LINKED_ICD9_ID") =!= lit("0"), df("LINKED_ICD9_ID")).otherwise(null), df("ICD9DIAGNOSISCODE")))
    val pivcodetype = unpivot(
      Seq("ICD10_CODE", "ICD9", "ICD10DIAGNOSISCODE"),
      Seq("ICD10", "ICD9", "ICD10_dx"), typeColumnName = "ICD_CODE_TYPE")

    val pivcode = pivcodetype("ICD_CODE_VALUE", addColumn)

    pivcode.repartition(1000).withColumn("LOCALDIAGNOSIS", pivcode("ICD_CODE_VALUE"))
      .withColumn("CODETYPE",
        when(pivcode("ICD_CODE_TYPE") === lit("ICD10_dx"), lit("ICD10")).otherwise(pivcode("ICD_CODE_TYPE")))
      .withColumn("MAPPEDDIAGNOSIS",
        when(pivcode("ICD_CODE_TYPE") === lit("ICD9"), coalesce(pivcode("ENTRYCODE"), pivcode("ICD_CODE_VALUE")))
          .otherwise(pivcode("ICD_CODE_VALUE")))
  }


  map = Map(
    "DATASRC" -> literal("charges"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "DX_TIMESTAMP" -> mapFrom("ENCOUNTER_DATE_TIME")
  )


  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALDIAGNOSIS"), df("DX_TIMESTAMP")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and DX_TIMESTAMP is not null and LOCALDIAGNOSIS is not null and MAPPEDDIAGNOSIS is not null")
  }
}