package com.humedica.mercury.etl.athena.claim

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ClaimTransaction(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("transaction:athena.util.UtilDedupedTransaction",
    "clinicalencounter:athena.util.UtilDedupedClinicalEncounter",
    "feescheduleprocedure",
    "claim:athena.util.UtilDedupedClaim")

  columnSelect = Map(
    "transaction" -> List("CLAIM_ID", "CLAIM_PATIENT_ID", "CLAIM_SERVICE_DATE", "PROCEDURE_CODE", "PLACE_OF_SERVICE",
      "UNITS", "CHARGE_ID", "AMOUNT", "RENDERING_PROVIDER_ID", "CHARGE_TO_DATE", "CLAIM_APPOINTMENT_ID",
      "SERVICE_DEPARTMENT_ID", "SUPERVISING_PROVIDER_ID"),
    "clinicalencounter" -> List("CLINICAL_ENCOUNTER_ID", "CLAIM_ID", "APPOINTMENT_ID"),
    "feescheduleprocedure" -> List("PROCEDURE_CODE", "REVENUE_CODE"),
    "claim" -> List("PATIENT_ID", "SERVICE_DEPARTMENT_ID", "SUPERVISING_PROVIDER_ID", "CLAIM_SERVICE_DATE",
      "RENDERING_PROVIDER_ID", "CLAIM_ID", "CLAIM_APPOINTMENT_ID")
  )

  beforeJoin = Map(
    "transaction" -> renameColumns(List(
      ("SUPERVISING_PROVIDER_ID", "SUPERVISING_PROVIDER_ID_t"),
      ("SERVICE_DEPARTMENT_ID", "SERVICE_DEPARTMENT_ID_t"),
      ("RENDERING_PROVIDER_ID", "RENDERING_PROVIDER_ID_t"),
      ("CLAIM_SERVICE_DATE", "CLAIM_SERVICE_DATE_t"),
      ("CLAIM_APPOINTMENT_ID", "CLAIM_APPOINTMENT_ID_t"))
    ),
    "feescheduleprocedure" -> ((df: DataFrame) => {
      df.filter("revenue_code is not null")
        .groupBy("PROCEDURE_CODE")
        .agg(max("REVENUE_CODE").as("REVENUE_CODE"))
    }),
    "claim" -> renameColumns(List(
      ("SUPERVISING_PROVIDER_ID", "SUPERVISING_PROVIDER_ID_c"),
      ("SERVICE_DEPARTMENT_ID", "SERVICE_DEPARTMENT_ID_c"),
      ("RENDERING_PROVIDER_ID", "RENDERING_PROVIDER_ID_c"),
      ("CLAIM_SERVICE_DATE", "CLAIM_SERVICE_DATE_c"),
      ("CLAIM_APPOINTMENT_ID", "CLAIM_APPOINTMENT_ID_c"))
    )
  )

  join = (dfs: Map[String, DataFrame]) => {
    val ce2 = table("clinicalencounter")
        .withColumnRenamed("CLINICAL_ENCOUNTER_ID", "CLINICAL_ENCOUNTER_ID_2")
        .withColumnRenamed("CLAIM_ID", "CLAIM_ID_2")
        .withColumnRenamed("APPOINTMENT_ID", "APPOINTMENT_ID_2")

    dfs("transaction")
      .join(dfs("claim"), Seq("CLAIM_ID"), "left_outer")
      .join(dfs("feescheduleprocedure"), Seq("PROCEDURE_CODE"), "left_outer")
      .join(dfs("clinicalencounter"), Seq("CLAIM_ID"), "left_outer")
      .join(ce2, ce2("APPOINTMENT_ID_2") === coalesce(dfs("claim")("CLAIM_APPOINTMENT_ID_c"), dfs("transaction")("CLAIM_APPOINTMENT_ID_t")), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("transaction"),
    "CLAIMID" -> mapFrom("CLAIM_ID"),
    "PATIENTID" -> cascadeFrom(Seq("PATIENT_ID", "CLAIM_PATIENT_ID")),
    "SERVICEDATE" -> cascadeFrom(Seq("CLAIM_SERVICE_DATE_t", "CLAIM_SERVICE_DATE_c")),
    "CHARGE" -> mapFrom("AMOUNT"),
    "CLAIMPROVIDERID" -> cascadeFrom(Seq("RENDERING_PROVIDER_ID_t", "RENDERING_PROVIDER_ID_c")),
    "LOCALCPT" -> mapFrom("PROCEDURE_CODE"),
    "MAPPEDCPT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("PROCEDURE_CODE"), 1, 5))
    }),
    "MAPPEDCPTMOD1" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("PROCEDURE_CODE")) >= 7, substring(df("PROCEDURE_CODE"), 7, 2)).otherwise(null))
    }),
    "MAPPEDCPTMOD2" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("PROCEDURE_CODE")) >= 10, substring(df("PROCEDURE_CODE"), 10, 2)).otherwise(null))
    }),
    "FACILITYID" -> cascadeFrom(Seq("SERVICE_DEPARTMENT_ID_t", "SERVICE_DEPARTMENT_ID_c")),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        df("CLINICAL_ENCOUNTER_ID"),
        df("CLINICAL_ENCOUNTER_ID_2"),
        when(df("CLAIM_APPOINTMENT_ID_t").isNotNull, concat(lit("a."), df("CLAIM_APPOINTMENT_ID_t")))
        )
      )
    }),
    "LOCALREV" -> mapFrom("REVENUE_CODE"),
    "MAPPEDREV" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("REVENUE_CODE")) <= 4, df("REVENUE_CODE")).otherwise(null))
    }),
    "LOCALBILLINGPROVIDERID" -> cascadeFrom(Seq("SUPERVISING_PROVIDER_ID_c", "SUPERVISING_PROVIDER_ID_t")),
    "POS" -> mapFrom("PLACE_OF_SERVICE"),
    "QUANTITY" -> mapFrom("UNITS"),
    "SEQ" -> mapFrom("CHARGE_ID"),
    "TO_DT" -> mapFrom("CHARGE_TO_DATE")
  )

  afterMap = includeIf("servicedate is not null")

}