package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedTransactionClaim (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "ADJUSTED_TOTAL_RVU", "ADJUSTED_WORK_RVU", "CHARGE_CREATED_DATE", "CLAIM_APPOINTMENT_ID",
    "CLAIM_CREATED_DATE", "CLAIM_PATIENT_ID", "CLAIM_PRIMARY_PATIENT_INS_ID", "CLAIM_REFERRAL_AUTH_ID",
    "CLAIM_REFERRING_PROVIDER_ID", "CLAIM_SCHEDULING_PROVIDER_ID", "CLAIM_SECONDARY_PATIENT_INS_ID", "CLAIM_SERVICE_DATE",
    "HOSPITALIZATION_FROM_DATE", "HOSPITALIZATION_TO_DATE", "ORIGINAL_CLAIM_ID", "PATIENT_CLAIM_STATUS",
    "PATIENT_DEPARTMENT_ID", "PATIENT_ROUNDING_LIST_ID", "PRIMARY_CLAIM_STATUS", "RENDERING_PROVIDER_ID", "RESERVED19",
    "SECONDARY_CLAIM_STATUS", "SERVICE_DEPARTMENT_ID", "SUPERVISING_PROVIDER_ID", "TRANSFER_TYPE_STATUS", "CONTEXT_ID",
    "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "TRANSACTION_ID", "CHARGE_ID", "PARENT_CHARGE_ID", "CUSTOM_TRANSACTION_CODE",
    "TRANSACTION_POSTED_BY", "TRANSACTION_CREATED_DATETIME", "CHARGE_FROM_DATE", "CHARGE_TO_DATE", "FIRST_BILLED_DATETIME",
    "LAST_BILLED_DATETIME", "TRANSACTION_TYPE", "TRANSACTION_TRANSFER_TYPE", "TRANSACTION_TRANSFER_INTENT",
    "TRANSACTION_REASON", "APPOINTMENT_ID", "TRANSACTION_PATIENT_INS_ID", "PROCEDURE_CODE", "OTHER_MODIFIER",
    "CLOSED_POST_DATE", "VOIDED_DATE", "CHARGE_VOID_PARENT_ID", "CLAIM_ID", "TRANSACTION_METHOD", "PLACE_OF_SERVICE",
    "PROVIDER_GROUP_ID", "ORIG_POSTED_PAYMENT_BATCH_ID", "PAYMENT_BATCH_ID", "VOID_PAYMENT_BATCH_ID", "EXPECTED_ALLOWED_AMOUNT",
    "EXPECTED_ALLOWABLE_SCHEDULE_ID", "REVERSAL_FLAG", "PATIENT_PAYMENT_ID", "POST_DATE", "WORK_RVU", "PRACTICE_EXPENSE_RVU",
    "MALPRACTICE_RVU", "TOTAL_RVU", "AMOUNT", "NUMBER_OF_CHARGES", "UNITS", "EMGYN", "PATIENT_ID")

  tables = List("transaction", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "transaction" -> List("FILEID", "ADJUSTED_TOTAL_RVU", "ADJUSTED_WORK_RVU", "CHARGE_CREATED_DATE",
      "CLAIM_APPOINTMENT_ID", "CLAIM_CREATED_DATE", "CLAIM_PATIENT_ID", "CLAIM_PRIMARY_PATIENT_INS_ID",
      "CLAIM_REFERRAL_AUTH_ID", "CLAIM_REFERRING_PROVIDER_ID", "CLAIM_SCHEDULING_PROVIDER_ID",
      "CLAIM_SECONDARY_PATIENT_INS_ID", "CLAIM_SERVICE_DATE", "HOSPITALIZATION_FROM_DATE", "HOSPITALIZATION_TO_DATE",
      "ORIGINAL_CLAIM_ID", "PATIENT_CLAIM_STATUS", "PATIENT_DEPARTMENT_ID", "PATIENT_ROUNDING_LIST_ID",
      "PRIMARY_CLAIM_STATUS", "RENDERING_PROVIDER_ID", "RESERVED19", "SECONDARY_CLAIM_STATUS", "SERVICE_DEPARTMENT_ID",
      "SUPERVISING_PROVIDER_ID", "TRANSFER_TYPE_STATUS", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID",
      "TRANSACTION_ID", "CHARGE_ID", "PARENT_CHARGE_ID", "CUSTOM_TRANSACTION_CODE", "TRANSACTION_POSTED_BY",
      "TRANSACTION_CREATED_DATETIME", "CHARGE_FROM_DATE", "CHARGE_TO_DATE", "FIRST_BILLED_DATETIME", "LAST_BILLED_DATETIME",
      "TRANSACTION_TYPE", "TRANSACTION_TRANSFER_TYPE", "TRANSACTION_TRANSFER_INTENT", "TRANSACTION_REASON", "APPOINTMENT_ID",
      "TRANSACTION_PATIENT_INS_ID", "PROCEDURE_CODE", "OTHER_MODIFIER", "CLOSED_POST_DATE", "VOIDED_DATE",
      "CHARGE_VOID_PARENT_ID", "CLAIM_ID", "TRANSACTION_METHOD", "PLACE_OF_SERVICE", "PROVIDER_GROUP_ID",
      "ORIG_POSTED_PAYMENT_BATCH_ID", "PAYMENT_BATCH_ID", "VOID_PAYMENT_BATCH_ID", "EXPECTED_ALLOWED_AMOUNT",
      "EXPECTED_ALLOWABLE_SCHEDULE_ID", "REVERSAL_FLAG", "PATIENT_PAYMENT_ID", "POST_DATE", "WORK_RVU",
      "PRACTICE_EXPENSE_RVU", "MALPRACTICE_RVU", "TOTAL_RVU", "AMOUNT", "NUMBER_OF_CHARGES", "UNITS", "EMGYN", "PATIENT_ID"),
    "fileExtractDates" -> List("FILEID", "FILEDATE")
  )

  beforeJoin = Map(
    "transaction"-> includeIf("TRANSACTION_TYPE = 'CHARGE'")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("transaction")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val groupsCharge = Window.partitionBy(df("CHARGE_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    val ChargeDF = df.withColumn("chg_rw", row_number.over(groupsCharge))
      .filter("chg_rw = 1 and voided_date is null")
      .drop("chg_rw")

    val groupsClaim = Window.partitionBy(ChargeDF("CLAIM_ID")).orderBy(ChargeDF("FILEID").desc_nulls_last)

    ChargeDF.withColumn("clm_rw", row_number.over(groupsClaim))
      .filter("clm_rw = 1 and voided_date is null")
      .drop("clm_rw")

  }


}

// test
//  val a = new UtilDedupedTransactionClaim(cfg); val o = build(a); o.count

