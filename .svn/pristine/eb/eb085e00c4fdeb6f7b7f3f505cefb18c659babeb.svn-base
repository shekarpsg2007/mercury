package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedClaim (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "CLAIM_ID", "ORIGINAL_CLAIM_ID",
    "PATIENT_ID", "CLAIM_PRIMARY_PATIENT_INS_ID", "CLAIM_SECONDARY_PATIENT_INS_ID", "CLAIM_SCHEDULING_PROVIDER_ID",
    "RENDERING_PROVIDER_ID", "SUPERVISING_PROVIDER_ID", "CLAIM_APPOINTMENT_ID", "PATIENT_DEPARTMENT_ID",
    "SERVICE_DEPARTMENT_ID", "CLAIM_REFERRING_PROVIDER_ID", "CLAIM_REFERRAL_AUTH_ID", "PATIENT_ROUNDING_LIST_ID",
    "PRIMARY_CLAIM_STATUS", "SECONDARY_CLAIM_STATUS", "PATIENT_CLAIM_STATUS", "CLAIM_CREATED_DATETIME", "CREATED_BY",
    "CLAIM_SERVICE_DATE", "HOSPITALIZATION_FROM_DATE", "HOSPITALIZATION_TO_DATE", "RESERVED19")

  tables = List("claim",
    "fileExtractDates:athena.util.UtilFileIdDates",
    "pat:athena.util.UtilSplitPatient")

  columnSelect = Map(
    "claim" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "CLAIM_ID", "ORIGINAL_CLAIM_ID",
      "PATIENT_ID", "CLAIM_PRIMARY_PATIENT_INS_ID", "CLAIM_SECONDARY_PATIENT_INS_ID", "CLAIM_SCHEDULING_PROVIDER_ID",
      "RENDERING_PROVIDER_ID", "SUPERVISING_PROVIDER_ID", "CLAIM_APPOINTMENT_ID", "PATIENT_DEPARTMENT_ID",
      "SERVICE_DEPARTMENT_ID", "CLAIM_REFERRING_PROVIDER_ID", "CLAIM_REFERRAL_AUTH_ID", "PATIENT_ROUNDING_LIST_ID",
      "PRIMARY_CLAIM_STATUS", "SECONDARY_CLAIM_STATUS", "PATIENT_CLAIM_STATUS", "CLAIM_CREATED_DATETIME", "CREATED_BY",
      "CLAIM_SERVICE_DATE", "HOSPITALIZATION_FROM_DATE", "HOSPITALIZATION_TO_DATE", "RESERVED19"),
    "fileExtractDates" -> List("FILEID","FILEDATE"),
    "pat" -> List("PATIENT_ID")
  )


  join = (dfs: Map[String, DataFrame]) => {
    val patJoinType = new UtilSplitTable(config).patprovJoinType
    dfs("claim")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
      .join(dfs("pat"), Seq("PATIENT_ID"), patJoinType)
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("CLAIM_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row=1")
  }


}

// test
//  val a = new UtilDedupedClaim(cfg); val o = build(a); o.count

