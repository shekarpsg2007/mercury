package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedProvider (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "PROVIDER_ID", "SCHEDULING_NAME",
    "REPORTING_NAME", "BILLED_NAME", "PROVIDER_FIRST_NAME", "PROVIDER_LAST_NAME", "PROVIDER_USER_NAME", "PROVIDER_TYPE",
    "PROVIDER_TYPE_NAME", "PROVIDER_TYPE_CATEGORY", "PROVIDER_NPI_NUMBER", "PROVIDER_NDC_TAT_NUMBER", "PROVIDER_GROUP_ID",
    "SUPERVISING_PROVIDER_ID", "TAXONOMY", "SPECIALTY_CODE", "SPECIALTY", "ENTITY_TYPE", "SCHEDULE_RESOURCE_TYPE",
    "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "PROVIDER_MIDDLE_INITIAL", "PROVIDER_MEDICAL_GROUP_ID",
    "TAXONOMY_ANSI_CODE", "BILLABLE_YN", "PATIENT_FACING_NAME", "HIDE_IN_PORTAL_YN", "SIGNATURE_ON_FILE_YN",
    "TRACK_MISSING_SLIPS_YN", "REFERRING_PROVIDER_YN", "CREATE_ENCOUNTER_YN", "STAFF_BUCKET_YN", "COMMUNICATOR_HOME_DEPT_ID", "DIRECT_ADDRESS")

  tables = List("provider","fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "provider" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "PROVIDER_ID", "SCHEDULING_NAME",
      "REPORTING_NAME", "BILLED_NAME", "PROVIDER_FIRST_NAME", "PROVIDER_LAST_NAME", "PROVIDER_USER_NAME", "PROVIDER_TYPE",
      "PROVIDER_TYPE_NAME", "PROVIDER_TYPE_CATEGORY", "PROVIDER_NPI_NUMBER", "PROVIDER_NDC_TAT_NUMBER", "PROVIDER_GROUP_ID",
      "SUPERVISING_PROVIDER_ID", "TAXONOMY", "SPECIALTY_CODE", "SPECIALTY", "ENTITY_TYPE", "SCHEDULE_RESOURCE_TYPE",
      "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "PROVIDER_MIDDLE_INITIAL",
      "PROVIDER_MEDICAL_GROUP_ID", "TAXONOMY_ANSI_CODE", "BILLABLE_YN", "PATIENT_FACING_NAME", "HIDE_IN_PORTAL_YN",
      "SIGNATURE_ON_FILE_YN", "TRACK_MISSING_SLIPS_YN", "REFERRING_PROVIDER_YN", "CREATE_ENCOUNTER_YN", "STAFF_BUCKET_YN",
      "COMMUNICATOR_HOME_DEPT_ID", "DIRECT_ADDRESS")
    ,"fileExtractDates" -> List("FILEID", "FILEDATE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("provider")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PROVIDER_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1")
  }


}

// test
//  val a = new UtilDedupedProvider(cfg); val o = build(a); o.count

