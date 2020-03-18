package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedClinicalEncounter (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "LAST_MODIFIED_DATETIME", "LAST_MODIFIED_BY", "CONTEXT_ID", "CONTEXT_NAME",
    "CONTEXT_PARENTCONTEXTID", "CLINICAL_ENCOUNTER_ID", "PATIENT_ID", "CHART_ID", "APPOINTMENT_ID",
    "APPOINTMENT_TICKLER_ID", "DEPARTMENT_ID", "CLAIM_ID", "CLINICAL_ENCOUNTERTYPE", "PROVIDER_ID",
    "SUPERVISING_PROVIDER_ID", "ENCOUNTER_DATE", "ENCOUNTER_STATUS", "CREATED_DATETIME", "CREATED_BY", "ASSIGNED_TO",
    "CLOSED_DATETIME", "CLOSED_BY", "DELETED_DATETIME", "DELETED_BY", "PATIENT_STATUS", "PATIENT_LOCATION", "SPECIALTY",
    "BILLING_TAB_REVIEWED")

  tables = List("clinicalencounter",
    "fileExtractDates:athena.util.UtilFileIdDates",
    "pat:athena.util.UtilSplitPatient")

  columnSelect = Map(
    "clinicalencounter" -> List("FILEID", "LAST_MODIFIED_DATETIME", "LAST_MODIFIED_BY", "CONTEXT_ID", "CONTEXT_NAME",
      "CONTEXT_PARENTCONTEXTID", "CLINICAL_ENCOUNTER_ID", "PATIENT_ID", "CHART_ID", "APPOINTMENT_ID",
      "APPOINTMENT_TICKLER_ID", "DEPARTMENT_ID", "CLAIM_ID", "CLINICAL_ENCOUNTERTYPE", "PROVIDER_ID",
      "SUPERVISING_PROVIDER_ID", "ENCOUNTER_DATE", "ENCOUNTER_STATUS", "CREATED_DATETIME", "CREATED_BY", "ASSIGNED_TO",
      "CLOSED_DATETIME", "CLOSED_BY", "DELETED_DATETIME", "DELETED_BY", "PATIENT_STATUS", "PATIENT_LOCATION",
      "SPECIALTY", "BILLING_TAB_REVIEWED"),
    "fileExtractDates" -> List("FILEID","FILEDATE"),
    "pat" -> List("PATIENT_ID")
  )


  join = (dfs: Map[String, DataFrame]) => {
    val patJoinType = new UtilSplitTable(config).patprovJoinType
    dfs("clinicalencounter")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
      .join(dfs("pat"), Seq("PATIENT_ID"), patJoinType)
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("CLINICAL_ENCOUNTER_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1 and DELETED_DATETIME is null")
  }


}

// test
//  val a = new UtilDedupedClinicalEncounter(cfg); val o = build(a); o.count

