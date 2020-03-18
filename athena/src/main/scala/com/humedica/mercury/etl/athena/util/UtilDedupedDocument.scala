package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedDocument (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "DOCUMENT_ID", "PATIENT_ID",
    "CHART_ID", "DEPARTMENT_ID", "CLINICAL_ENCOUNTER_ID", "CREATED_CLINICAL_ENCOUNTER_ID", "ORDER_DOCUMENT_ID",
    "CLINICAL_PROVIDER_ID", "DOCUMENT_CLASS", "DOCUMENT_SUBCLASS", "DOCUMENT_PRECLASS", "DOCUMENT_CATEGORY",
    "DOCUMENT_SUBJECT", "DELEGATE_SIGNED_OFF_BY", "NEEDS_PROVIDER_DELEGATE_ACK_YN", "CLINICAL_ORDER_TYPE",
    "CLINICAL_ORDER_TYPE_GROUP", "FBD_MED_ID", "CLINICAL_PROVIDER_ORDER_TYPE", "CLINICAL_ORDER_GENUS", "STATUS",
    "PROVIDER_USERNAME", "CVX", "VACCINE_ROUTE", "PRIORITY", "ASSIGNED_TO", "NOTIFIER", "SOURCE", "ROUTE",
    "SPECIMEN_DESCRIPTION", "SPECIMEN_COLLECTED_BY", "SPECIMEN_COLLECTED_DATETIME", "SPECIMEN_DRAW_LOCATION",
    "SPECIMEN_SOURCE", "ALARM_DAYS", "ALARM_DATE", "IMAGE_EXISTS_YN", "ORDER_DATETIME", "CREATED_DATETIME",
    "CREATED_BY", "RECEIVED_DATETIME", "REVIEWED_BY", "REVIEWED_DATETIME", "OBSERVATION_DATETIME",
    "DEACTIVATED_DATETIME", "DEACTIVATED_BY", "FUTURE_SUBMIT_DATETIME", "DENIED_DATETIME", "DENIED_BY",
    "APPROVED_DATETIME", "APPROVED_BY", "DELETED_DATETIME", "DELETED_BY", "ORDER_TEXT", "PROVIDER_NOTE", "PATIENT_NOTE",
    "EXTERNAL_NOTE", "RESULT_NOTES", "INTERFACE_VENDOR_NAME", "OUT_OF_NETWORK_REF_REASON_NAME")

  tables = List("document","fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "document" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "DOCUMENT_ID", "PATIENT_ID",
      "CHART_ID", "DEPARTMENT_ID", "CLINICAL_ENCOUNTER_ID", "CREATED_CLINICAL_ENCOUNTER_ID", "ORDER_DOCUMENT_ID",
      "CLINICAL_PROVIDER_ID", "DOCUMENT_CLASS", "DOCUMENT_SUBCLASS", "DOCUMENT_PRECLASS", "DOCUMENT_CATEGORY",
      "DOCUMENT_SUBJECT", "DELEGATE_SIGNED_OFF_BY", "NEEDS_PROVIDER_DELEGATE_ACK_YN", "CLINICAL_ORDER_TYPE",
      "CLINICAL_ORDER_TYPE_GROUP", "FBD_MED_ID", "CLINICAL_PROVIDER_ORDER_TYPE", "CLINICAL_ORDER_GENUS", "STATUS",
      "PROVIDER_USERNAME", "CVX", "VACCINE_ROUTE", "PRIORITY", "ASSIGNED_TO", "NOTIFIER", "SOURCE", "ROUTE",
      "SPECIMEN_DESCRIPTION", "SPECIMEN_COLLECTED_BY", "SPECIMEN_COLLECTED_DATETIME", "SPECIMEN_DRAW_LOCATION",
      "SPECIMEN_SOURCE", "ALARM_DAYS", "ALARM_DATE", "IMAGE_EXISTS_YN", "ORDER_DATETIME", "CREATED_DATETIME",
      "CREATED_BY", "RECEIVED_DATETIME", "REVIEWED_BY", "REVIEWED_DATETIME", "OBSERVATION_DATETIME",
      "DEACTIVATED_DATETIME", "DEACTIVATED_BY", "FUTURE_SUBMIT_DATETIME", "DENIED_DATETIME", "DENIED_BY",
      "APPROVED_DATETIME", "APPROVED_BY", "DELETED_DATETIME", "DELETED_BY", "ORDER_TEXT", "PROVIDER_NOTE",
      "PATIENT_NOTE", "EXTERNAL_NOTE", "RESULT_NOTES", "INTERFACE_VENDOR_NAME", "OUT_OF_NETWORK_REF_REASON_NAME"),
    "fileExtractDates" -> List("FILEID","FILEDATE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("document")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("DOCUMENT_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1 and DELETED_DATETIME is null")
  }


}

// test
//  val a = new UtilDedupedDocument(cfg); val o = build(a); o.count

