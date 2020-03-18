package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedClinicalResult (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "CLINICAL_RESULT_ID", "DOCUMENT_ID",
    "ORDER_DOCUMENT_ID", "CLINICAL_PROVIDER_ID", "RESULT_DOCUMENT_ID", "SPECIMEN_SOURCE", "CLINICAL_ORDER_TYPE",
    "CLINICAL_ORDER_TYPE_GROUP", "FBD_MED_ID", "CLINICAL_PROVIDER_ORDER_TYPE", "CLINICAL_ORDER_GENUS", "CVX",
    "PROVIDER_NOTE", "EXTERNAL_NOTE", "RESULT_STATUS", "REPORT_STATUS", "CREATED_DATETIME", "CREATED_BY",
    "OBSERVATION_DATETIME", "SPECIMEN_RECEIVED_DATETIME", "RESULTS_REPORTED_DATETIME", "DELETED_DATETIME", "DELETED_BY")

  tables = List("clinicalresult", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "clinicalresult" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "CLINICAL_RESULT_ID",
      "DOCUMENT_ID", "ORDER_DOCUMENT_ID", "CLINICAL_PROVIDER_ID", "RESULT_DOCUMENT_ID", "SPECIMEN_SOURCE",
      "CLINICAL_ORDER_TYPE", "CLINICAL_ORDER_TYPE_GROUP", "FBD_MED_ID", "CLINICAL_PROVIDER_ORDER_TYPE",
      "CLINICAL_ORDER_GENUS", "CVX", "PROVIDER_NOTE", "EXTERNAL_NOTE", "RESULT_STATUS", "REPORT_STATUS",
      "CREATED_DATETIME", "CREATED_BY", "OBSERVATION_DATETIME", "SPECIMEN_RECEIVED_DATETIME", "RESULTS_REPORTED_DATETIME",
      "DELETED_DATETIME", "DELETED_BY"),
    "fileExtractDates" -> List("FILEID","FILEDATE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinicalresult")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("CLINICAL_RESULT_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1 and DELETED_DATETIME is null and " +
        "(lower(RESULT_STATUS) not in ('canceled','cancelled','incomplete','partial','pending','preliminary') or RESULT_STATUS is null)")
  }


}

// test
//  val a = new UtilDedupedClinicalResult(cfg); val o = build(a); o.count

