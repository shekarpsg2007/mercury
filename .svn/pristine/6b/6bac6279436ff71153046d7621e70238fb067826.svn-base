package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame

class UtilDedupedPatientSocialHistory (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "SOCIAL_HISTORY_ID", "HUM_TYPE",
    "PATIENT_ID", "CHART_ID", "SOCIAL_HISTORY_KEY", "SOCIAL_HISTORY_NAME", "SOCIAL_HISTORY_ANSWER", "CREATED_DATETIME",
    "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "FD_FILEDATE")

  tables = List("patientsocialhistory", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "patientsocialhistory" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "SOCIAL_HISTORY_ID",
      "HUM_TYPE", "PATIENT_ID", "CHART_ID", "SOCIAL_HISTORY_KEY", "SOCIAL_HISTORY_NAME", "SOCIAL_HISTORY_ANSWER",
      "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY")
    ,"fileExtractDates" -> List("FILEID","FILEDATE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("patientsocialhistory")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "inner")
  }


  afterJoin = (df: DataFrame) => {
    df.withColumnRenamed("FILEDATE", "FD_FILEDATE")
  }


}

// test
//  val a = new UtilDedupedPatientSocialHistory(cfg); val o = build(a); o.count

