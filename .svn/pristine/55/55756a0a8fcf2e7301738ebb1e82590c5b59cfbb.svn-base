package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedPatientAssertion(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns = List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "PATIENT_ASSERTION_ID", "HUM_TYPE",
    "PATIENT_ID", "CHART_ID", "CLINICAL_ENCOUNTER_ID", "PATIENT_ASSERTION_KEY", "PATIENT_ASSERTION_KEY_ID",
    "PATIENT_ASSERTION_VALUE", "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "OBSDATE")

  tables = List("patientassertion", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "patientassertion" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "PATIENT_ASSERTION_ID",
      "HUM_TYPE", "PATIENT_ID", "CHART_ID", "CLINICAL_ENCOUNTER_ID", "PATIENT_ASSERTION_KEY", "PATIENT_ASSERTION_KEY_ID",
      "PATIENT_ASSERTION_VALUE", "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY"),
    "fileExtractDates" -> List("FILEID", "FILEDATE")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patientassertion")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {

    val obsdf = df.withColumn("CR_DATE", expr("from_unixtime(unix_timestamp(substring(CREATED_DATETIME,1,10),\"yyyy-MM-dd\"))").cast("Date"))
      .withColumn("OBSDATE", expr(" CASE when CR_DATE >= FILEDATE or FILEDATE > current_date then CR_DATE ELSE coalesce(FILEDATE, CR_DATE) END"))
    val groups = Window.partitionBy(obsdf("OBSDATE"), obsdf("PATIENT_ASSERTION_VALUE"), obsdf("PATIENT_ASSERTION_ID"), obsdf("HUM_TYPE")).orderBy(obsdf("FILEID").desc_nulls_last)
    obsdf.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1")
  }


}


// test
//  val a = new UtilDedupedPatientAssertion(cfg); val o = build(a); o.count

