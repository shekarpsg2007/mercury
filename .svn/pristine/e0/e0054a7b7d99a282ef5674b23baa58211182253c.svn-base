package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedDepartment (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "DEPARTMENT_ID",
    "CHART_SHARING_GROUP_ID", "DEPARTMENT_NAME", "BILLING_NAME", "DEPARTMENT_ADDRESS", "DEPARTMENT_ADDRESS_2",
    "DEPARTMENT_CITY", "DEPARTMENT_STATE", "DEPARTMENT_ZIP", "DEPARTMENT_PHONE", "DEPARTMENT_FAX",
    "DEPARTMENT_MEDICAL_PROVIDER_ID", "GPCI_LOCATION_ID", "GPCI_LOCATION_NAME", "PLACE_OF_SERVICE_CODE",
    "PLACE_OF_SERVICE_TYPE", "SPECIALTY_CODE", "DEPARTMENT_SPECIALTY", "PROVIDER_GROUP_ID", "DEPARTMENT_GROUP",
    "TYPE_OF_BILL_CODE", "TYPE_OF_BILL", "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "CAMPUS_LOCATION")

  tables = List("department", "fileExtractDates:athena.util.UtilFileIdDates")

  columnSelect = Map(
    "department" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "DEPARTMENT_ID",
      "CHART_SHARING_GROUP_ID", "DEPARTMENT_NAME", "BILLING_NAME", "DEPARTMENT_ADDRESS", "DEPARTMENT_ADDRESS_2",
      "DEPARTMENT_CITY", "DEPARTMENT_STATE", "DEPARTMENT_ZIP", "DEPARTMENT_PHONE", "DEPARTMENT_FAX",
      "DEPARTMENT_MEDICAL_PROVIDER_ID", "GPCI_LOCATION_ID", "GPCI_LOCATION_NAME", "PLACE_OF_SERVICE_CODE",
      "PLACE_OF_SERVICE_TYPE", "SPECIALTY_CODE", "DEPARTMENT_SPECIALTY", "PROVIDER_GROUP_ID", "DEPARTMENT_GROUP",
      "TYPE_OF_BILL_CODE", "TYPE_OF_BILL", "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "CAMPUS_LOCATION")
    ,"fileExtractDates" -> List("FILEID", "FILEDATE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("department")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("DEPARTMENT_ID")).orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row = 1 and DEPARTMENT_ID is not null")
  }


}

// test
//  val a = new UtilDedupedDepartment(cfg); val o = build(a); o.count

