package com.humedica.mercury.etl.crossix.labresultshipaa

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class LabresulthipaaLabresulthipaav2(config: Map[String,String], count:Int) extends EntitySource(config: Map[String,String]) {

  tables = List("labresulthipaa.lab_result_hipaa"+count, "provider")

  columns = List("ANALYTE_SEQUENCE_NBR", "DATA_SOURCE_CODE","HI_NORMAL_VALUE_NBR","LOINC_CODE",
    "LOW_NORMAL_VALUE_NBR","LSRD_INDIVIDUAL_ID","LSRD_LAB_CLAIM_HEADER_SYSTEM_ID","LSRD_MEMBER_SYSTEM_ID","PROC_CODE",
    "PROVIDER_NPI","RESULT_ABNORMAL_CODE", "RESULT_UNITS_NAME","RESULT_VALUE_NBR", "RESULT_VALUE_TEXT",
    "SERVICE_FROM_DATE", "VENDOR_TEST_DESC", "VENDOR_TEST_NBR")

  columnSelect = Map(
    "labresulthipaa.lab_result_hipaa"+count -> List("ANALYTE_SEQUENCE_NBR", "DATA_SOURCE_CODE","HI_NORMAL_VALUE_NBR","LOINC_CODE",
      "LOW_NORMAL_VALUE_NBR","LSRD_INDIVIDUAL_ID","LSRD_LAB_CLAIM_HEADER_SYS_ID","LSRD_MEMBER_SYSTEM_ID","PROC_CODE",
      "RESULT_ABNORMAL_CODE","RESULT_UNITS_NAME", "RESULT_VALUE_NBR","RESULT_VALUE_TEXT", "SERVICE_FROM_DATE",
      "VENDOR_TEST_DESC", "VENDOR_TEST_NBR", "PROVIDER_KEY"),
    "provider" -> List("NPI_NBR", "PROVIDER_KEY")
  )

  join = (dfs: Map[String, DataFrame]) => {
    println("FILE ===> labresulthipaa.lab_result_hipaa"+count)

    var provider = dfs("provider")
    var lab = dfs("labresulthipaa.lab_result_hipaa"+count)
    lab.join(provider, Seq("PROVIDER_KEY"), "left_outer")
  }

  map = Map(
    "LSRD_LAB_CLAIM_HEADER_SYSTEM_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("LSRD_LAB_CLAIM_HEADER_SYS_ID"), 256))),
    "LSRD_MEMBER_SYSTEM_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("LSRD_MEMBER_SYSTEM_ID"), 256))),
    "PROVIDER_NPI" -> mapFrom("NPI_NBR")
  )
}

//  build(new LabresulthipaaLabresulthipaa(cfg, 9), allColumns=true).distinct.write.parquet(cfg.get("LABRESULTHIPAA_DATA_ROOT").get+"/LAB_RESULT_HIPAA_201803_P9")



// sqlContext.read.parquet(cfg.get("LABRESULTHIPAA_DATA_ROOT").get+"/LAB_RESULT_HIPAA_201803_P9").write.mode("append").parquet(cfg.get("EMR_DATA_ROOT").get+"/LAB_RESULT_HIPAA_201803")

