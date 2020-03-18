package com.humedica.mercury.etl.crossix.membercoverage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource

class MembercoverageMembercoverage(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("member_coverage")

  columns = List("ASO", "BUSINESS_LINE_CODE","CUSTOMER_SEGMENT_NBR","D_CUST_DRIVEN_HEALTH_PLAN_CODE",
    "D_ZIPCODE_3","EFF_DATE","END_DATE","GENDER","INDUSTRY_PRODUCT_CODE","LSRD_INDIVIDUAL_ID","LSRD_MEMBER_SYSTEM_ID",
    "STATE_CODE","YEAR_OF_BIRTH")

  columnSelect = Map(
    "member_coverage" -> List("FUNDING_PHI_IND", "BUSINESS_LINE_CODE", "CUSTOMER_SEGMENT_NBR", "D_CUST_DRIVEN_HEALTH_PLAN_CODE",
      "D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "INDUSTRY_PRODUCT_CODE", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
      "STATE_CODE", "YEAR_OF_BIRTH")
  )

  map = Map(
    "ASO" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("FUNDING_PHI_IND") isin ("3", "4"), "Y").otherwise("N"))
    }),
    "LSRD_MEMBER_SYSTEM_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("LSRD_MEMBER_SYSTEM_ID"), 256)))
  )
}

//  build(new MembercoverageMembercoverage(cfg), allColumns=true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/MEMBER_COVERAGE_201710")