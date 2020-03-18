package com.humedica.mercury.etl.crossix.pharmacyclaim

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._


class PharmacyclaimPharmacyclaim(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "pharmacy_claim",
    "ndc",
    "pharmacy",
    "pharmacy_claim_std_cost"
  )

  columns = List("AHFS_THERAPEUTIC_CLASS", "AMT_AVERAGE_WHOLESALE","AMT_COPAY","AMT_DEDUCTIBLE",
    "AMT_DISPENSING_FEE","AMT_STANDARD_COST","BRAND_NAME","CODE","COUNT_DAYS_SUPPLY",
    "DAW_CODE","DRUG_PRICE_TYPE_CODE","DRUG_STRENGTH_DESC","FILL_DATE","FIRST_FILL_IND"
    ,"FORMULARY_IND","FORMULARY_TYPE_CODE","GENERIC_IND","LSRD_CLAIM_NBR","LSRD_INDIVIDUAL_ID","LSRD_MEMBER_SYSTEM_ID"
    ,"LSRD_PRESCRIPTION_NBR","MAIL_ORDER_IND","NHI_NABP_NBR","PAID_DATE","PHRM_NPI_NBR","PRSC_NPI_NBR","QUANTITY_DRUG_UNITS",
    "REFILL_NBR","SPECIALTY_PHMCY","SPECIFIC_THERAPEUTIC_CLASS", "STANDARD_COST_YEAR")

  columnSelect = Map(
    "pharmacy_claim" -> List("AMT_AVERAGE_WHOLESALE","AMT_COPAY","AMT_DEDUCTIBLE","AMT_DISPENSING_FEE","COUNT_DAYS_SUPPLY","DAW_CODE",
      "FILL_DATE","FIRST_FILL_IND","FORMULARY_IND","FORMULARY_TYPE_CODE","LSRD_CLAIM_NBR","LSRD_INDIVIDUAL_ID","LSRD_MEMBER_SYSTEM_ID",
      "LSRD_PRESCRIPTION_NBR","MAIL_ORDER_IND","PAID_DATE","PHRM_NPI_NBR","PRSC_NPI_NBR","QUANTITY_DRUG_UNITS","REFILL_NBR",
      "NDC_KEY", "PHARMACY_KEY","COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "LSRD_PRESCRIPTION_NBR", "MEMBER_COVERAGE_KEY"),
    "ndc" -> List("NDC_KEY", "AHFS_THERAPEUTIC_CLASS","BRAND_NAME","CODE","DRUG_PRICE_TYPE_CODE","DRUG_STRENGTH_DESC","GENERIC_NBR","SPECIFIC_THERAPEUTIC_CLASS"),
    "pharmacy" -> List("PHARMACY_KEY", "NABP_NBR","NC_SPECIALTY_PHMCY"),
    "pharmacy_claim_std_cost" -> List("FILL_DATE","COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "LSRD_PRESCRIPTION_NBR", "MEMBER_COVERAGE_KEY", "AMT_STANDARD_COST", "STANDARD_COST_YEAR")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("pharmacy_claim")
            .join(dfs("ndc"), Seq("NDC_KEY"), "left_outer")
            .join(dfs("pharmacy"), Seq("PHARMACY_KEY"), "left_outer")
            .join(dfs("pharmacy_claim_std_cost"), Seq("FILL_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "LSRD_PRESCRIPTION_NBR", "MEMBER_COVERAGE_KEY"), "left_outer")
  }

  map = Map(
    "NHI_NABP_NBR" -> mapFrom("NABP_NBR"),
    "GENERIC_IND" -> mapFrom("GENERIC_NBR"),

    "SPECIALTY_PHMCY" -> mapFrom("NC_SPECIALTY_PHMCY"),
    "LSRD_MEMBER_SYSTEM_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("LSRD_MEMBER_SYSTEM_ID"), 256)))

  )

}
