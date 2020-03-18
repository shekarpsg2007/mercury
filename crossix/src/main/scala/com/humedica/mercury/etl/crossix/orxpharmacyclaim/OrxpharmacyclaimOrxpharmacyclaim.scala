package com.humedica.mercury.etl.crossix.orxpharmacyclaim

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.crossix.util.CrossixETLUtility._


/**
  * Created by rbabu on 6/27/17.
  */
class OrxpharmacyclaimOrxpharmacyclaim(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

    tables = List("claim", "ndc", "pharmacy", "carriersblc2v2.carriers_blc2_v2")

    columns = List("AHFS_THERAPEUTIC_CLASS", "AMT_AVERAGE_WHOLESALE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_DISPENSING_FEE",
        "BRAND_NAME", "CLAIM_SEQUENCE_NBR", "CLAIM_STATUS", "CODE", "COUNT_DAYS_SUPPLY", "DAW_CODE", "DRUG_PRICE_TYPE_CODE", "DRUG_STRENGTH_DESC", "FILL_DATE",
        "FIRST_FILL_IND", "FORMULARY_IND", "FORMULARY_TYPE_CODE", "GENERIC_IND", "LSRD_NABP_NBR", "MAIL_ORDER_IND",
        "ORX_CLAIM_NBR", "ORX_INDIVIDUAL_ID", "PAID_DATE", "PHRM_NPI_NBR", "PRESCRIPTION_NBR", "PRSC_NPI_NBR", "QUANTITY_DRUG_UNITS",
        "REFILL_NBR", "SPECIALTY_PHMCY", "SPECIFIC_THERAPEUTIC_CLASS")

    columnSelect = Map(
        "claim" -> List("CLIENTFLAG", "AWPUNITCST", "CTYPEUCOST", "RSPCOPAY", "CLTATRDED", "CLTDISPFEE", "CLAIMSTS",
            "PRODUCTID", "PSC", "DTEFILLED", "REFILL", "FORMLRFLAG", "PLANQUAL", "RXCLAIMNBR", "ORGPDSBMDT", "NPIPROV", "NPIPRESCR",
            "DECIMALQTY", "CLMSEQNBR", "CARRIERID", "GROUPID", "MEMBERID", "DAYSSUPPLY", "ACCOUNTID", "RXNUMBER"),
        "ndc" -> List("CODE", "AHFS_THERAPEUTIC_CLASS", "BRAND_NAME", "DRUG_PRICE_TYPE_CODE", "DRUG_STRENGTH_DESC", "GENERIC", "SPECIFIC_THERAPEUTIC_CLASS", "EFF_DATE", "END_DATE"),
        "pharmacy" -> List("LSRD_NABP_NBR", "DISPENSE_TYPE_CODE", "NC_SPECIALTY_PHMCY", "NABP_NBR"),
        "carriersblc2v2.carriers_blc2_v2" -> List("CARRIER_ID", "BUSINESS_LINE_CODE", "OPTYPE")
    )

    beforeJoin = Map(
        "claim" -> renameColumns(List(("GROUPID", "GRPID")))
    )

    join = (dfs: Map[String, DataFrame]) => {
        var claim = dfs("claim")
        var ndc = dfs("ndc")
        var carriers_blc2_v2 = dfs("carriersblc2v2.carriers_blc2_v2")
        carriers_blc2_v2=carriers_blc2_v2.filter(carriers_blc2_v2("BUSINESS_LINE_CODE")==="COM")

        claim.join(carriers_blc2_v2,
            ((carriers_blc2_v2("OPTYPE")==="0" && claim("CARRIERID") === carriers_blc2_v2("CARRIER_ID")).or
            (carriers_blc2_v2("OPTYPE")==="1" && claim("CARRIERID").contains(carriers_blc2_v2("CARRIER_ID")))), "inner")
                .join(ndc, (claim("PRODUCTID") === ndc("CODE")).and(unix_timestamp(trim(claim("DTEFILLED")), "yyyyMMdd").between(unix_timestamp(trim(ndc("EFF_DATE")), "yyyy-MM-dd"), unix_timestamp( trim(ndc("END_DATE")), "yyyy-MM-dd"))), "left_outer")
                .join(dfs("pharmacy"), claim("NPIPROV") === dfs("pharmacy")("NABP_NBR"), "left_outer")
    }

    map = Map(
        "AMT_AVERAGE_WHOLESALE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("CLIENTFLAG") isin("P", "S"), when(df("AWPUNITCST").isNotNull, df("AWPUNITCST")).otherwise("0.00")).otherwise(
                when(df("CLIENTFLAG") isin("Y", "M"), when(df("CTYPEUCOST").isNotNull, df("CTYPEUCOST")).otherwise("0.00")).otherwise(when(df("AWPUNITCST").isNotNull, df("AWPUNITCST")).otherwise("0.00"))))
        }),
        "AMT_COPAY" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("RSPCOPAY").isNull, "0.00").otherwise(df("RSPCOPAY")))
        }),
        "AMT_DEDUCTIBLE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("CLTATRDED").isNull, "0.00").otherwise(df("CLTATRDED")))
        }),
        "AMT_DISPENSING_FEE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("CLTDISPFEE").isNull, "0.00").otherwise(df("CLTDISPFEE")))
        }),
        "CLAIM_SEQUENCE_NBR" -> mapFrom("CLMSEQNBR"),
        "CLAIM_STATUS" -> mapFrom("CLAIMSTS"),
        "CODE" -> mapFrom("PRODUCTID"),
        "COUNT_DAYS_SUPPLY" -> mapFrom("DAYSSUPPLY"),
        "DAW_CODE" -> mapFrom("PSC"),
        "FILL_DATE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, parseORXCrossixDateStr1("DTEFILLED", "yyyyMMdd", df))
        }),
        "FIRST_FILL_IND" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("REFILL") === "00", "Y").otherwise("N"))
        }),
        "FORMULARY_IND" -> mapFrom("FORMLRFLAG"),
        "FORMULARY_TYPE_CODE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("PLANQUAL") === "3", "7").otherwise(
                when(df("PLANQUAL") === "2", "3").otherwise(
                    when(df("PLANQUAL") === "1", "1").otherwise(
                        when(df("PLANQUAL") === "6", "E").otherwise(
                            when(df("PLANQUAL") === "13", "D").otherwise(
                                when(df("PLANQUAL") === "4", "8").otherwise("0")))))))
        }),
        "MAIL_ORDER_IND" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("DISPENSE_TYPE_CODE").isNull, df("DISPENSE_TYPE_CODE")).otherwise(when(df("DISPENSE_TYPE_CODE") === "5", "Y").otherwise("N")))
        }),
        "ORX_CLAIM_NBR" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("RXCLAIMNBR"), 256))),
        "PAID_DATE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, parseORXCrossixDateStr1("ORGPDSBMDT", "yyyyMMdd", df))
        }),
        "PHRM_NPI_NBR" -> mapFrom("NPIPROV"),
        "PRSC_NPI_NBR" -> mapFrom("NPIPRESCR"),
        "QUANTITY_DRUG_UNITS" -> mapFrom("DECIMALQTY"),
        "REFILL_NBR" -> ((col: String, df: DataFrame) => parseORXRefill(col, df)),
        "GENERIC_IND" -> mapFrom("GENERIC"),
        "SPECIALTY_PHMCY" -> mapFrom("NC_SPECIALTY_PHMCY"),
        "ORX_INDIVIDUAL_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(concat(df("CARRIERID"), df("ACCOUNTID"), df("GRPID"), df("MEMBERID")), 256))),
        "LSRD_NABP_NBR" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("LSRD_NABP_NBR").isNotNull, df("LSRD_NABP_NBR")).otherwise(
                when(df("NABP_NBR").isNotNull, sha2(df("NABP_NBR"), 256)).otherwise(df("NABP_NBR"))))
        }),
        "PRESCRIPTION_NBR" -> mapFrom("RXNUMBER")
    )
}


//   build(new OrxpharmacyclaimOrxpharmacyclaim(cfg), allColumns=true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/ORX_PHARMACY_CLAIM")

