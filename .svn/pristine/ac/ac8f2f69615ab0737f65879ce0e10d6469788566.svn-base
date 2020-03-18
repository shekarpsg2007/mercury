package com.humedica.mercury.etl.crossix.orxmembercoverage

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.crossix.util.CrossixETLUtility._
/**
  * Created by rbabu on 6/27/17.
  */
class OrxmembercoverageOrxmembercoverage (config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

        tables = List("member", "member_eligibility_history", "zip.zip", "carriersblc2v2.carriers_blc2_v2")

        columns = List("BUSINESS_LINE_CODE", "CUSTOMER_SEGMENT_NBR", "D_ZIPCODE_3", "EFF_DATE", "END_DATE",
            "GENDER", "INDUSTRY_PRODUCT_CODE", "ORX_INDIVIDUAL_ID", "STATE_CODE", "YEAR_OF_BIRTH")

        columnSelect = Map(
            "member" -> List("ABADCD", "ABAAST", "ABAACD", "ABACCD", "ABADCD", "ABABCD","ABAZTX", "ABAJDT", "ABA0TX"),
            "member_eligibility_history" -> List("FLCPDA", "FLCQDA", "FLAACD", "FLACCD", "FLADCD", "FLABCD"),
            "zip.zip" -> List("ZIP3", "ZIP3_COLLAPSE_CODE"),
            "carriersblc2v2.carriers_blc2_v2" -> List("CARRIER_ID", "BUSINESS_LINE_CODE", "BOOK", "OPTYPE")
        )

    join = (dfs: Map[String, DataFrame]) => {
        var member_eligibility_history = dfs("member_eligibility_history").groupBy("FLAACD", "FLACCD", "FLADCD", "FLABCD").agg(max("FLCPDA").as("FLCPDA"), max("FLCQDA").as("FLCQDA"))
        var member = dfs("member")
        var carriers_blc2_v2 = dfs("carriersblc2v2.carriers_blc2_v2")
        member
                .join(carriers_blc2_v2,
                    ((carriers_blc2_v2("OPTYPE")==="0" && member("ABAACD") === carriers_blc2_v2("CARRIER_ID")).or
                            (carriers_blc2_v2("OPTYPE")==="1" && member("ABAACD").contains(carriers_blc2_v2("CARRIER_ID")))), "left_outer")
                .join(dfs("zip.zip"), substring(member("ABA0TX"), 1, 3) ===  dfs("zip.zip")("ZIP3"), "left_outer")
                .join(member_eligibility_history,
                    member("ABAACD") ===  member_eligibility_history("FLAACD") &&
                member("ABADCD") ===  member_eligibility_history("FLADCD") &&
                member("ABACCD") ===  member_eligibility_history("FLACCD") &&
                member("ABABCD") ===  member_eligibility_history("FLABCD"), "inner")
    }

    map = Map(
        "BUSINESS_LINE_CODE"  -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("BUSINESS_LINE_CODE").isNull, null).otherwise(df("BUSINESS_LINE_CODE")))}),
        "CUSTOMER_SEGMENT_NBR" -> mapFrom("FLADCD"),
        "D_ZIPCODE_3" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("ZIP3_COLLAPSE_CODE").isNull, "000").otherwise(df("ZIP3_COLLAPSE_CODE")))}),
        "EFF_DATE" ->  ((col: String, df: DataFrame) => {df.withColumn(col, parseORXCrossixDateStr("FLCPDA", df))}),
        "END_DATE" -> ((col: String, df: DataFrame) => {df.withColumn(col, parseORXCrossixDateStr("FLCQDA", df))}),
        "GENDER" -> ((col: String, df: DataFrame) => parseORXGender(col, df)),
        "INDUSTRY_PRODUCT_CODE" -> literal("UNK"),
        "ORX_INDIVIDUAL_ID" -> ((col: String, df: DataFrame) =>{
            df.withColumn(col,  sha2(concat(df("ABAACD"), df("ABACCD") ,df("ABADCD"), df("ABABCD")), 256)
        )}),
        "STATE_CODE" -> ((col: String, df: DataFrame) => parseORXCrossixStageCode(col, df)),
        "YEAR_OF_BIRTH"  -> ((col: String, df: DataFrame) => parseORXCrossixYearOfBirth(col, df))
    )

}


//  build(new OrxmembercoverageOrxmembercoverage(cfg), allColumns=true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/ORX_MEMBER_COVERAGE")


