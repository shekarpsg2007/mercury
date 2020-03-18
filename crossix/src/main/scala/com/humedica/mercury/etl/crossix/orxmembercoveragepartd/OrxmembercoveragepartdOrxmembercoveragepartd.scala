package com.humedica.mercury.etl.crossix.orxmembercoveragepartd

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.crossix.util.CrossixETLUtility._
/**
  * Created by rbabu on 6/27/17.
  */
class OrxmembercoveragepartdOrxmembercoveragepartd  (config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

    tables = List("member", "mbr_mcr_partd_history", "zip.zip")

    columns = List("D_ZIPCODE_3", "EFF_DATE",  "END_DATE", "GENDER", "ORX_INDIVIDUAL_ID", "STATE_CODE", "YEAR_OF_BIRTH")

    columnSelect = Map(
        "member" -> List("ABAAST", "ABAACD", "ABACCD", "ABADCD", "ABABCD", "ABAZTX", "ABAJDT", "ABA0TX"),
        "mbr_mcr_partd_history" -> List("A0OCHG", "A0ODHG" , "A0AACD", "A0ACCD", "A0ADCD", "A0ABCD"),
        "zip.zip" -> List("ZIP3", "ZIP3_COLLAPSE_CODE")
    )
    join = (dfs: Map[String, DataFrame]) => {
        var mbr_mcr_partd_history = dfs("mbr_mcr_partd_history").groupBy("A0AACD", "A0ACCD", "A0ADCD", "A0ABCD").agg(max("A0OCHG").as("A0OCHG"),max("A0ODHG").as("A0ODHG"))
        dfs("member")
                .join(dfs("zip.zip"), substring(dfs("member")("ABA0TX"), 1, 3)  ===  dfs("zip.zip")("ZIP3"), "left_outer")
                .join(mbr_mcr_partd_history,
                            dfs("member")("ABAACD") ===  mbr_mcr_partd_history("A0AACD") &&
                            dfs("member")("ABACCD") ===  mbr_mcr_partd_history("A0ACCD") &&
                            dfs("member")("ABADCD") ===  mbr_mcr_partd_history("A0ADCD") &&
                            dfs("member")("ABABCD") ===  mbr_mcr_partd_history("A0ABCD"), "inner")
    }

    map = Map(
        "D_ZIPCODE_3" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("ZIP3_COLLAPSE_CODE").isNull, "000").otherwise(df("ZIP3_COLLAPSE_CODE")))}),
        "EFF_DATE" -> ((col: String, df: DataFrame) => {df.withColumn(col, parseORXCrossixDateStr("A0OCHG", df))}),
        "END_DATE" -> ((col: String, df: DataFrame) => {df.withColumn(col, parseORXCrossixDateStr("A0ODHG", df))}),
        "GENDER" -> ((col: String, df: DataFrame) => parseORXGender(col, df)),
        "ORX_INDIVIDUAL_ID" -> ((col: String, df: DataFrame) =>{
            df.withColumn(col, sha2(concat(df("ABAACD"), df("ABACCD") , df("ABADCD"),  df("ABABCD")), 256)
            )}),
        "STATE_CODE" -> ((col: String, df: DataFrame) => parseORXCrossixStageCode(col, df)),
        "YEAR_OF_BIRTH"  -> ((col: String, df: DataFrame) => parseORXCrossixYearOfBirth(col, df))
    )
}

//  build(new OrxmembercoveragepartdOrxmembercoveragepartd(cfg), allColumns=true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/ORX_MEMBER_COVERAGE_PARTD")