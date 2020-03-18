package com.humedica.mercury.etl.crossix.orxmemberphi

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.crossix.util.CrossixETLUtility._

/**
  * Created by rbabu on 6/27/17.
  */
class OrxmemberphiOrxmemberphi  (config: Map[String,String]) extends EntitySource(config: Map[String,String]){
    tables = List("member", "member_phi", "carriersblc2v2.carriers_blc2_v2")

    columns = List("ACCOUNT_ID", "ADD_DATE", "ADDRESS_1", "ADDRESS_2", "ALT_ID", "CARRIER_ID", "CITY_NAME", "DATA_SOURCE_CODE",
        "DATE_OF_BIRTH", "FAMILY_ID", "FIRST_NAME", "GENDER", "GROUP_ID", "LANGUAGE_CODE", "LAST_NAME", "LSRD_INDIVIDUAL_ID",
        "MEDICAL_CLAIMS_PROCESSOR_CODE", "MEDICARE_ID", "MEMBER_ID", "MIDDLE_INITIAL", "ORX_INDIVIDUAL_ID", "PHI_ADD_DATE",
        "PHONE", "SSN", "STATE_CODE", "UPDATE_DATE", "ZIPCODE")

    columnSelect = Map(
        "member" -> List("ABACCD", "ABC2DT", "ABADTM", "ABA1TX", "ABA2TX", "ABABCD", "ABAACD", "ABAYTX", "ABAJDT", "ABQBCD", "ABARTX", "ABAAST", "ABADCD", "ABB5S2",
            "ABAQTX", "ABAXTX", "ABABCD", "ABASTX", "ABAACD", "ABACCD", "ABADCD", "ABABCD", "ABC2DT", "ABADTM", "ABABNB", "ABA0NB",
            "ABAZTX", "ABBMDT", "ABABTM", "ABA0TX"),
        "member_phi" -> List("LSRD_INDIVIDUAL_ID", "SOURCE_MEMBER_ID"),
        "carriersblc2v2.carriers_blc2_v2" -> List("CARRIER_ID", "MEDICAL_CLAIMS_PROCESSOR_CODE", "BOOK", "OPTYPE")
    )

    beforeJoin = Map(
        "carriersblc2v2.carriers_blc2_v2" -> renameColumns(List(("CARRIER_ID", "MAP_CARRIER_ID")))
    )

    join = (dfs: Map[String, DataFrame]) => {
        var member_phi = dfs("member_phi").select("LSRD_INDIVIDUAL_ID", "SOURCE_MEMBER_ID").distinct()
        dfs("member")
                .join(dfs("carriersblc2v2.carriers_blc2_v2"),
                    ((dfs("carriersblc2v2.carriers_blc2_v2")("OPTYPE")==="0" && dfs("member")("ABAACD") === dfs("carriersblc2v2.carriers_blc2_v2")("MAP_CARRIER_ID")) ||
                            (dfs("carriersblc2v2.carriers_blc2_v2")("OPTYPE")==="1" && dfs("member")("ABAACD").contains(dfs("carriersblc2v2.carriers_blc2_v2")("MAP_CARRIER_ID")))), "left_outer")
                .join(member_phi, createOrxIndividualId(dfs("member")) ===  member_phi("SOURCE_MEMBER_ID"), "left_outer")
    }

    afterMap = (df: DataFrame) => {
        df.groupBy("ACCOUNT_ID", "ADD_DATE", "ADDRESS_1", "ADDRESS_2", "ALT_ID", "CARRIER_ID", "CITY_NAME", "DATA_SOURCE_CODE",
            "DATE_OF_BIRTH", "FAMILY_ID", "FIRST_NAME", "GENDER", "GROUP_ID", "LANGUAGE_CODE", "LAST_NAME", "LSRD_INDIVIDUAL_ID",
            "MEDICAL_CLAIMS_PROCESSOR_CODE", "MEDICARE_ID", "MEMBER_ID", "MIDDLE_INITIAL", "ORX_INDIVIDUAL_ID", "PHI_ADD_DATE",
            "PHONE", "SSN", "STATE_CODE", "ZIPCODE").agg(max("UPDATE_DATE").as("UPDATE_DATE"))
    }

    def createOrxIndividualId(df:DataFrame) = {
        concat(df("ABAACD"), df("ABACCD"), df("ABADCD"),df("ABABCD"))
    }

    map = Map(

        "ACCOUNT_ID" -> mapFrom("ABACCD"),
        "MEDICAL_CLAIMS_PROCESSOR_CODE" ->  ((col: String, df: DataFrame) => {df.withColumn(col, when(df("MEDICAL_CLAIMS_PROCESSOR_CODE")==="null", null).otherwise(df("ABABCD")))}),
        "ADD_DATE"  -> ((col: String, df: DataFrame) => {df.withColumn(col,  concat(parseORXCrossixDateStr("ABC2DT", df), lit(" "), parseCrossixTime("ABADTM", df)))}),
        "ADDRESS_1" -> mapFrom("ABA1TX"),
        "ADDRESS_2" -> mapFrom("ABA2TX"),
        "ALT_ID" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("ABABCD").isNull || length(df("ABABCD")) > 11, "NA").otherwise(df("ABABCD")))}),
        "CARRIER_ID" -> mapFrom("ABAACD"),
        "CITY_NAME" -> ((col: String, df: DataFrame) => {df.withColumn(col, upper(df("ABAYTX")))}),
        "DATA_SOURCE_CODE" -> literal("RS"),
        "DATE_OF_BIRTH" -> ((col: String, df: DataFrame) => {df.withColumn(col, when((df("ABAJDT").isNull).or(trim(df("ABAJDT"))==="00000000").or(trim(df("ABAJDT"))==="0"), "0100-01-01").otherwise(parseORXCrossixDateStr1("ABAJDT", "yyyyMMdd", df)))}),
        "FAMILY_ID" -> mapFrom("ABQBCD"),
        "FIRST_NAME" -> mapFrom("ABARTX"),
        "GENDER" -> ((col: String, df: DataFrame) => parseORXGender(col, df)),
        "GROUP_ID" -> mapFrom("ABADCD"),
        "LANGUAGE_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("ABB5S2").isNull, "UNK").otherwise(df("ABB5S2")))}),
        "LAST_NAME" -> mapFrom("ABAQTX"),
        "LSRD_INDIVIDUAL_ID" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("LSRD_INDIVIDUAL_ID").isNotNull, (df("LSRD_INDIVIDUAL_ID"))))}),
        "MEDICARE_ID" ->((col: String, df: DataFrame) => {df.withColumn(col, when((df("ABAXTX").isNull).or(length(trim(df("ABAXTX")))===0), null).otherwise(trim(df("ABAXTX"))))}),
        "MEMBER_ID" -> mapFrom("ABABCD"),
        "MIDDLE_INITIAL" -> ((col: String, df: DataFrame) => {df.withColumn(col, upper(df("ABASTX")))}),
        "ORX_INDIVIDUAL_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(createOrxIndividualId(df), 256))),
        "PHI_ADD_DATE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  concat(parseORXCrossixDateStr("ABC2DT", df), lit(" "),  parseCrossixTime("ABADTM",df)))}),
        "PHONE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("ABABNB").isNull, "0000000000").otherwise(df("ABABNB")))}),
        "SSN" ->((col: String, df: DataFrame) => {df.withColumn(col, when((length(df("ABA0NB")) !== 9), "000000000").otherwise(df("ABA0NB")))}),
        "STATE_CODE" -> ((col: String, df: DataFrame) => parseORXCrossixStageCode(col, df)),
        "UPDATE_DATE"  -> ((col: String, df: DataFrame) => {df.withColumn(col, concat(parseORXCrossixDateStr("ABBMDT", df), lit(" "), parseCrossixTime("ABABTM", df)))}),
        "ZIPCODE" ->((col: String, df: DataFrame) => {df.withColumn(col, when((length(df("ABA0TX")) < 5), null).otherwise(when((length(df("ABA0TX"))===5),df("ABA0TX")).otherwise(df("ABA0TX").substr(0,5))))})
    )
}

// Toolkit.build(new OrxmemberphiOrxmemberphi(cfg), allColumns=true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/ORX_MEMBER_PHI")
