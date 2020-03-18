package com.humedica.mercury.etl.cerner_v2.encountercarearea

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 08/09/2018
 */


class EncountercareareaEnclochist(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinicalencounter:cerner_v2.clinicalencounter.ClinicalencounterEncounter", "enc_loc_hist", "zh_code_value",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID", "ARRIVALTIME", "ADMITTIME", "DISCHARGETIME"),
    "enc_loc_hist" -> List("LOC_BUILDING_CD", "LOC_BUILDING_CD", "ENCNTR_ID", "LOC_FACILITY_CD", "END_EFFECTIVE_DT_TM",
      "BEG_EFFECTIVE_DT_TM", "LOC_NURSE_UNIT_CD", "MED_SERVICE_CD", "UPDT_DT_TM"),
    "zh_code_value" -> List("CODE_VALUE", "CDF_MEANING", "DESCRIPTION", "DISPLAY")
  )

  beforeJoin = Map(
    "enc_loc_hist" -> ((df: DataFrame) => {
      df.filter("coalesce(loc_nurse_unit_cd, '0') <> '0'")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("enc_loc_hist")
      .join(dfs("clinicalencounter"), dfs("enc_loc_hist")("ENCNTR_ID") === dfs("clinicalencounter")("ENCOUNTERID"), "inner")
      .join(dfs("zh_code_value"), dfs("enc_loc_hist")("LOC_NURSE_UNIT_CD") === dfs("zh_code_value")("CODE_VALUE"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("enc_loc_hist"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ENCOUNTERTIME" -> mapFrom("ARRIVALTIME"),
    "FACILITYID" -> mapFrom("LOC_FACILITY_CD", nullIf = Seq("0")),
    "CAREAREAENDTIME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("END_EFFECTIVE_DT_TM").isNotNull &&
          substring(df("END_EFFECTIVE_DT_TM"),1, 4) =!= lit("2100") &&
          from_unixtime(unix_timestamp(df("END_EFFECTIVE_DT_TM"))) <= from_unixtime(unix_timestamp(df("DISCHARGETIME"))) &&
          from_unixtime(unix_timestamp(df("END_EFFECTIVE_DT_TM"))) > from_unixtime(unix_timestamp(df("ADMITTIME"))) &&
          from_unixtime(unix_timestamp(df("END_EFFECTIVE_DT_TM"))) > from_unixtime(unix_timestamp(df("BEG_EFFECTIVE_DT_TM")))
          , df("END_EFFECTIVE_DT_TM"))
        .otherwise(df("DISCHARGETIME")))
    }),
    "CAREAREASTARTTIME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("BEG_EFFECTIVE_DT_TM").isNotNull &&
          substring(df("BEG_EFFECTIVE_DT_TM"),1, 4) =!= lit("2100") &&
          from_unixtime(unix_timestamp(df("BEG_EFFECTIVE_DT_TM"))) <= from_unixtime(unix_timestamp(df("DISCHARGETIME"))) &&
          from_unixtime(unix_timestamp(df("BEG_EFFECTIVE_DT_TM"))) >= from_unixtime(unix_timestamp(df("ADMITTIME"))) &&
          from_unixtime(unix_timestamp(df("BEG_EFFECTIVE_DT_TM"))) < from_unixtime(unix_timestamp(df("END_EFFECTIVE_DT_TM")))
          , df("BEG_EFFECTIVE_DT_TM"))
          .otherwise(df("ADMITTIME")))
    }),
    "LOCALCAREAREACODE" -> mapFrom("LOC_NURSE_UNIT_CD", prefix=config(CLIENT_DS_ID)+"."),
    "LOCALLOCATIONNAME" -> ((col: String, df: DataFrame) => {
      val list_cdf_meaning = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENC_LOC_HIST", "ENCOUNTERCAREAREA", "ZH_CODE_VALUE", "CDF_MEANING")
      val useDisplayAsLocName = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENC_LOC_HIST", "ENCOUNTERCAREAREA", "ZH_CODE_VALUE", "CODE_VALUE")
      df.withColumn(col,
        when(lit(list_cdf_meaning) === lit("'Y'") && df("CDF_MEANING") === lit("NURSEUNIT"), df("DESCRIPTION"))
        .when(lit(list_cdf_meaning) === lit("'Y'") && df("CDF_MEANING") =!= lit("NURSEUNIT"), null)
        .when(df("CODE_VALUE").isin(useDisplayAsLocName: _*), df("DISPLAY"))
        .otherwise(df("DESCRIPTION")))
    }),
    "SERVICECODE" -> mapFrom("MED_SERVICE_CD", prefix = config(CLIENT_DS_ID) + ".", nullIf = Seq("0"))
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("LOCALCAREAREACODE"), df("PATIENTID"), df("ENCOUNTERID"), df("SERVICECODE"),
                                      df("CAREAREASTARTTIME"), df("CAREAREAENDTIME"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and encountertime is not null")
      .drop("rn")
  }

  mapExceptions = Map(
    ("H416989_CR2", "FACILITYID") -> mapFrom("LOC_BUILDING_CD", nullIf = Seq("0"))
  )

}