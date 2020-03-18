package com.humedica.mercury.etl.epic_v2.treatmentadmin

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/**
 * Auto-generated on 02/01/2017
 */


class TreatmentadminPatass(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patassessment",
    "cdr.zcm_treatment_type_code")


  columnSelect = Map(
    "cdr.zcm_treatment_type_code" -> List("GROUPID", "LOCAL_CODE", "TREATMENT_TYPE_CUI", "TREATMENT_TYPE_STD_UNITS", "LOCAL_UNIT")
  )

  beforeJoin = Map(
    "cdr.zcm_treatment_type_code" ->  ((df: DataFrame) =>
    {
      includeIf("GROUPID='"+config(GROUP)+"'")(df).drop("GROUPID")
    }),
    "patassessment" -> ((df: DataFrame) => {
      val fil = df.filter("PAT_ID is not null and FLO_MEAS_ID is not null")
      val groups = Window.partitionBy(fil("FLO_MEAS_ID"), fil("PAT_ID"), fil("PAT_ENC_CSN_ID"), fil("RECORDED_TIME")).orderBy(fil("ENTRY_TIME").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    }
      ))


  join = (dfs: Map[String, DataFrame]) => {
    dfs("patassessment")
      .join(dfs("cdr.zcm_treatment_type_code"), concat(lit(config(CLIENT_DS_ID) + "."),dfs("patassessment")("FLO_MEAS_ID")) === dfs("cdr.zcm_treatment_type_code")("LOCAL_CODE"))}


  map = Map(
    "DATASRC" -> literal("patass"),
    "ADMINISTERED_ID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat(df("FLO_MEAS_ID"), df("PAT_ID"), df("PAT_ENC_CSN_ID"), from_unixtime(unix_timestamp(df("RECORDED_TIME"), "yyyymmddHHmmss"))))
    }),
    "ADMINISTERED_ID" -> concatFrom(Seq("FLO_MEAS_ID", "PAT_ID", "PAT_ENC_CSN_ID", "RECORDED_TIME"), delim ="."),
    "ADMINISTERED_DATE" -> mapFrom("RECORDED_TIME"),
    "LOCALCODE" -> mapFrom("FLO_MEAS_ID", prefix=config(CLIENT_DS_ID)+"."),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "LOCAL_UNIT" -> mapFrom("LOCAL_UNIT"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "ADMINISTERED_QUANTITY" -> ((col:String,df:DataFrame) => df.withColumn(col, substring(df("MEAS_VALUE"),1,255))),
    "LOCAL_UNIT" -> mapFrom("LOCAL_UNIT"),
    "CUI" -> mapFrom("TREATMENT_TYPE_CUI"),
    "STD_UNIT_CUI" -> mapFrom("TREATMENT_TYPE_STD_UNITS")

  )


}