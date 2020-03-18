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


class TreatmentadminMedadminrec(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("medadminrec",
    "generalorders",
    "cdr.zcm_treatment_type_code",
    "cdr.map_predicate_values"
  )


  columnSelect = Map(
    "medadminrec" -> List("ORDER_MED_ID", "TAKEN_TIME", "PAT_ID", "MAR_ENC_CSN", "MAR_ACTION_C", "SAVED_TIME", "LINE"),
    "generalorders" -> List("PROC_CODE", "ORDER_PROC_ID"),
    "cdr.zcm_treatment_type_code" -> List("GROUPID", "LOCAL_CODE", "TREATMENT_TYPE_CUI", "TREATMENT_TYPE_STD_UNITS", "LOCAL_UNIT")
  )


  beforeJoin = Map(
    "cdr.zcm_treatment_type_code" -> ((df: DataFrame) =>
    {
      includeIf("GROUPID='"+config(GROUP)+"'")(df).drop("GROUPID")
    }),
    "medadminrec" -> ((df: DataFrame) => {
      val act_c_val = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MEDADMINREC", "RXADMIN", "MEDADMINREC", "MAR_ACTION_C")
      df.filter("PAT_ID is not null and MAR_ACTION_C in (" + act_c_val + ")")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("medadminrec")
      .join(dfs("generalorders")
        .join(dfs("cdr.zcm_treatment_type_code"),dfs("cdr.zcm_treatment_type_code")("LOCAL_CODE") === concat(lit(config(CLIENT_DS_ID) + "."),dfs("generalorders")("PROC_CODE")))
        ,dfs("medadminrec")("ORDER_MED_ID") === dfs("generalorders")("ORDER_PROC_ID"))}


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PAT_ID"), df("MAR_ENC_CSN"), df("PROC_CODE"), df("TAKEN_TIME")).orderBy(df("SAVED_TIME").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")

  }



  map = Map(
    "DATASRC" -> literal("medadminrec"),
    "ADMINISTERED_ID" -> concatFrom(Seq("ORDER_MED_ID", "LINE"),delim="."),
    "ADMINISTERED_DATE" -> mapFrom("TAKEN_TIME"),
    "LOCALCODE" -> mapFrom("PROC_CODE", prefix = config(CLIENT_DS_ID) + "."),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ORDER_ID" -> mapFrom("ORDER_MED_ID"),
    "ENCOUNTERID" -> mapFrom("MAR_ENC_CSN"),
    "ADMINISTERED_QUANTITY" -> literal("1"),
    "LOCAL_UNIT" -> mapFrom("LOCAL_UNIT"),
    "CUI" -> mapFrom("TREATMENT_TYPE_CUI"),
    "STD_UNIT_CUI" -> mapFrom("TREATMENT_TYPE_STD_UNITS")

  )




}