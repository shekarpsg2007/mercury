package com.humedica.mercury.etl.epic_v2.treatmentorder

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._

/**
 * Auto-generated on 02/01/2017
 */


class TreatmentorderGeneralorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("generalorders",
    "cdr.zcm_treatment_type_code")


  columnSelect = Map(
    "generalorders" -> List("PROC_CODE", "ORDER_TIME", "PAT_ID", "ORDER_PROC_ID", "AUTHRZING_PROV_ID", "PAT_ENC_CSN_ID", "ORDER_STATUS_C", "UPDATE_DATE"),
    "cdr.zcm_treatment_type_code" -> List("GROUPID", "LOCAL_CODE", "TREATMENT_TYPE_CUI", "TREATMENT_TYPE_STD_UNITS", "LOCAL_UNIT")
  )


  beforeJoin =
    Map("generalorders" -> ((df: DataFrame) =>
    {
      val fil = df.filter("ORDER_PROC_ID IS NOT NULL AND PAT_ID IS NOT NULL AND ORDER_STATUS_C IN ('5','2','3','-1')")
      val groups = Window.partitionBy(fil("PAT_ID"), fil("PAT_ENC_CSN_ID"), fil("PROC_CODE"), fil("ORDER_TIME")).orderBy(fil("UPDATE_DATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    }
      ),
      "cdr.zcm_treatment_type_code" -> ((df: DataFrame) =>
      {
        includeIf("GROUPID='"+config(GROUP)+"'")(df).drop("GROUPID")
      })
    )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("generalorders")
      .join(dfs("cdr.zcm_treatment_type_code"), concat(lit(config(CLIENT_DS_ID) + "."),dfs("generalorders")("PROC_CODE")) === dfs("cdr.zcm_treatment_type_code")("LOCAL_CODE"))}



  map = Map(
    "DATASRC" -> literal("generalorders"),
    "LOCALCODE" -> mapFrom("PROC_CODE", prefix = config(CLIENT_DS_ID) + "."),
    "ORDER_DATE" -> mapFrom("ORDER_TIME"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCAL_UNIT" -> mapFrom("LOCAL_UNIT"),
    "ORDER_ID" -> mapFrom("ORDER_PROC_ID"),
    "ORDER_PROV_ID" -> mapFrom("AUTHRZING_PROV_ID"),
    "CUI" -> mapFrom("TREATMENT_TYPE_CUI"),
    "STD_UNIT_CUI" -> mapFrom("TREATMENT_TYPE_STD_UNITS")
  )

}
