package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ObservationOrderresults(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("orderresults",
    "generalorders",
    "cdr.zcm_obstype_code")

  columnSelect = Map(
    "orderresults" -> List("ORDER_PROC_ID", "COMPONENT_ID", "RESULT_DATE", "RESULT_TIME", "PAT_ID", "REFERENCE_UNIT", "RESULT_STATUS_C",
      "PAT_ENC_CSN_ID", "ORD_VALUE", "UPDATE_DATE"),
    "generalorders" -> List("ORDER_PROC_ID", "SPECIMN_TAKEN_TIME", "PAT_ID", "PAT_ENC_CSN_ID"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "LOCALUNIT", "GROUPID")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'orderresults' and GROUPID = '" + config(GROUP) + "'").drop("GROUPID")
    }),
    "generalorders" -> ((df: DataFrame) => {
      df.withColumnRenamed("PAT_ID", "PAT_ID_GO")
        .withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_GO")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("orderresults")
      .join(dfs("generalorders"), Seq("ORDER_PROC_ID"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), concat(lit(config(CLIENT_DS_ID) + "."), dfs("orderresults")("COMPONENT_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE"))

  }


  afterJoin = includeIf("coalesce(RESULT_STATUS_C,'-1') IN ('3','4','-1')")



  map = Map(
    "DATASRC" -> literal("orderresults"),
    "LOCALCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(isnull(df("COMPONENT_ID")), null)
        .otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("COMPONENT_ID"))))
    }),
    "OBSDATE" -> cascadeFrom(Seq("SPECIMN_TAKEN_TIME", "RESULT_DATE")),
    "PATIENTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PAT_ID").isNull || df("PAT_ID") === lit("-1"), df("PAT_ID_GO")).otherwise(df("PAT_ID")))
    }),
    "LOCAL_OBS_UNIT" -> mapFrom("REFERENCE_UNIT"),
    "STATUSCODE" -> mapFrom("RESULT_STATUS_C"),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PAT_ENC_CSN_ID").isNull || df("PAT_ENC_CSN_ID") === lit("-1"), df("PAT_ENC_CSN_ID_GO")).otherwise(df("PAT_ENC_CSN_ID")))
    }),
    "LOCALRESULT" -> mapFrom("ORD_VALUE"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "OBSTYPE" -> mapFrom("OBSTYPE")
  )


  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ENCOUNTERID"), df("PATIENTID"), df("LOCALCODE"), df("OBSDATE"), df("OBSTYPE")).orderBy(df("UPDATE_DATE").desc, df("RESULT_TIME").desc)
    val addColumn = df.withColumn("rank_obs", row_number.over(groups))
    addColumn.filter("rank_obs = 1 and OBSDATE is not null and PATIENTID is not null")
  }

}
