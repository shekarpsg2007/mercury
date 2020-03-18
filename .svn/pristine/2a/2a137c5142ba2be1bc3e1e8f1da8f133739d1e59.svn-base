package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/27/2017
 */


class ObservationHmtppn(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("hmt_ppn_at", "cdr.zcm_obstype_code")

  beforeJoin =
    Map("hmt_ppn_at" -> ((df: DataFrame) =>
    {
      df.filter(df("HMT_PPN_RSN_AT_C") =!= "-1")
        .withColumn("LOCALRESULT", when(isnull(df("HMT_PPN_RSN_AT_C")),null).otherwise(concat(lit(config(CLIENT_DS_ID)+"."),df("HMT_PPN_RSN_AT_C"))))
        .withColumn("LOCALCODE", when(isnull(df("HMT_PPN_TOPIC_ID")),null).otherwise(concat(lit(config(CLIENT_DS_ID)+"."),df("HMT_PPN_TOPIC_ID"))))
    }
      ),
      "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
        df.filter("DATASRC = 'hmt_ppn' and GROUPID = '"+config(GROUP)+"'").drop("GROUPID")
      })
    )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("hmt_ppn_at")
      .join(dfs("cdr.zcm_obstype_code"), dfs("hmt_ppn_at")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("obscode"), "inner")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PAT_ID"), df("LOCALCODE"), df("HMT_PPN_DTTM"), df("OBSTYPE")).orderBy(df("LINE").desc)
    val addColumn = df.withColumn("rank_obs", row_number.over(groups))
    addColumn.filter("rank_obs = 1 and HMT_PPN_DTTM is not null and PAT_ID is not null")
  }


  map = Map(
    "DATASRC" -> literal("hmt_ppn"),
    "LOCALCODE" -> mapFrom("HMT_PPN_TOPIC_ID", prefix=config(CLIENT_DS_ID)+"."),
    "OBSDATE" -> mapFrom("HMT_PPN_DTTM"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "LOCALRESULT" -> mapFrom("HMT_PPN_RSN_AT_C", prefix=config(CLIENT_DS_ID)+"."),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "OBSTYPE" -> mapFrom("OBSTYPE")
  )

 }