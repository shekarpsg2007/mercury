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


class ObservationOrdermetrics(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_prl_map", "order_metrics", "generalorders", "order_parent_info", "zh_cl_prl_ss", "cdr.zcm_obstype_code")


  columnSelect = Map(
    "generalorders" -> List("PROC_CODE", "PAT_ENC_CSN_ID", "PAT_ID", "UPDATE_DATE", "ORDER_PROC_ID"),
    "order_parent_info" ->List("ORDER_ID", "PARENT_ORDER_ID"),
    "order_metrics" -> List("ORDER_DTTM", "PAT_ENC_CSN_ID", "ORDER_ID", "PRL_ORDERSET_ID", "PAT_ID"),
    "zh_cl_prl_ss" -> List("PROTOCOL_ID", "CM_LOG_OWNER_ID"),
    "zh_prl_map" -> List("CID", "CM_ACTV_DPLY_ID", "INTERNAL_ID"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "GROUPID")
  )


  beforeJoin = Map(
    "generalorders" -> includeIf("PROC_CODE is not null"),
    "order_metrics" -> ((df: DataFrame) => {
      df.filter("PRL_ORDERSET_ID is not null and ORDER_DTTM is not null and PAT_ID is not null" +
        " and (PAT_ID <> '-1' or PAT_ENC_CSN_ID <> '-1')").drop("PAT_ID")
        .withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_OMO")
        .withColumnRenamed("ORDER_ID", "ORDER_ID_OMO")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'order_metrics' and GROUPID = '"+config(GROUP)+"'").drop("GROUPID")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("generalorders")
      .join(dfs("order_parent_info")
        .join(dfs("order_metrics")
            .join(dfs("zh_cl_prl_ss")
                .join(dfs("zh_prl_map"),dfs("zh_cl_prl_ss")("PROTOCOL_ID") === dfs("zh_prl_map")("CID") &&
                  dfs("zh_cl_prl_ss")("CM_LOG_OWNER_ID") === dfs("zh_prl_map")("CM_ACTV_DPLY_ID"))
             ,dfs("order_metrics")("PRL_ORDERSET_ID") === dfs("zh_cl_prl_ss")("PROTOCOL_ID"))
          ,dfs("order_parent_info")("PARENT_ORDER_ID") === dfs("order_metrics")("ORDER_ID_OMO"))
        ,dfs("generalorders")("ORDER_PROC_ID") === dfs("order_parent_info")("ORDER_ID"))
       }

  afterJoin = (df: DataFrame) => {
    val addColumn = df.withColumn("LOCALCODE", concat(df("INTERNAL_ID"), lit("_"), df("PROC_CODE")))
      .withColumn("ENCOUNTERID", coalesce(df("PAT_ENC_CSN_ID"), df("PAT_ENC_CSN_ID_OMO")))
    val groups = Window.partitionBy(addColumn("PAT_ID"), addColumn("LOCALCODE"), addColumn("ORDER_DTTM"))
      .orderBy(when(coalesce(addColumn("PAT_ENC_CSN_ID"), addColumn("PAT_ENC_CSN_ID_OMO"), lit("-1")) === lit("-1"), lit("1")).otherwise(lit("0")).asc, addColumn("UPDATE_DATE").desc_nulls_last)
    val dedupe = addColumn.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
    val zcm = table("cdr.zcm_obstype_code")
    dedupe.join(zcm, dedupe("LOCALCODE") === zcm("obscode"), "inner")

  }


  map = Map(
    "DATASRC" -> literal("order_metrics"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "OBSDATE" -> mapFrom("ORDER_DTTM"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCALRESULT" -> mapFrom("LOCALCODE")
  )

}