package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by mschlomka on 8/22/2018.
  */

class ProcedurePatienthmtstatus(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient_hmt_status", "zh_clarityhmtopic", "cdr.map_custom_proc")

  columnSelect = Map(
    "patient_hmt_status" -> List("PAT_ID", "QUALIFIED_HMT_ID", "HMT_LAST_UPDATE_DT", "UPDATE_DATE"),
    "zh_clarityhmtopic" -> List("HM_TOPIC_ID", "NAME")
  )

  beforeJoin = Map(
    "patient_hmt_status" -> ((df: DataFrame) => {
      df.filter("pat_id is not null and qualified_hmt_id is not null and hmt_last_update_dt is not null")
        .withColumn("LOCALCODE", concat_ws(".", lit(config(CLIENT_DS_ID)), df("QUALIFIED_HMT_ID")))
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and datasrc = 'patient_hmt_status'")
        .select("LOCALCODE", "MAPPEDVALUE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patient_hmt_status")
      .join(dfs("cdr.map_custom_proc"), Seq("LOCALCODE"), "inner")
      .join(dfs("zh_clarityhmtopic"), dfs("patient_hmt_status")("QUALIFIED_HMT_ID") === dfs("zh_clarityhmtopic")("HM_TOPIC_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("patient_hmt_status"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("HMT_LAST_UPDATE_DT"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "LOCALNAME" -> mapFrom("NAME"),
    "CODETYPE" -> literal("CUSTOM")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("LOCALCODE"), df("PROCEDUREDATE"))
      .orderBy(df("UPDATE_DATE").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}