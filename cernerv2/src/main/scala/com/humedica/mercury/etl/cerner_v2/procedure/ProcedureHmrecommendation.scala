package com.humedica.mercury.etl.cerner_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 08/09/2018
  */


class ProcedureHmrecommendation(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hm_recommendation", "hm_expect_hist", "hm_recommendation_action", "cdr.map_custom_proc",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "hm_recommendation" -> List("RECOMMENDATION_ID", "EXPECT_ID", "PERSON_ID"),
    "hm_expect_hist" -> List("EXPECT_ID", "EXPECT_NAME", "BEG_EFFECTIVE_DT_TM"),
    "hm_recommendation_action" -> List("RECOMMENDATION_ID", "REASON_CD", "UPDT_DT_TM", "SATISFACTION_DT_TM"),
    "cdr.map_custom_proc" -> List("GROUPID", "DATASRC", "LOCALCODE", "MAPPEDVALUE", "CODETYPE")
  )

  beforeJoin = Map(
    "hm_recommendation" -> ((df: DataFrame) => {
      df.filter("person_id is not null and expect_id is not null")
        .withColumn("LOCALCODE", concat_ws(".", lit(config(CLIENT_DS_ID)), df("EXPECT_ID")))
    }),
    "hm_expect_hist" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("EXPECT_ID")).orderBy(df("BEG_EFFECTIVE_DT_TM").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn")
    }),
    "hm_recommendation_action" -> ((df: DataFrame) => {
      val list_reason_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "HM_RECOMMENDATION", "PROCEDURE", "HM_RECOMMENDATION_ACTION", "REASON_CD")
      df.filter("satisfaction_dt_tm is not null and ('NO_MPV_MATCHES' in (" + list_reason_cd + ") or " +
        "reason_cd in (" + list_reason_cd + "))")
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and datasrc = 'hm_recommendation'")
        .select("LOCALCODE", "MAPPEDVALUE", "CODETYPE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hm_recommendation")
      .join(dfs("hm_recommendation_action"), Seq("RECOMMENDATION_ID"), "inner")
      .join(dfs("hm_expect_hist"), Seq("EXPECT_ID"), "left_outer")
      .join(dfs("cdr.map_custom_proc"), Seq("LOCALCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hm_recommendation"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "PROCEDUREDATE" -> mapFrom("SATISFACTION_DT_TM"),
    "LOCALNAME" -> mapFrom("EXPECT_NAME"),
    "ACTUALPROCDATE" -> mapFrom("SATISFACTION_DT_TM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> mapFrom("CODETYPE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("PROCEDUREDATE"), df("MAPPEDCODE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}