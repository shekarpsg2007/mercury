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


class ProcedureHmrecommendation2(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hm_recommendation", "hm_expect_hist", "cdr.map_custom_proc")

  columnSelect = Map(
    "hm_recommendation" -> List("LAST_SATISFACTION_DT_TM", "EXPECT_ID", "PERSON_ID", "UPDT_DT_TM"),
    "hm_expect_hist" -> List("EXPECT_ID", "EXPECT_NAME", "END_EFFECTIVE_DT_TM"),
    "cdr.map_custom_proc" -> List("GROUPID", "DATASRC", "LOCALCODE", "MAPPEDVALUE", "CODETYPE")
  )

  beforeJoin = Map(
    "hm_recommendation" -> ((df: DataFrame) => {
      df.filter("person_id is not null and expect_id is not null and last_satisfaction_dt_tm is not null")
        .withColumn("LOCALCODE", concat_ws(".", lit(config(CLIENT_DS_ID)), df("EXPECT_ID")))
    }),
    "hm_expect_hist" -> ((df: DataFrame) => {
      df.filter("active_ind = '1'")
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and datasrc = 'hm_recommendation_2'")
        .select("LOCALCODE", "MAPPEDVALUE", "CODETYPE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hm_recommendation")
      .join(dfs("hm_expect_hist"), Seq("EXPECT_ID"), "left_outer")
      .join(dfs("cdr.map_custom_proc"), Seq("LOCALCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hm_recommendation_2"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "PROCEDUREDATE" -> mapFrom("LAST_SATISFACTION_DT_TM"),
    "LOCALNAME" -> mapFrom("EXPECT_NAME"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> mapFrom("CODETYPE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("PROCEDUREDATE"), df("MAPPEDCODE"), df("LOCALCODE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last, df("END_EFFECTIVE_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }
}