package com.humedica.mercury.etl.cerner_v2.providerfacilityrelation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 08/09/2018
 */


class ProviderfacilityrelationZhprsnl(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_prsnl", "zh_prsnl_group", "zh_prsnl_group_reltn")

  columnSelect = Map(
    "zh_prsnl" -> List("PERSON_ID"),
    "zh_prsnl_group" -> List("BEG_EFFECTIVE_DT_TM", "PRSNL_GROUP_ID", "PRSNL_GROUP_TYPE_CD", "END_EFFECTIVE_DT_TM"),
    "zh_prsnl_group_reltn" -> List("PERSON_ID", "PRSNL_GROUP_ID")
  )

  beforeJoin = Map(
    "zh_prsnl_group" -> ((df: DataFrame) => {
      df.filter("prsnl_group_type_cd in ('22090972','22082628','37461411')")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_prsnl")
      .join(dfs("zh_prsnl_group_reltn"), Seq("PERSON_ID"), "inner")
      .join(dfs("zh_prsnl_group"), Seq("PRSNL_GROUP_ID"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("zh_prsnl"),
    "LOCALRELSHIPCODE" -> literal("Admitting Privileges"),
    "PROVIDERID" -> mapFrom("PERSON_ID"),
    "STARTDATE" -> mapFrom("BEG_EFFECTIVE_DT_TM"),
    "ENDDATE" -> mapFrom("END_EFFECTIVE_DT_TM"),
    "FACILITYID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("PRSNL_GROUP_TYPE_CD") === lit("22090972"), lit("773220"))
        .when(df("PRSNL_GROUP_TYPE_CD") === lit("22082628"), lit("692287"))
        .when(df("PRSNL_GROUP_TYPE_CD") === lit("37461411"), lit("14335240"))
        .otherwise(null))
    })
  )

}