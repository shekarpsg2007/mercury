package com.humedica.mercury.etl.cerner_v2.provider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 08/09/2018
  */


class ProviderZhprsnl(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_prsnl_alias", "zh_prsnl", "zh_credential", "cdr.map_predicate_values", "zh_code_value")

  columnSelect = Map(
    "zh_prsnl_alias" -> List("PERSON_ID", "ALIAS_POOL_CD", "ALIAS", "PRSNL_ALIAS_TYPE_CD", "UPDT_DT_TM"),
    "zh_prsnl" -> List("PERSON_ID", "EMAIL", "NAME_FIRST", "NAME_LAST", "UPDT_DT_TM"),
    "zh_credential" -> List("PRSNL_ID", "CREDENTIAL_CD", "DISPLAY_SEQ", "ACTIVE_IND"),
    "zh_code_value" -> List("CODE_VALUE", "DISPLAY")
  )

  beforeJoin = Map(
    "zh_prsnl_alias" -> ((df: DataFrame) => {
      val list_alias_pool = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "NULL", "ZH_PROVIDER", "ZH_PRSNL_ALIAS", "ALIAS_POOL_CD")
      val list_alias_type = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "NULL", "ZH_PROVIDER", "ZH_PRSNL_ALIAS", "PRSNL_ALIAS_TYPE_CD")

      val fil = df.filter("(alias_pool_cd in (" + list_alias_pool + ") or prsnl_alias_type_cd in (" + list_alias_type + ")) " +
        "and coalesce(alias,'0') <> '0'")
      val groups = Window.partitionBy(fil("PERSON_ID"), fil("ALIAS_POOL_CD"))
        .orderBy(fil("ALIAS"), fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM")
    }),
    "zh_prsnl" -> ((df: DataFrame) => {
      df.filter("coalesce(person_id, '0') <> '0'")
    }),
    "zh_credential" -> ((df: DataFrame) => {
      val cv = table("zh_code_value")
      val df2 = df.filter("active_ind = '1'")
      val joined = df2.join(cv, df2("CREDENTIAL_CD") === cv("CODE_VALUE"), "inner")
      val joined2 = joined.groupBy("PRSNL_ID").agg(collect_list("DISPLAY").as("DISPLAY"))
      joined2.withColumn("DISPLAY", concat_ws(" ", joined2("DISPLAY")))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_prsnl")
      .join(dfs("zh_prsnl_alias"), Seq("PERSON_ID"), "left_outer")
      .join(dfs("zh_credential"), dfs("zh_prsnl")("PERSON_ID") === dfs("zh_credential")("PRSNL_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("zh_prsnl"),
    "LOCALPROVIDERID" -> mapFrom("PERSON_ID"),
    "CREDENTIALS" -> mapFrom("DISPLAY"),
    "EMAILADDRESS" -> mapFrom("EMAIL"),
    "FIRST_NAME" -> mapFrom("NAME_FIRST"),
    "LAST_NAME" -> mapFrom("NAME_LAST"),
    "NPI" -> mapFrom("ALIAS")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("LOCALPROVIDERID")).orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}