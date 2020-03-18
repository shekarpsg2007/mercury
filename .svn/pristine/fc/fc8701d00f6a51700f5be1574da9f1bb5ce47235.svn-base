package com.humedica.mercury.etl.cerner_v2.providerspecialty

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Engine
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._


/**
 * Auto-generated on 08/09/2018
 */


class ProviderspecialtyZhprsnl(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_prsnl", "zh_prsnl_group", "zh_prsnl_group_reltn")

  columnSelect = Map(
    "zh_prsnl" -> List("PERSON_ID"),
    "zh_prsnl_group" -> List("PRSNL_GROUP_ID", "PRSNL_GROUP_TYPE_CD"),
    "zh_prsnl_group_reltn" -> List("PERSON_ID", "PRSNL_GROUP_ID")
  )

  beforeJoin = Map(
    "zh_prsnl" -> ((df: DataFrame) => {
      df.filter("person_id is not null")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_prsnl")
      .join(dfs("zh_prsnl_group_reltn"), Seq("PERSON_ID"), "inner")
      .join(dfs("zh_prsnl_group"), Seq("PRSNL_GROUP_ID"), "inner")
  }

  map = Map(
    "LOCALPROVIDERID" -> mapFrom("PERSON_ID"),
    "LOCALSPECIALTYCODE" -> mapFrom("PRSNL_GROUP_TYPE_CD", prefix = config(CLIENT_DS_ID) + "."),
    "LOCAL_CODE_ORDER" -> literal("1"),
    "LOCALCODESOURCE" -> literal("zh_provider")
  )

  afterMap = (df: DataFrame) => {
    val cols = Engine.schema.getStringList("Providerspecialty").asScala.map(_.split("-")(0).toUpperCase())
    df.select(cols.map(col): _*).distinct
  }

}