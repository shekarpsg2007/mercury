package com.humedica.mercury.etl.cerner_v2.providercontact

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 08/09/2018
 */


class ProvidercontactZhprsnl(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_prsnl")

  columnSelect = Map(
    "zh_prsnl" -> List("UPDT_DT_TM", "PERSON_ID", "EMAIL")
  )

  afterJoin = (df: DataFrame) => {
    val df2 = df.filter("coalesce(person_id, '0') <> '0'")
    val groups = Window.partitionBy(df2("PERSON_ID")).orderBy(df2("UPDT_DT_TM").desc_nulls_last)
    df2.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

  map = Map(
    "DATASRC" -> literal("zh_prsnl"),
    "UPDATE_DATE" -> ((col:String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("UPDT_DT_TM"), current_timestamp()))
    }),
    "LOCAL_PROVIDER_ID" -> mapFrom("PERSON_ID"),
    "EMAIL_ADDRESS" -> mapFrom("EMAIL")
  )

}