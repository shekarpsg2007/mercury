package com.humedica.mercury.etl.epic_v2.provider

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * Created by bhenriksen on 1/18/17.
 */
class ProviderZhreferralsrc(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("zh_referral_src")

  beforeJoin = Map(
    "zh_referral_src" -> ((df: DataFrame) => {
      val dedup = Window.partitionBy(df("referring_prov_id")).orderBy(df("doctor_degree").desc)
      val addColumn = df.withColumn("rn", row_number.over(dedup))
      addColumn.filter("rn = 1 and referring_prov_id is not null").drop("rn")
    })
  )


  map = Map(
    "DATASRC" -> literal("zh_referral_src"),
    "CREDENTIALS" -> mapFrom("DOCTOR_DEGREE"),
    "PROVIDERNAME" -> mapFrom("REFERRING_PROV_NAM"),
    "LOCALPROVIDERID" -> mapFrom("REFERRING_PROV_ID", prefix="ref.")  //prefix with "ref."
  )


  afterMap = (df: DataFrame) => {
    df.filter("LOCALPROVIDERID is not null")
  }

}

