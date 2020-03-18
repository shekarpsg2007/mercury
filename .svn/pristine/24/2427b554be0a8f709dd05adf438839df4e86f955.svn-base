package com.humedica.mercury.etl.asent.provideridentifier

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ProvideridentifierStaffinfo(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zh_staffinfo")

  columnSelect = Map(
    "as_zh_staffinfo" -> List("DEA_NUMBER", "MASTER_USER_ID", "STATE_LICENSE", "LICENSE_NUMBER")
  )

  join = noJoin()

  afterJoin = (df: DataFrame) => {
    val df1 = df.filter("MASTER_USER_ID  != 0")
    val df_lic = df1.filter("LICENSE_NUMBER is not null").withColumn("ID_TYPE", lit("State License"))
      .withColumn("ID_VALUE", when(df1("STATE_LICENSE").isNotNull, concat(df1("STATE_LICENSE"), lit("."), df1("LICENSE_NUMBER")))
        .otherwise(concat(lit("."), df1("LICENSE_NUMBER"))))
    val df_dea = df1.filter("DEA_NUMBER is not null").withColumn("ID_TYPE", lit("DEA")).withColumn("ID_VALUE", df1("DEA_NUMBER"))
    df_lic.union(df_dea).select("MASTER_USER_ID", "ID_VALUE", "ID_TYPE").distinct()
  }

  map = Map(
    "PROVIDER_ID" -> mapFrom("MASTER_USER_ID"),
    "ID_VALUE" -> mapFrom("ID_VALUE"),
    "ID_TYPE" -> mapFrom("ID_TYPE")
  )

}