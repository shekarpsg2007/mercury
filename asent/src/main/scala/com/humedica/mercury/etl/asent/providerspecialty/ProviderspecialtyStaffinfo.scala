package com.humedica.mercury.etl.asent.providerspecialty

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ProviderspecialtyStaffinfo(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zh_staffinfo", "as_zh_referring_provider_de", "temptable:asent.providerspecialty.ProviderspecialtyTemptable")

  columnSelect = Map(
    "as_zh_staffinfo" -> List("SPECIALTY_ID", "SPECIALTY_2_ID", "MASTER_USER_ID"),
    "as_zh_referring_provider_de" -> List("ID", "SPECIALTYDE")
  )


  beforeJoin = Map(
    "as_zh_staffinfo" -> ((df: DataFrame) => {
      val fpiv = unpivot(Seq("SPECIALTY_ID", "SPECIALTY_2_ID"), Seq("1", "2"), typeColumnName = "LOCAL_ORDER")
      val df1 = fpiv("SPECIALTY", df)
      val addColumns = df1.filter("MASTER_USER_ID != 0 and SPECIALTY != 0")
        .withColumn("LOCALSPECIALTYCODE", concat(lit(config(CLIENT_DS_ID) + "."), df1("SPECIALTY")))
      addColumns.select("MASTER_USER_ID", "LOCALSPECIALTYCODE", "LOCAL_ORDER")
    }),
    "as_zh_referring_provider_de" -> ((df: DataFrame) => {
      val fil = df.filter("ID != '0' and SPECIALTYDE != 0")
      val addColumns = fil.withColumn("MASTER_USER_ID", concat(lit("R."), fil("ID")))
        .withColumn("LOCALSPECIALTYCODE", concat(lit(config(CLIENT_DS_ID) + "."), fil("SPECIALTYDE")))
        .withColumn("LOCAL_ORDER", lit("1"))
      addColumns.select("MASTER_USER_ID", "LOCALSPECIALTYCODE", "LOCAL_ORDER")
    }),
    "temptable" -> ((df: DataFrame) => {
      val fpiv = unpivot(Seq("SPECIALTY_ID", "SPECIALTY_2_ID"), Seq("2", "3"), typeColumnName = "LOCAL_ORDER")
      val df1 = fpiv("SPECIALTY", df)
      val addColumns = df1.filter("ID != '0' and SPECIALTY != '0'")
        .withColumn("MASTER_USER_ID", concat(lit("R."), df1("ID")))
      addColumns.select("MASTER_USER_ID", "SPECIALTY", "LOCAL_ORDER")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_zh_staffinfo")
      .union(dfs("as_zh_referring_provider_de"))
      .union(dfs("temptable"))
  }

  map = Map(
    "LOCALPROVIDERID" -> mapFrom("MASTER_USER_ID"),
    "LOCALSPECIALTYCODE" -> mapFrom("LOCALSPECIALTYCODE"),
    "LOCALCODESOURCE" -> literal("zh_provider"),
    "LOCAL_CODE_ORDER" -> mapFrom("LOCAL_ORDER")
  )

  afterMap = (df: DataFrame) => {
    df.filter("LOCALPROVIDERID is not null and LOCALSPECIALTYCODE is not null").distinct()
  }

  beforeJoinExceptions = Map(
    "H285893_AS ENT" -> Map(
      "temptable" -> ((df: DataFrame) => {
        val df1 = df.filter("ID != '0'")
          .withColumn("MASTER_USER_ID", concat(lit("R."), df("ID")))
          .withColumn("LOCAL_ORDER", lit("1"))
          .withColumn("SPECIALTY", coalesce(df("SPECIALTY_ID"), df("SPECIALTYDE")))
        df1.select("MASTER_USER_ID", "SPECIALTY", "LOCAL_ORDER")
      }),
      "as_zh_staffinfo" -> ((df: DataFrame) => {
        val fpiv = unpivot(Seq("SPECIALTY_ID", "SPECIALTY_2_ID"), Seq("1", "2"), typeColumnName = "LOCAL_ORDER")
        val df1 = fpiv("SPECIALTY", df)
        val addColumns = df1.filter("MASTER_USER_ID != 0 and SPECIALTY != 0 and LOCAL_ORDER != '2'")
          .withColumn("LOCALSPECIALTYCODE", concat(lit(config(CLIENT_DS_ID) + "."), df1("SPECIALTY")))
        addColumns.select("MASTER_USER_ID", "LOCALSPECIALTYCODE", "LOCAL_ORDER")
      })
    )
  )

  joinExceptions = Map(
    "H285893_AS ENT" -> ((dfs: Map[String, DataFrame]) => {
      dfs("as_zh_staffinfo").union(dfs("temptable"))
    })
  )
}