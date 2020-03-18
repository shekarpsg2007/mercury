package com.humedica.mercury.etl.asent.provider

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ProviderAszhcredentialde(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zh_staffinfo", "as_zh_credential_de", "as_zh_referring_provider_de", "ProvSpeciality:asent.providerspecialty.ProviderspecialtyTemptable")

  columnSelect = Map(
    "as_zh_staffinfo" -> List("LAST_NAME", "MASTER_USER_ID", "FIRST_NAME", "NPI_NUMBER", "CREDENTIAL_ID", "LAST_UPDATED_DATE", "SPECIALTY_ID", "SPECIALTY_2_ID"),
    "as_zh_credential_de" -> List("ENTRYCODE", "ID"),
    "as_zh_referring_provider_de" -> List("FIRSTNAME", "LASTNAME", "MIDDLENAME", "ENTRYNAME", "ID", "SUFFIXNAME")

  )

  beforeJoin = Map(
    "as_zh_staffinfo" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("MASTER_USER_ID"), df("LAST_UPDATED_DATE")).orderBy(df("CREDENTIAL_ID").asc_nulls_last)
      val filterdf = df.withColumn("rn", row_number.over(groups)).filter("rn=1 AND MASTER_USER_ID  <> 0")
      val zh_credential_de = table("as_zh_credential_de")
      val staffinfo = filterdf.join(zh_credential_de, zh_credential_de("ID") === filterdf("CREDENTIAL_ID"), "left_outer")
      val addColumns = staffinfo.withColumn("LOCALPROVIDERID", staffinfo("MASTER_USER_ID"))
        .withColumn("NPI_new", when(staffinfo("NPI_NUMBER").isin("0", "0000000000", "1234567890", "1111111111", "9999999999"), null).otherwise(staffinfo("NPI_NUMBER")))
        .withColumn("MIDDLE_NAME", lit(null))
        .withColumn("CREDENTIALS", staffinfo("ENTRYCODE"))
        .withColumn("PROVIDERNAME", lit(null))
      addColumns.select("LOCALPROVIDERID", "NPI_new", "FIRST_NAME", "LAST_NAME", "MIDDLE_NAME", "CREDENTIALS", "PROVIDERNAME")
    }),
    "ProvSpeciality" -> ((df: DataFrame) => {
      val fil = table("as_zh_referring_provider_de").withColumnRenamed("ID", "ID_rp").filter("ID_rp <> '0'")
      val referringProvider = fil.join(df, fil("ID_rp") === df("ID"), "left_outer")
      val addColumns = referringProvider.withColumn("LOCALPROVIDERID", when(referringProvider("ID_rp").isNotNull, concat(lit("R."), referringProvider("ID_rp"))))
        .withColumn("NPI_new", when(referringProvider("NPI_NUMBER").isin("0", "0000000000", "1234567890", "1111111111", "9999999999"), null).otherwise(referringProvider("NPI_NUMBER")))
        .withColumn("PROVIDERNAME", referringProvider("ENTRYNAME"))
        .withColumn("FIRST_NAME", referringProvider("FIRSTNAME"))
        .withColumn("LAST_NAME", referringProvider("LASTNAME"))
        .withColumn("MIDDLE_NAME", referringProvider("MIDDLENAME"))
        .withColumn("CREDENTIALS", referringProvider("SUFFIXNAME"))
      addColumns.select("LOCALPROVIDERID", "NPI_new", "FIRST_NAME", "LAST_NAME", "MIDDLE_NAME", "CREDENTIALS", "PROVIDERNAME")
    }))


  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_zh_staffinfo").union(dfs("ProvSpeciality"))
  }

  joinExceptions = Map(
    "H503982_AS ENT" -> ((dfs: Map[String, DataFrame]) => {
      dfs("as_zh_staffinfo")
    })
  )

  map = Map(
    "DATASRC" -> literal("As_zh_credential_de"),
    "NPI" -> mapFrom("NPI_new")
  )

  beforeJoinExceptions = Map(
    "H285893_AS ENT" -> Map(
      "as_zh_staffinfo" -> ((df: DataFrame) => {
        val groups = Window.partitionBy(df("MASTER_USER_ID"), df("LAST_UPDATED_DATE")).orderBy(df("CREDENTIAL_ID").asc_nulls_last)
        val filterdf = df.withColumn("rn", row_number.over(groups)).filter("rn=1 AND MASTER_USER_ID  <> 0")
        val zh_credential_de = table("as_zh_credential_de")
        val staffinfo = filterdf.join(zh_credential_de, zh_credential_de("ID") === filterdf("CREDENTIAL_ID"), "left_outer")
        val addColumns = staffinfo.withColumn("LOCALPROVIDERID", staffinfo("MASTER_USER_ID"))
          .withColumn("NPI_new", when(staffinfo("NPI_NUMBER").isin("0", "0000000000", "1234567890", "1111111111", "9999999999"), null).otherwise(staffinfo("NPI_NUMBER")))
          .withColumn("MIDDLE_NAME", lit(null))
          .withColumn("CREDENTIALS", staffinfo("ENTRYCODE"))
          .withColumn("PROVIDERNAME", lit(null))
          .withColumn("LAST_NAME", when(df("SPECIALTY_ID").isin("87", "89", "90", "91", "95", "96", "99", "100"), concat(lit("MPS - "), df("LAST_NAME"))).otherwise(df("LAST_NAME")))
        addColumns.select("LOCALPROVIDERID", "NPI_new", "FIRST_NAME", "LAST_NAME", "MIDDLE_NAME", "CREDENTIALS", "PROVIDERNAME")
      }),
      "ProvSpeciality" -> ((df: DataFrame) => {
        val fil = table("as_zh_referring_provider_de").withColumnRenamed("ID", "ID_rp").filter("ID_rp <> '0'")
        val referringProvider = fil.join(df, fil("ID_rp") === df("ID"), "left_outer")
        val addColumns = referringProvider.withColumn("LOCALPROVIDERID", when(referringProvider("ID_rp").isNotNull, concat(lit("R."), referringProvider("ID_rp"))))
          .withColumn("NPI_new", when(referringProvider("NPI_NUMBER").isin("0", "0000000000", "1234567890", "1111111111", "9999999999"), null).otherwise(referringProvider("NPI_NUMBER")))
          .withColumn("PROVIDERNAME", when(referringProvider("SPECIALTYDE").isin("87", "89", "90", "91", "95", "96", "99", "100"),
            concat(lit("MPS - "), referringProvider("ENTRYNAME"))).otherwise(referringProvider("ENTRYNAME")))
          .withColumn("FIRST_NAME", referringProvider("FIRSTNAME"))
          .withColumn("LAST_NAME", when(referringProvider("SPECIALTYDE").isin("87", "89", "90", "91", "95", "96", "99", "100"),
            concat(lit("MPS - "), referringProvider("LASTNAME"))).otherwise(referringProvider("LASTNAME")))
          .withColumn("MIDDLE_NAME", referringProvider("MIDDLENAME"))
          .withColumn("CREDENTIALS", referringProvider("SUFFIXNAME"))
        addColumns.select("LOCALPROVIDERID", "NPI_new", "FIRST_NAME", "LAST_NAME", "MIDDLE_NAME", "CREDENTIALS", "PROVIDERNAME")
      })
    ))

}