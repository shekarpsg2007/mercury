package com.humedica.mercury.etl.asent.provider


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class ProviderResourcede(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zh_staffinfo",
    "as_zh_resource_de")

  columnSelect = Map(
    "as_zh_staffinfo" -> List("DATE_OF_BIRTH", "NPI_NUMBER", "PROVIDER_CODE"),
    "as_zh_resource_de" -> List("ID", "ENTRYNAME", "ENTRYCODE", "EFFECTIVEDT")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_zh_resource_de")
      .join(dfs("as_zh_staffinfo"), dfs("as_zh_staffinfo")("provider_code") === dfs("as_zh_resource_de")("entrycode"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("resourcede"),
    "LOCALPROVIDERID" -> mapFrom("ID", prefix = "de."),
    "DOB" -> mapFrom("DATE_OF_BIRTH"),
    "PROVIDERNAME" -> ((col, df) => df.withColumn(col, expr("upper(substr(ENTRYNAME, 1,instr(ENTRYNAME,'(')-1))"))),
    "NPI" -> mapFrom("NPI_NUMBER")
  )


  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("LOCALPROVIDERID")).orderBy(df("EFFECTIVEDT").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw=1 and LOCALPROVIDERID is not null ")
  }


}

// Test
// val bp = new ProviderResourcede(cfg) ; val p = build(bp); p.orderBy("LOCALPROVIDERID").show; p.count