package com.humedica.mercury.etl.asent.provider


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class ProviderAlliedprovider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zh_chart_location_de")

  columnSelect = Map(
    "as_zh_chart_location_de" -> List("ID", "ENTRYNAME", "EFFECTIVEDT")
  )

  beforeJoin = Map(
    "as_encounters" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ID")).orderBy(df("EFFECTIVEDT").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1 AND ID NOT NULL")
    }))

  map = Map(
    "DATASRC" -> literal("alliedprovider"),
    "LOCALPROVIDERID" -> mapFrom("ID", prefix = "allied."),
    "PROVIDERNAME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, upper(trim(df("ENTRYNAME"))))
    })
  )

}

// Test
// val bp = new ProviderAlliedprovider(cfg) ; val p = build(bp); p.show; p.count