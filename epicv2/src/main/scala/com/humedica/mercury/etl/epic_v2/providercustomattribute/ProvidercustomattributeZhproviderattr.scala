package com.humedica.mercury.etl.epic_v2.providercustomattribute

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class ProvidercustomattributeZhproviderattr (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_providerattr")

  columnSelect = Map(
    "zh_providerattr" -> List("PROV_ID", "PROV_SPECIALTY1", "PROV_SPECIALTY2", "PROV_SPECIALTY3", "PROV_SPECIALTY4", "PROV_SPECIALTY5","PROV_TYPE","EXTRACT_DATE")
  )

  beforeJoin = Map(
    "zh_providerattr" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PROV_ID")).orderBy(df("EXTRACT_DATE").desc)
       df.filter("PROV_ID is not null and PROV_ID <> '-1'")
          .withColumn("rw", row_number.over(groups))
          .filter("rw=1")
          .withColumn("ATTRIBUTE_VALUE_EL", coalesce(df("PROV_SPECIALTY1"), df("PROV_SPECIALTY2"), df("PROV_SPECIALTY3"), df("PROV_SPECIALTY4"), df("PROV_SPECIALTY5"), df("PROV_TYPE")))
          .select("ATTRIBUTE_VALUE_EL","PROV_SPECIALTY1","PROV_ID")
    })
  )

  map = Map(
    "LOCALPROVIDERID" -> mapFrom("PROV_ID"),
    "ATTRIBUTE_VALUE" -> mapFrom("ATTRIBUTE_VALUE_EL")
    )

  mapExceptions = Map(
    ("H303173_EPIC_DH", "ATTRIBUTE_VALUE") -> mapFrom("PROV_SPECIALTY1"),
    ("H303173_EPIC_DH", "ATTRIBUTE_TYPE_CUI") -> literal("CH002488"),
    ("H303173_EPIC_EL", "ATTRIBUTE_TYPE_CUI") -> literal("CH002489")
  )

  afterMap = (df: DataFrame) => {
    df.filter("LOCALPROVIDERID is not null and ATTRIBUTE_VALUE is not null")
  }

}


// val zhp = new ProvidercustomattributeZhproviderattr(cfg); val p = build(zhp) ;  p.show; p.count;