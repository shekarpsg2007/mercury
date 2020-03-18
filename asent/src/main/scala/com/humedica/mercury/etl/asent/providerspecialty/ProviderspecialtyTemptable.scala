package com.humedica.mercury.etl.asent.providerspecialty

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ProviderspecialtyTemptable(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zh_staffinfo", "as_zh_referring_provider_de")

  columns = List("ID", "NPI_NUMBER", "SPECIALTY_ID", "SPECIALTY_2_ID", "SPECIALTYDE")

  columnSelect = Map(
    "as_zh_staffinfo" -> List("SPECIALTY_ID", "SPECIALTY_2_ID", "MASTER_USER_ID", "NPI_NUMBER"),
    "as_zh_referring_provider_de" -> List("ID", "SPECIALTYDE", "LINKEDPROVIDERID")
  )


  beforeJoin = Map(
    "as_zh_staffinfo" -> ((df: DataFrame) => {
      df.filter("MASTER_USER_ID != 0")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_zh_staffinfo")
      .join(dfs("as_zh_referring_provider_de"), dfs("as_zh_staffinfo")("MASTER_USER_ID") === dfs("as_zh_referring_provider_de")("LINKEDPROVIDERID"), "inner")
  }

  map = Map(
    "ID" -> mapFrom("ID"),
    "NPI" -> mapFrom("NPI_NUMBER"),
    "SPECIALTY_ID" -> mapFrom("SPECIALTY_ID"),
    "SPECIALTY_2_ID" -> mapFrom("SPECIALTY_2_ID"),
    "SPECIALTYDE" -> mapFrom("SPECIALTYDE")
  )

  afterMap = (df: DataFrame) => {
    df.select("ID", "NPI_NUMBER", "SPECIALTY_ID", "SPECIALTY_2_ID", "SPECIALTYDE").distinct()
  }
}
