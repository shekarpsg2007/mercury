package com.humedica.mercury.etl.asent.labmapperdict

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class LabmapperdictVitals(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "cdr.map_observation",
    "as_vitals",
    "as_zc_unit_code_de"
  )

  columnSelect = Map(
    "cdr.map_observation" -> List("GROUPID", "DATASRC", "LOCALCODE", "LOCAL_NAME", "LOCALUNIT"),
    "as_vitals" -> List("UNITS_ID", "VITAL_ENTRY_NAME"),
    "as_zc_unit_code_de" -> List("ID", "ENTRYNAME")
  )

  beforeJoin = Map(
    "cdr.map_observation" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and datasrc = 'vitals' and obstype = 'LABRESULT' and cui = 'CH002048'").drop("GROUPID")
    }),
    "as_vitals" -> ((df: DataFrame) => {
      val prejoin = df.join(table("as_zc_unit_code_de"), table("as_zc_unit_code_de")("ID") === df("UNITS_ID"), "left_outer")
      prejoin.withColumn("LOCALCODE", coalesce(concat(prejoin("VITAL_ENTRY_NAME"), lit("_"), prejoin("ENTRYNAME")),
        prejoin("VITAL_ENTRY_NAME"))).select("LOCALCODE").distinct()
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_vitals").join(dfs("cdr.map_observation"), Seq("LOCALCODE"), "inner")
  }

  map = Map(
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "LOCALDESC" -> mapFrom("LOCAL_NAME"),
    "LOCALNAME" -> mapFrom("LOCAL_NAME"),
    "LOCALUNITS" -> mapFrom("LOCALUNIT")
  )

  mapExceptions = Map(
    ("H984926_AS ENT", "LOCALCODE") -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("LOCALCODE_map"))))
  )
}

