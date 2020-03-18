package com.humedica.mercury.etl.asent.labmapperdict

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame

class LabmapperdictLabdictionary(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_qo_de")

  beforeJoin = Map(
    "as_flowsheet" -> ((df: DataFrame) => {
      df.filter("ENTRYNAME is not null").select("ENTRYNAME").distinct()
    })
  )

  join = noJoin()

  map = Map(
    "LOCALCODE" -> mapFrom("ENTRYNAME"),
    "LOCALNAME" -> mapFrom("ENTRYNAME")
  )
}
