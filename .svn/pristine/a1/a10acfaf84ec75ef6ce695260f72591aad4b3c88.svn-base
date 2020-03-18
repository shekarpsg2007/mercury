package com.humedica.mercury.etl.asent.labmapperdict

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame

class LabmapperdictFlowsheet(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_flowsheet")

  beforeJoin = Map(
    "as_flowsheet" -> ((df: DataFrame) => {
      df.filter("flowsheetid = '8'")
    })
  )

  join = noJoin()

  map = Map(
    "LOCALCODE" -> mapFrom("RESULTNAME"),
    "LOCALNAME" -> mapFrom("RESULTNAME")
  )

}
