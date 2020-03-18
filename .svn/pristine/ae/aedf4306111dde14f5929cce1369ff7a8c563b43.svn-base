package com.humedica.mercury.etl.epic_v2.labmapperdict

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame

/**
 * Auto-generated on 02/02/2017
 * 2/2/17 - hjl - additions
 */

class LabmapperdictLabmapper(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_lab_data_type",
    "zh_clarity_comp")

  columnSelect = Map(
    "zh_lab_data_type" -> List("LAB_DATA_TYPE_C", "NAME"),
    "zh_clarity_comp" -> List("LAB_DATA_TYPE_C", "COMPONENT_ID", "NAME", "COMMON_NAME")
  )

  beforeJoin = Map(
    "zh_clarity_comp" -> ((df: DataFrame) => {
      df.filter("component_id <> '-1' and component_id is not null")
    }),
    "zh_lab_data_type" -> ((df: DataFrame) => {
      df.withColumnRenamed("NAME", "NAME_ldt")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_clarity_comp")
      .join(dfs("zh_lab_data_type"), Seq("LAB_DATA_TYPE_C"), "left_outer")
  }

  map = Map(
    "LOCALCODE" -> mapFrom("COMPONENT_ID"),
    "LOCALDATATYPE" -> mapFrom("NAME_ldt"),
    "LOCALDESC" -> mapFrom("NAME"),
    "LOCALNAME" -> mapFrom("COMMON_NAME")
  )

}

// val l = new LabmapperdictLabmapper(cfg) ; val ld = build(l) ; ld.show
