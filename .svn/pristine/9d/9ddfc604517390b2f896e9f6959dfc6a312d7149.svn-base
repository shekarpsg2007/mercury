package com.humedica.mercury.etl.epic_v2.labmapperdict

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame

/**
  * Created by abendiganavale on 3/19/18.
  */
class LabmapperdictO2sats(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("cdr.map_observation",
    "zh_lab_dict:epic_v2.labmapperdict.LabmapperdictLabmapper")

  columnSelect = Map(
    "cdr.map_observation" -> List("GROUPID", "LOCALCODE","LOCAL_NAME","LOCALUNIT"),
    "zh_lab_dict" -> List("LOCALCODE")
  )

  beforeJoin = Map(
    "cdr.map_observation" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and obstype = 'LABRESULT'")
        .withColumnRenamed("LOCALCODE", "LOCALCODE_map")
        .withColumnRenamed("LOCAL_NAME", "LOCAL_NAME_map")
        .withColumnRenamed("LOCALUNIT", "LOCALUNIT_map")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("cdr.map_observation")
      .join(dfs("zh_lab_dict"), dfs("cdr.map_observation")("LOCALCODE_map") === dfs("zh_lab_dict")("LOCALCODE"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    df.filter("LOCALCODE is null")
  }

  map = Map(
    "LOCALCODE" -> mapFrom("LOCALCODE_map"),
    "LOCALDESC" -> mapFrom("LOCAL_NAME_map"),
    "LOCALNAME" -> mapFrom("LOCAL_NAME_map"),
    "LOCALUNITS" -> mapFrom("LOCALUNIT_map")
  )

}

// val x = new  LabmapperdictO2sats(cfg); val ld = build(x) ; ld.show ; ld.count
