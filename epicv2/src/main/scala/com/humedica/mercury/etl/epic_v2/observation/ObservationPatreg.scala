package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Engine
import scala.collection.JavaConverters._

/**
  * Created by abendiganavale on 8/21/18.
  */
class ObservationPatreg(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patreg", "cdr.zcm_obstype_code")

  columnSelect = Map(
    "patreg" -> List("PAT_ID", "UPDATE_DATE", "FILEID", "ADV_DIRECTIVE_DATE", "ADVANCED_DIR_YN"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "GROUPID","LOCALUNIT")
  )

  beforeJoin = Map(
    "patreg" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PAT_ID")).orderBy(df("UPDATE_DATE").desc_nulls_last, df("FILEID").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 AND PAT_ID is not null and ADVANCED_DIR_YN is not null").drop("rn")
    }),
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'patreg' and GROUPID='" + config(GROUP) + "'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patreg")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("OBSCODE") === lit("ADVANCED_DIR_YN"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("patreg"),
    "LOCALCODE" -> literal("ADVANCED_DIR_YN"),
    "OBSDATE" -> cascadeFrom(Seq("ADV_DIRECTIVE_DATE","UPDATE_DATE")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "LOCALRESULT" -> mapFrom("ADVANCED_DIR_YN"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "OBSTYPE" -> mapFrom("OBSTYPE")
  )

  afterMap = (df:DataFrame) => {
    val cols = Engine.schema.getStringList("Observation").asScala.map(_.split("-")(0).toUpperCase())
    df.filter("OBSDATE IS NOT NULL").select(cols.map(col): _*).distinct
  }

}