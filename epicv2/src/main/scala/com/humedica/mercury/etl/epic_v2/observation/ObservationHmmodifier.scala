package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/27/2017
 */


class ObservationHmmodifier(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hm_modifier",
                "cdr.zcm_obstype_code")


  beforeJoin = Map(
    "hm_modifier" -> includeIf("PAT_ID IS NOT NULL AND UPDATE_DATE IS NOT NULL"),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
    df.filter("DATASRC = 'hm_modifier' and GROUPID = '"+config(GROUP)+"'").drop("GROUPID")
  })
    )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hm_modifier")
      .join(dfs("cdr.zcm_obstype_code"),concat(lit(config(CLIENT_DS_ID)+"."),dfs("hm_modifier")("MODIFIER_NUM_C")) === dfs("cdr.zcm_obstype_code")("OBSCODE"))
  }



      map = Map(
        "DATASRC" -> literal("hm_modifier"),
        "LOCALCODE" -> mapFrom("MODIFIER_NUM_C", prefix=config(CLIENT_DS_ID)+"."),
        "OBSDATE" -> mapFrom("UPDATE_DATE"),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "LOCALRESULT" -> mapFrom("MODIFIER_NUM_C", prefix=config(CLIENT_DS_ID)+"."),
        "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
        "OBSTYPE" -> mapFrom("OBSTYPE")
  )


  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"),df("LOCALCODE")).orderBy(df("OBSDATE").desc, df("LINE").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

 }