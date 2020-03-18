package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */

class ObservationPtgoalsinfo(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "pt_goals_info",
    "cdr.zcm_obstype_code"
  )

  columnSelect = Map(
    "pt_goals_info" -> List("PAT_ID", "GOAL_TEMPLATE_ID", "CREATE_INST_DTTM")
  )

  beforeJoin = Map(
    "pt_goals_info" -> ((df: DataFrame) => {
      val fil = df.filter("PAT_ID is not null and GOAL_TEMPLATE_ID is not null and CREATE_INST_DTTM is not null")
      val groups = Window.partitionBy(fil("PAT_ID"), fil("GOAL_TEMPLATE_ID")).orderBy(fil("CREATE_INST_DTTM").desc)
      fil.withColumn("rw", row_number.over(groups)).filter("rw =1")
    }),
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'pt_goals_info'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("pt_goals_info")
      .join(
        dfs("cdr.zcm_obstype_code"),
        dfs("cdr.zcm_obstype_code")("GROUPID") === config(GROUP) &&
          concat(lit(config(CLIENT_DS_ID) + "."),dfs("pt_goals_info")("GOAL_TEMPLATE_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE"),
        "inner"
      )
  }

  map = Map(
    "DATASRC" -> literal("pt_goals_info"),
    "LOCALCODE" -> mapFrom("GOAL_TEMPLATE_ID", prefix = config(CLIENT_DS_ID) + "."),
    "OBSDATE" -> mapFrom("CREATE_INST_DTTM"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "LOCALRESULT" -> mapFrom("GOAL_TEMPLATE_ID", prefix = config(CLIENT_DS_ID) + "."),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

}
