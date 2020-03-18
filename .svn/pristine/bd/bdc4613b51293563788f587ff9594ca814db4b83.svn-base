package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ProcedureHmhistory_test(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hm_history", "cdr.map_custom_proc")

  columnSelect = Map(
        "hm_history" -> List("HM_TOPIC_ID", "HM_TYPE_C", "PAT_ID", "HM_HX_DATE"),
        "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID","MAPPEDVALUE")
      )

  beforeJoin = Map(


  "cdr.map_custom_proc "-> ((df: DataFrame) => {
    val fil = df.filter("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'HM_HISTORY'")
    fil.withColumnRenamed("LOCALCODE", "LOCALCODE2")
  }
  ))

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hm_history").join(dfs("cdr.map_custom_proc"),
        concat(lit(config(CLIENT_DS_ID)+"."), dfs("hm_history")("HM_TOPIC_ID")) === dfs("cdr.map_custom_proc")("LOCALCODE2"), "inner")

  }


  map = Map(
    "DATASRC" -> literal("hm_history"),
    "LOCALCODE" ->  mapFrom("HM_TOPIC_ID",nullIf=Seq(null),prefix=config(CLIENT_DS_ID)+"."),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE"-> mapFrom("MAPPEDVALUE")
  )

 }