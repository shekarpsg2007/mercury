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


class ObservationSmartlistsused(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "smartlists_used",
    "cdr.zcm_obstype_code"
  )
  
  columnSelect = Map(
    "smartlists_used" -> List("SMARTLIST_USED_ID", "CONTACT_DATE", "PAT_ID", "PAT_ENC_CSN_ID"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "smartlists_used" -> ((df: DataFrame) => {
      val fil = df.filter("smartlist_used_id is not null and contact_date is not null and pat_id is not null")
      val groups = Window.partitionBy(fil("PAT_ID"), fil("PAT_ENC_CSN_ID"), fil("SMARTLIST_USED_ID"))
        .orderBy(fil("CONTACT_DATE").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("smartlists_used")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("GROUPID") === lit(config(GROUP))
          && dfs("cdr.zcm_obstype_code")("DATASRC") === lit("smartlists_used")
          && concat(lit(config(CLIENT_DS_ID) + "."),dfs("smartlists_used")("SMARTLIST_USED_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner"
      )
  }

  afterJoin = (df: DataFrame) => {
    df.filter("rn = 1")
      .drop("rn")
  }


  map = Map(
    "DATASRC" -> literal("smartlists_used"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "OBSDATE" -> mapFrom("CONTACT_DATE"),
    "LOCALCODE" -> mapFrom("SMARTLIST_USED_ID", prefix = config(CLIENT_DS_ID) + "."),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCALRESULT" -> mapFrom("SMARTLIST_USED_ID", prefix = config(CLIENT_DS_ID) + "."),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

}