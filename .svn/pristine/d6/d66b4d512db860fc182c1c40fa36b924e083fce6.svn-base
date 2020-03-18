package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by abendiganavale on 8/14/18.
  */
class ObservationPatencconcept(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("pat_enc_concept", "cdr.zcm_obstype_code")

  columnSelect = Map(
    "pat_enc_concept" -> List("PAT_ID", "CONCEPT_ID", "CONCEPT_VALUE", "CUR_VALUE_DATETIME", "LINE", "PAT_ENC_CSN_ID"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "GROUPID")
  )

  beforeJoin = Map(
    "pat_enc_concept" -> ((df: DataFrame) => {
      df.filter("PAT_ID is not null and CONCEPT_ID is not null and CUR_VALUE_DATETIME is not null")
    }),
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'pat_enc_concept' and GROUPID='" + config(GROUP) + "'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("pat_enc_concept")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("OBSCODE") === concat_ws("", lit(config(CLIENT_DS_ID)+"."), dfs("pat_enc_concept")("CONCEPT_ID")), "inner")
  }

  map = Map(
    "DATASRC" -> literal("pat_enc_concept"),
    "LOCALCODE" -> mapFrom("CONCEPT_ID", prefix = config(CLIENT_DS_ID) + "."),
    "OBSDATE" -> mapFrom("CUR_VALUE_DATETIME"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALRESULT" -> mapFrom("CONCEPT_VALUE"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "OBSTYPE" -> mapFrom("OBSTYPE")
  )

  afterMap = (df:DataFrame) => {
    df.distinct()
  }

}