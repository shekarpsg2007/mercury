package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._

/**
  * Auto-generated on 02/01/2017
  */



class ObservationPatientfyiflags(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient_fyi_flags" ,"cdr.zcm_obstype_code")

  columnSelect = Map(
    "patient_fyi_flags" -> List("PATIENT_ID", "ACCT_NOTE_INSTANT", "PAT_FLAG_TYPE_C", "NOTE_ID", "LAST_UPDATE_INST", "ACTIVE_C")
  )

  beforeJoin = Map(
    "patient_fyi_flags" -> ((df: DataFrame) => {
      val fil = df.filter("pat_flag_type_c is not null and acct_note_instant is not null and patient_id is not null")
      val groups = Window.partitionBy(fil("PATIENT_ID"),fil("NOTE_ID"))
        .orderBy(fil("LAST_UPDATE_INST"),fil("ACCT_NOTE_INSTANT").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1 and ACTIVE_C = '1'")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'patient_fyi_flags' and GROUPID = '" + config(GROUPID) + "'")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patient_fyi_flags")
      .join(dfs("cdr.zcm_obstype_code"), (concat(lit(config(CLIENT_DS_ID) + "."), dfs("patient_fyi_flags")("PAT_FLAG_TYPE_C"))) === dfs("cdr.zcm_obstype_code")("OBSCODE"))
  }

  map = Map(
    "DATASRC" -> literal("patient_fyi_flags"),
    "LOCALCODE" -> mapFrom("PAT_FLAG_TYPE_C",prefix = config(CLIENT_DS_ID) + "."),
    "OBSDATE" -> mapFrom("ACCT_NOTE_INSTANT"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "LOCALRESULT" -> mapFrom("PAT_FLAG_TYPE_C",prefix = config(CLIENT_DS_ID) + "."),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT")
  )

}