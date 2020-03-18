
package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._



/**
 * Created by swallis on 6/5/2018
 */


class ObservationNotesmarttextids(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("note_smarttext_ids", "hno_notes", "cdr.zcm_obstype_code") 
  
  columnSelect = Map(
    "note_smarttext_ids" -> List("NOTE_ID","SMARTTEXTS_ID"),
    "hno_notes" -> List("NOTE_ID","UPDATE_DATE","CONTACT_DATE","PAT_ID","PAT_ENC_CSN_ID"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "GROUPID")
  )

  beforeJoin = Map(
    //"hno_notes" -> renameColumn("NOTE_ID", "NOTE_ID_hno"),
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'note_smarttext_ids' and GROUPID='"+config(GROUP)+"'")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("note_smarttext_ids")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("OBSCODE") === concat(lit(config(CLIENT_DS_ID)+"."), dfs("note_smarttext_ids")("SMARTTEXTS_ID")), "inner")
      //.join(dfs("hno_notes"), dfs("note_smarttext_ids")("NOTE_ID") === dfs("hno_notes")("NOTE_ID_hno"), "inner")
      .join(dfs("hno_notes"), Seq("NOTE_ID"), "inner")  
  }

  afterJoin = (df: DataFrame) => {
    val df1 = df.filter("PAT_ID is not null and CONTACT_DATE is not null")
    val groups = Window.partitionBy(df1("NOTE_ID")).orderBy(df1("UPDATE_DATE").desc_nulls_last)
    val addColumn = df1.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }


  map = Map(
    "DATASRC" -> literal("note_smarttext_ids"),
    "LOCALCODE" -> mapFrom("SMARTTEXTS_ID", prefix = config(CLIENT_DS_ID) + "."),    
    "OBSDATE" -> mapFrom("CONTACT_DATE"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),    
    "LOCALRESULT" -> mapFrom("SMARTTEXTS_ID", prefix = config(CLIENT_DS_ID) + ".")
  )
  
}
