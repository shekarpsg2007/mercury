package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Created by cdivakaran on 7/21/17.
  */
class ObservationClqanswerqa(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables=List("cl_qanswer_qa", "cdr.zcm_obstype_code", "pat_enc_form_ans")

  columnSelect = Map(
    "cl_qanswer_qa" -> List("QUEST_ID", "ANSWER_ID", "QUEST_ANSWER"),
    "pat_enc_form_ans" -> List("PAT_ID", "PAT_ENC_CSN_ID", "CONTACT_DATE", "QF_HQA_ID", "QF_STAT_CHNG_INST"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSTYPE", "OBSCODE", "LOCALUNIT", "OBSTYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'cl_qanswer_qa' and obstype <> 'LABRESULT'")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("cl_qanswer_qa")
      .join(dfs("cdr.zcm_obstype_code"), concat(lit(config(CLIENT_DS_ID)+"."), dfs("cl_qanswer_qa")("QUEST_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
      .join(dfs("pat_enc_form_ans"), dfs("cl_qanswer_qa")("ANSWER_ID") === dfs("pat_enc_form_ans")("QF_HQA_ID"), "inner")
  }

  afterJoin = includeIf("PAT_ID is not null and CONTACT_DATE is not null and QUEST_ID is not null")


  map = Map(
    "DATASRC" -> literal("cl_qanswer_qa"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "OBSDATE" -> mapFrom("CONTACT_DATE"),
    "LOCALCODE" -> mapFrom("QUEST_ID", prefix=config(CLIENT_DS_ID) + "."),
    "LOCALRESULT" -> mapFrom("QUEST_ANSWER"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("QUEST_ID"), df("OBSDATE"), df("OBSTYPE")).orderBy(df("QF_STAT_CHNG_INST").desc_nulls_last)
    val addColumn = df.withColumn("obs_rn", row_number.over(groups))
    addColumn.filter("obs_rn =1")
  }

}
