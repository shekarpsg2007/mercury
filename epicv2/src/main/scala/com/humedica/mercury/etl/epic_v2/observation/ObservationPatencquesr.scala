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
class ObservationPatencquesr(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables=List("pat_enc_quesr", "cdr.zcm_obstype_code")


  beforeJoin = Map(
    "pat_enc_quesr" -> ((df: DataFrame) => {
      val fil = df.filter("PAT_ID is not null and CONTACT_DATE is not null")
      val groups1 = Window.partitionBy(fil("PAT_ENC_CSN_ID"), fil("QUEST_ID"), fil("QUEST_ANSWER")).orderBy(fil("CONTACT_DATE").desc_nulls_last)
      val addColumn = fil.withColumn("rn", row_number.over(groups1))
      addColumn.filter("rn = 1").drop("rn")
    }),
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'pat_enc_quesr' and obstype <> 'LABRESULT'")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("pat_enc_quesr")
      .join(dfs("cdr.zcm_obstype_code"), concat(lit(config(CLIENT_DS_ID)+"."), dfs("pat_enc_quesr")("QUEST_ID")) === dfs("cdr.zcm_obstype_code")("obscode"), "inner")
  }


  map = Map(
    "DATASRC" -> literal("pat_enc_quesr"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "OBSDATE" -> mapFrom("CONTACT_DATE"),
    "LOCALCODE" -> mapFrom("QUEST_ID", nullIf=Seq(null), prefix=config(CLIENT_DS_ID) + "."),
    "OBSRESULT" -> mapFrom("QUEST_ANSWER"),
    "LOCALRESULT" -> mapFrom("QUEST_ANSWER"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("QUEST_ID"), df("OBSDATE"), df("OBSTYPE")).orderBy(df("CONTACT_DATE").desc_nulls_last,df("ANS_CHANGE_INST_DTTM").desc_nulls_last)
    val addColumn = df.withColumn("obs_rn", row_number.over(groups))
    addColumn.filter("obs_rn =1")
  }

}