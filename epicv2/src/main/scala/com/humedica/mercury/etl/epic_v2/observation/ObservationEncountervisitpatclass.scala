package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
  * Created by abendiganavale on 4/27/18.
  */
class ObservationEncountervisitpatclass(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("encountervisit"
    ,"zh_pat_class"
    , "cdr.zcm_obstype_code")

  columnSelect = Map(
    "encountervisit" -> List("ADT_PAT_CLASS_C", "APPT_STATUS_C", "ENC_TYPE_C", "PAT_ENC_CSN_ID", "PAT_ID", "CHECKIN_TIME"
      ,"ED_ARRIVAL_TIME", "HOSP_ADMSN_TIME", "CONTACT_DATE", "EFFECTIVE_DATE_DT", "UPDATE_DATE","FILEID"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "LOCALUNIT", "GROUPID")
    //"zh_pat_class" -> List("NAME", "ADT_PAT_CLASS_C")
  )

  beforeJoin = Map(

    "encountervisit" -> ((df: DataFrame) => {
     // val fil = df.filter("(APPT_STATUS_C not in ('3','4','5') or APPT_STATUS_C IS NULL) and (ENC_TYPE_C <> '5' OR ENC_TYPE_C IS NULL) and (PAT_ENC_CSN_ID <> '-1' or PAT_ID <> '-1')")
      val groups =  Window.partitionBy(df("PAT_ENC_CSN_ID")).orderBy(df("UPDATE_DATE").desc,df("FILEID").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'encountervisit_pat_class' and GROUPID = '" + config(GROUP) + "'").drop("GROUPID")
    })
    //"zh_pat_class" -> renameColumn("ADT_PAT_CLASS_C", "ADT_PAT_CLASS_C_zh")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("encountervisit")
      .join(dfs("cdr.zcm_obstype_code"), concat(lit(config(CLIENT_DS_ID)+"."),dfs("encountervisit")("ADT_PAT_CLASS_C")) === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
      //.join(dfs("zh_pat_class"), dfs("encountervisit")("ADT_PAT_CLASS_C") === dfs("zh_pat_class")("ADT_PAT_CLASS_C_zh"))
  }

  map = Map(
    "DATASRC" -> literal("encountervisit_pat_class"),
    "LOCALCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ADT_PAT_CLASS_C").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID) + "."), df("ADT_PAT_CLASS_C"))).otherwise(null))
    }),
    "OBSDATE" -> cascadeFrom(Seq("CHECKIN_TIME","ED_ARRIVAL_TIME", "HOSP_ADMSN_TIME", "EFFECTIVE_DATE_DT", "CONTACT_DATE")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALRESULT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ADT_PAT_CLASS_C").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID) + "."), df("ADT_PAT_CLASS_C"))).otherwise(null))
    }),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT")
    //"LOCALOBSNAME" -> mapFrom("NAME")
  )

  afterMap = (df: DataFrame) => {
      df.filter("PATIENTID IS NOT NULL AND OBSDATE IS NOT NULL AND LOCALCODE IS NOT NULL AND (APPT_STATUS_C not in ('3','4','5') or APPT_STATUS_C IS NULL) and (ENC_TYPE_C <> '5' OR ENC_TYPE_C IS NULL) and " +
        "(ENCOUNTERID <> '-1' or PATIENTID <> '-1')").distinct()
  }

}
