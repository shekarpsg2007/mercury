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

class ObservationEncountervisitgestage(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("encountervisit",
    "cdr.zcm_obstype_code")

  beforeJoin = Map(
    "encountervisit" -> ((df: DataFrame) => {
      val fil = df.filter("APPT_STATUS_C not in ('3','4','5') and ENC_TYPE_C <> '5' and (PAT_ENC_CSN_ID <> '-1' or PAT_ID <> '-1') and CONTACT_DATE is not null and PAT_ID is not null")
      val groups = Window.partitionBy(fil("PAT_ID"), fil("CONTACT_DATE"), fil("LMP_DATE")).orderBy(fil("UPDATE_DATE").desc_nulls_last)
          fil.withColumn("ev_rw", row_number.over(groups))
             .filter("ev_rw=1")
            .withColumn("LOCALCODE", lit("GEST_AGE_LMP"))
            .withColumn("OBSDATE", coalesce(substring(fil("CHECKIN_TIME"),1,10), substring(fil("ED_ARRIVAL_TIME"),1,10), substring(fil("HOSP_ADMSN_TIME"),1,10), substring(fil("EFFECTIVE_DATE_DT"),1,10), substring(fil("CONTACT_DATE"),1,10)))
            .withColumn("LMP_DATE_dt",substring(fil("LMP_DATE"),1,10))
    }),
       "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
          df.filter("DATASRC = 'encountervisit_gestage' and GROUPID='"+ config(GROUP) +"'")
     })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("encountervisit")
      .join(dfs("cdr.zcm_obstype_code"), dfs("encountervisit")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("encountervisit_gestage"),
    //"OBSDATE" -> cascadeFrom(Seq("CHECKIN_TIME","ED_ARRIVAL_TIME","HOSP_ADMSN_TIME","EFFECTIVE_DATE_DT","CONTACT_DATE")),
    "OBSDATE" -> mapFrom("OBSDATE"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALCODE " -> mapFrom("LOCALCODE"),
    //"LOCALRESULT" -> ((col: String, df: DataFrame) => {
    //  df.withColumn(col, datediff(coalesce(df("CHECKIN_TIME"), df("ED_ARRIVAL_TIME"), df("HOSP_ADMSN_TIME"), df("EFFECTIVE_DATE_DT"), df("CONTACT_DATE")), df("LMP_DATE")))
    //}) ,
    "LOCALRESULT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, datediff(df("OBSDATE"), df("LMP_DATE_dt")))
    }),
     "OBSTYPE" -> mapFrom("OBSTYPE"),
     "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
     "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    df.filter("LOCALRESULT is not null")
  }

}

// test
// var gs = new ObservationEncountervisitgestage(cfg) ; val ogs = build(gs); ogs.show(false) ; ogs.count()
