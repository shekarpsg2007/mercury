package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by cdivakaran on 5/31/17.
  */
class ObservationTemptablepatass(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patassessment",
    "cdr.zcm_obstype_code", "cdr.map_custom_proc")


  cacheMe = true



  columns=List("FILEID", "ENTRY_TIME", "ENTRY_USER_ID", "FLO_DIS_NAME", "FLO_MEAS_ID", "FLO_MEAS_NAME", "FLO_MED_UNIT_C", "FLO_ROW_NAME", "FSD_ID", "INTAKE_TYPE_C", "LINE", "MAX_VALUE",
  "MEAS_COMMENT", "MEAS_VALUE", "MIN_VALUE", "OCCURANCE", "OUTPUT_TYPE_C", "PAT_ENC_CSN_ID", "PAT_ID", "RECORDED_TIME", "TAKEN_USER_ID", "UNIT", "VALUE_TYPE_NAME")

  beforeJoin = Map(
    "patassessment" -> ((df1: DataFrame) => {
      val df = df1.withColumn("MEAS_VALUE_new", regexp_replace(df1("MEAS_VALUE"),"[^\\p{Print}]",""))
        .drop("MEAS_VALUE")
        .withColumnRenamed("MEAS_VALUE_new","MEAS_VALUE")
      val zcm = table("cdr.zcm_obstype_code").filter("DATASRC = 'patass' and GROUPID = '"+config(GROUP)+"'").select("OBSCODE")
      val macp = table("cdr.map_custom_proc").filter("DATASRC = 'PATASSESSMENT'and GROUPID = '"+config(GROUP)+"'").select("LOCALCODE").withColumnRenamed("LOCALCODE", "OBSCODE")
      val un = zcm.unionAll(macp)
      val unz = df.join(un, concat(lit(config(CLIENT_DS_ID)+"."), df("FLO_MEAS_ID")) === un("OBSCODE"), "inner")
      val zcm1 = table("cdr.zcm_obstype_code").filter("DATASRC = 'patass' and GROUPID = '"+config(GROUP)+"'").select("OBSCODE")
      val uny = df.join(zcm1, concat(lit(config(CLIENT_DS_ID)+".pa."), df("FLO_MEAS_ID")) === zcm1("OBSCODE"))
      unz.unionAll(uny).drop("OBSCODE")
    })
  )

  map=Map(
    "FILEID" -> mapFrom("FILEID")
  )


}
