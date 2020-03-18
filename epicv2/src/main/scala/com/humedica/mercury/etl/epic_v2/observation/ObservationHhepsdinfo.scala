package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._


class ObservationHhepsdinfo(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hh_epsd_info", "hh_pat_cert_pr", "cdr.zcm_obstype_code")

  columnSelect = Map(//ADDED NEW COL CONTACT_DATE
    "hh_epsd_info" -> List("HH_EPISODE_TYPE_C", "CARE_PLAN_ID", "HOSPICE_ELEC_DT", "EPISODE_DELETED_C", "HOSPICE_ELEC_DT", "HOSPICE_REV_DT"),
    "hh_pat_cert_pr" -> List("CARE_PLAN_ID", "PAT_ID","CONTACT_DATE"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "GROUPID", "OBSTYPE", "OBSCODE", "LOCALUNIT")
  )

  beforeJoin = Map(
    "hh_epsd_info" -> includeIf("HH_EPISODE_TYPE_C IS NOT NULL AND CARE_PLAN_ID IS NOT NULL AND CARE_PLAN_ID <> '-1' AND HOSPICE_ELEC_DT IS NOT NULL AND HOSPICE_ELEC_DT <> '-1'"),
    "hh_pat_cert_pr" -> includeIf("CARE_PLAN_ID IS NOT NULL AND CARE_PLAN_ID <> '-1' AND PAT_ID IS NOT NULL"),
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'hh_epsd_info' AND GROUPID='"+config(GROUP)+"'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hh_epsd_info")
      .join(dfs("hh_pat_cert_pr"),Seq("CARE_PLAN_ID"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), concat(lit(config(CLIENT_DS_ID) + "."),dfs("hh_epsd_info")("HH_EPISODE_TYPE_C")) === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")

  }

  afterJoin = (df:DataFrame) => {
    val groups = Window.partitionBy(df("PAT_ID"), df("CARE_PLAN_ID"), df("HOSPICE_ELEC_DT"), df("HOSPICE_REV_DT")).orderBy(df("CONTACT_DATE").desc)
    val addColumn1 = df.withColumn("rn", row_number.over(groups))
    val addColumn2 = addColumn1.filter("rn = 1 and (EPISODE_DELETED_C IS NULL OR EPISODE_DELETED_C = '-1')").drop("rn")
    val addColumn3 = addColumn2.withColumn("LOCALCODE",concat(lit(config(CLIENT_DS_ID) + "."), addColumn2("HH_EPISODE_TYPE_C")) )
    val fpiv = unpivot(
      Seq("HOSPICE_ELEC_DT", "HOSPICE_REV_DT"),types = Seq("HOSPICE_ELEC_DT", "HOSPICE_REV_DT"),typeColumnName = "PIV_COL")
    val piv = fpiv("OBSDATE", addColumn3)
    piv.withColumn("LOCALRESULT", 
      when(lit("HOSPICE_ELEC_DT") === piv("PIV_COL"), concat(piv("LOCALCODE"), lit(".elec")))
      .when(lit("HOSPICE_REV_DT") === piv("PIV_COL"), concat(piv("LOCALCODE"), lit(".rev"))).otherwise(null))
  }


  map = Map(
    "DATASRC" -> literal("hh_epsd_info"),
      "LOCALCODE" -> mapFrom("HH_EPISODE_TYPE_C", prefix = config(CLIENT_DS_ID) + "."),
      "OBSTYPE" -> mapFrom("OBSTYPE"),
      "OBSDATE" -> mapFrom("OBSDATE"),
      "PATIENTID" -> mapFrom("PAT_ID"),
     "LOCALRESULT" -> mapFrom("LOCALRESULT"),
      "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT") )


 }