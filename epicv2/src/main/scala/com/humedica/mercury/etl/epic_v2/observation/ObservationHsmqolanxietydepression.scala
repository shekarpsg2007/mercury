package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants.GROUP
import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
  * Created by abendiganavale on 5/16/18.
  */
class ObservationHsmqolanxietydepression(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("identity_id","hsm_qol_anxiety_depression","cdr.zcm_obstype_code")

  columnSelect = Map(
    "identity_id" -> List("PAT_ID","IDENTITY_ID","IDENTITY_TYPE_ID","UPDATE_DATE"),
    "hsm_qol_anxiety_depression" -> List("ENTERPRISEID","VISITDATE","PHQ2_SCORE","PHQ9_SCORE"
      ,"PROMIS10_MENTAL_PAIN","PROMIS10_MENTAL_TSCORE","PROMIS10_PHYSICAL_TSCORE")
  )

  beforeJoin = Map(
    "identity_id" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val fil = df1.filter("PAT_ID is not null and IDENTITY_TYPE_ID = '1400'")
      val groups = Window.partitionBy(fil("IDENTITY_ID")).orderBy(fil("UPDATE_DATE").desc_nulls_last)
      val addColumn = fil.withColumn("rn_id", row_number.over(groups))
      addColumn.filter("rn_id = 1").drop("rn_id")
    }),
    "hsm_qol_anxiety_depression"-> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val promis_ttl = df1.withColumn("PROMIS_TTL", df1("PROMIS10_MENTAL_PAIN") + df1("PROMIS10_MENTAL_TSCORE") + df1("PROMIS10_PHYSICAL_TSCORE"))

      val fpiv = unpivot(
        Seq("PHQ2_SCORE", "PHQ9_SCORE", "PROMIS10_MENTAL_PAIN", "PROMIS10_MENTAL_TSCORE", "PROMIS10_PHYSICAL_TSCORE", "PROMIS_TTL"),
        Seq("PHQ2_SCORE", "PHQ9_SCORE", "PROMIS10_MENTAL_PAIN", "PROMIS10_MENTAL_TSCORE", "PROMIS10_PHYSICAL_TSCORE", "PROMIS_TOTAL" ), typeColumnName = "LOCAL_CD")
      fpiv("LOCAL_RES", promis_ttl)
    }),
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'hsm_qol_anxiety_depression' and GROUPID='"+config(GROUP)+"'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hsm_qol_anxiety_depression")
      .join(dfs("identity_id"), dfs("hsm_qol_anxiety_depression")("ENTERPRISEID") === dfs("identity_id")("IDENTITY_ID"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), dfs("hsm_qol_anxiety_depression")("LOCAL_CD") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hsm_qol_anxiety_depression"),
    "LOCALCODE" -> mapFrom("LOCAL_CD"),
    "OBSDATE" -> mapFrom("VISITDATE"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "LOCALRESULT" -> mapFrom("LOCAL_RES"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "OBSTYPE" -> mapFrom("OBSTYPE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"), df("OBSDATE"), df("LOCALRESULT"), df("OBSTYPE")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn =1 and PATIENTID IS NOT NULL AND OBSDATE IS NOT NULL AND LOCALCODE IS NOT NULL")
  }

}
