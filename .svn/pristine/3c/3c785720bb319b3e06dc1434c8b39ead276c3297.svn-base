package com.humedica.mercury.etl.athena.observation

import com.humedica.mercury.etl.athena.util.UtilSplitTable
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame


class ObservationChartquestionnaire(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("chartQuestionnaire:athena.util.UtilDedupedChartQuestionnaire", "clinicalEncounter:athena.util.UtilDedupedClinicalEncounter", "pat:athena.util.UtilSplitPatient")

  columnSelect = Map(
    "chartQuestionnaire" -> List("OBSCODE", "SCORED_DATETIME", "OBSTYPE", "LOCALUNIT", "SCORING_STATUS", "SCORE", "OBSTYPE_STD_UNITS", "CLINICAL_ENCOUNTER_ID"),
    "clinicalEncounter" -> List("ENCOUNTER_DATE", "PATIENT_ID", "CLINICAL_ENCOUNTER_ID"),
    "pat" -> List("PATIENT_ID")
  )


  join = (dfs: Map[String, DataFrame]) => {
    val patJoinType = new UtilSplitTable(config).patprovJoinType
    dfs("chartQuestionnaire").join(dfs("clinicalEncounter"), Seq("CLINICAL_ENCOUNTER_ID"), "inner")
      .join(dfs("pat"), Seq("PATIENT_ID"), patJoinType)
  }

  map = Map(
    "DATASRC" -> literal("patientobgynhistory"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID", prefix = "a."),
    "LOCALCODE" -> mapFrom("OBSCODE"),
    "OBSDATE" -> cascadeFrom(Seq("SCORED_DATETIME", "ENCOUNTER_DATE")),
    "LOCALRESULT" -> mapFrom("SCORE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STATUSCODE" -> mapFrom("SCORING_STATUS"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    df.filter("PATIENTID is not null and LOCALRESULT is not null and OBSDATE is not null")
  }

}

// test
//  val ob = new ObservationChartquestionnaire(cfg); val obs = build(ob); obs.show ; obs.count;
