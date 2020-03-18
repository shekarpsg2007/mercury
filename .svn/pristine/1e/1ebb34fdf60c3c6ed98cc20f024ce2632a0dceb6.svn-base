package com.humedica.mercury.etl.cerner_v2.encounterreason

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame

/**
 * Auto-generated on 08/09/2018
 */


class EncounterreasonEncounter(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:cerner_v2.clinicalencounter.ClinicalencounterTemptable")

  columnSelect = Map(
    "temptable" -> List("ENCNTR_ID", "PERSON_ID", "ARRIVE_DT_TM", "REG_DT_TM", "REASON_FOR_VISIT")
  )

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      df.filter("rownumber = 1 and active_ind <> '0' and person_id is not null and reason_for_visit is not null")
    })
  )

  map = Map(
    "DATASRC" -> literal("encounter"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "REASONTIME" -> cascadeFrom(Seq("ARRIVE_DT_TM", "REG_DT_TM")),
    "LOCALREASONTEXT" -> mapFrom("REASON_FOR_VISIT")
  )

}