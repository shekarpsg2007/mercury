package com.humedica.mercury.etl.athena.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ObservationVitalsign(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("vitalsign",
                   "clinicalencounter")

      columnSelect = Map(
                   "vitalsign" -> List("HUM_KEY", "CLINICAL_ENCOUNTER_ID", "HUM_VALUE"),
                   "clinicalencounter" -> List("ENCOUNTER_DATE", "ENCOUNTER_DATE", "PATIENT_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where localobscode in MAP_OBSERVATION.localcode and map_observaton.cui <> 'CH002048' and vitalsign.deleted_datetime is null and clinicalencounter.deleted_datetime is null.


      map = Map(
        "LOCALCODE" -> mapFrom("HUM_KEY"),
        "OBSDATE" -> mapFrom("ENCOUNTER_DATE"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "LOCALRESULT" -> mapFrom("HUM_VALUE")
      )

      mapExceptions = Map(
        ("H984442_ATHENA", "OBSDATE") -> cascadeFrom(Seq("ENCOUNTER_DATE", "CREATED_DATETIME"))
      )

 }