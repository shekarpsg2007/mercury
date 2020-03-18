package com.humedica.mercury.delta.athena_generated.labresult;

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class LabresultVitalsign(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("vitalsign",
                   "clinicalencounter")

      columnSelect = Map(
                   "vitalsign" -> List("ENCOUNTER_DATA_ID", "HUM_KEY", "CLINICAL_ENCOUNTER_ID", "HUM_KEY", "HUM_KEY",
                               "DB_UNIT", "HUM_VALUE"),
                   "clinicalencounter" -> List("PATIENT_ID", "DEPARTMENT_ID", "ENCOUNTER_DATE")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where LocalResultCode (vitalsign.hum_key) is mapped to LABRESULT in MAP_OBSERVATION (map_observation.Cui = 'CH002048') and vitalsign.deleted_datetime is null and clinicalencounter.deleted_datetime is null.


      map = Map(
        "LABRESULTID" -> mapFrom("ENCOUNTER_DATA_ID"),
        "LOCALCODE" -> mapFrom("HUM_KEY"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "FACILITYID" -> mapFrom("DEPARTMENT_ID"),
        "LOCALNAME" -> mapFrom("HUM_KEY"),
        "LOCALTESTNAME" -> mapFrom("HUM_KEY"),
        "LOCALUNITS" -> mapFrom("DB_UNIT"),
        "DATECOLLECTED" -> mapFrom("ENCOUNTER_DATE"),
        "LOCALRESULT" -> mapFrom("HUM_VALUE")
      )

 }