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


class ObservationClinicalresultobservation(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("clinicalresultobservation",
                   "clinicalresult",
                   "document")

      columnSelect = Map(
                   "clinicalresultobservation" -> List("OBSERVATION_IDENTIFIER_TEXT", "HUM_RESULT"),
                   "clinicalresult" -> List("RESULT_STATUS"),
                   "document" -> List("OBSERVATION_DATETIME", "PATIENT_ID", "DEPARTMENT_ID", "CLINICAL_ENCOUNTER_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where Local Obscode is in MAP_OBSERVATION.localcode AND Map_Observation.cui <> 'CH002048' (LABRESULT) AND ClinicalResultObservation.deleted_datetime is null AND ClinicalResult.deleted_datetime is null AND Document.deleted_Datetime is null


      map = Map(
        "LOCALCODE" -> mapFrom("OBSERVATION_IDENTIFIER_TEXT"),
        "OBSDATE" -> cascadeFrom(Seq("OBSERVATION_DATETIME", "CREATED_DATETIME")),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "FACILITYID" -> mapFrom("DEPARTMENT_ID"),
        "STATUSCODE" -> mapFrom("RESULT_STATUS"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "LOCALRESULT" -> mapFrom("HUM_RESULT")
      )

 }