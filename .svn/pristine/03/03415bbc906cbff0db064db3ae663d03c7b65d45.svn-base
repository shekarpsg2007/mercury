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


class ObservationPatientsocialhistory(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientsocialhistory")

      columnSelect = Map(
                   "patientsocialhistory" -> List("SOCIAL_HISTORY_KEY", "PATIENT_ID", "SOCIAL_HISTORY_ANSWER")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //Include where PatientSocialHistory.Social_History_Key (local obscode) is in Map_observation.localcode AND Map_observation.cui <> 'CH002048' (LABRESULT) AND PatientSocialHistory.Deleted_datetime is null


      map = Map(
        "LOCALCODE" -> mapFrom("SOCIAL_HISTORY_KEY"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "LOCALRESULT" -> mapFrom("SOCIAL_HISTORY_ANSWER")
      )

 }