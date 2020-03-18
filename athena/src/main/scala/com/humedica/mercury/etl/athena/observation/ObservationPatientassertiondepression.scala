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


class ObservationPatientassertiondepression(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientassertion")

      columnSelect = Map(
                   "patientassertion" -> List("PATIENT_ASSERTION_KEY", "CREATED_DATETIME", "PATIENT_ID", "PATIENT_ASSERTION_VALUE")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //where localcode = 'ASSERTIONVALUE_DEPRESSIONASSESSMENTANDPLAN' AND Patientid is not null AND patientassertion.deleted_datetime IS NULL


      map = Map(
        "LOCALCODE" -> mapFrom("PATIENT_ASSERTION_KEY"),
        "OBSDATE" -> mapFrom("CREATED_DATETIME"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "LOCALRESULT" -> mapFrom("PATIENT_ASSERTION_VALUE")
      )

 }