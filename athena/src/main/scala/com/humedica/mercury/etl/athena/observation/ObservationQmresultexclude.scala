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


class ObservationQmresultexclude(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("qmresult")

      columnSelect = Map(
                   "qmresult" -> List("P4P_MEASURE", "SATISFIED_DATE", "PATIENT_ID", "RESULT_STATUS")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where LocalObscode in Map_Observation and map_observaton.cui <> 'CH002048' (LABRESULT) and qmresult.satisfied_date IS NOT NULL and qmresult.exclusion_reason IS NOT NULL


      map = Map(
        "LOCALCODE" -> mapFrom("P4P_MEASURE"),
        "OBSDATE" -> mapFrom("SATISFIED_DATE"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "LOCALRESULT" -> cascadeFrom(Seq("RESULT_STATUS", "P4P_MEASURE", "EXCLUSION_REASON"))
      )

 }