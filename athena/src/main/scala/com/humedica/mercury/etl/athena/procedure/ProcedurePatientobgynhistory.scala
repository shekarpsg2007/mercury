package com.humedica.mercury.etl.athena.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ProcedurePatientobgynhistory(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientobgynhistory")

      columnSelect = Map(
                   "patientobgynhistory" -> List("OBGYN_HISTORY_QUESTION", "PATIENT_ID", "OBGYN_HISTORY_ANSWER", "OBGYN_HISTORY_QUESTION")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //ALL DEDUPE CRITERIA APPLIED BEFORE INCLUSION CRITIERA!Include where patientobgynhistory.deleted_datetime is null


      map = Map(
        "LOCALCODE" -> todo("OBGYN_HISTORY_QUESTION"),      //TODO - to be coded
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "PROCEDUREDATE" -> mapFrom("OBGYN_HISTORY_ANSWER"),
        "LOCALNAME" -> mapFrom("OBGYN_HISTORY_QUESTION")
      )

 }