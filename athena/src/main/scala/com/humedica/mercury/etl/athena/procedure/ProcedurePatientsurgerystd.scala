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


class ProcedurePatientsurgerystd(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientsurgery")

      columnSelect = Map(
                   "patientsurgery" -> List("PROCEDURE_CODE", "PATIENT_ID", "SURGERY_DATETIME", "CLINICAL_ENCOUNTER_ID", "PROCEDURE_CODE",
                               "PROCEDURE_CODE")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //localcode (patientsurgery.procedure_code) is not null andpatientsurgery.deleted_datetime is null and (patientsurgery.deactivated_datetime is null or (patientsurgery.deactivated_datetime is not null AND patientsurgery.reactivated_datetime is not null)


      map = Map(
        "LOCALCODE" -> mapFrom("PROCEDURE_CODE"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "PROCEDUREDATE" -> mapFrom("SURGERY_DATETIME"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "MAPPEDCODE" -> mapFrom("PROCEDURE_CODE"),
        "CODETYPE" -> mapFrom("PROCEDURE_CODE")
      )

 }