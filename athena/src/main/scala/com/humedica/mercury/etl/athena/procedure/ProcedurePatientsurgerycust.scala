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


class ProcedurePatientsurgerycust(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientsurgery")

      columnSelect = Map(
                   "patientsurgery" -> List("HUM_PROCEDURE", "PATIENT_ID", "SURGERY_DATETIME", "HUM_PROCEDURE", "CLINICAL_ENCOUNTER_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where Localcode is mapped in CDR.Map_Custom_Proc and patientsurgery.deleted_datetime is null and (patientsurgery.deactivated_datetime is null or (patientsurgery.deactivated_datetime is not null AND patientsurgery.reactivated_datetime is not null)


      map = Map(
        "LOCALCODE" -> mapFrom("HUM_PROCEDURE"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "PROCEDUREDATE" -> mapFrom("SURGERY_DATETIME"),
        "LOCALNAME" -> mapFrom("HUM_PROCEDURE"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "CODETYPE" -> todo("")      //TODO - to be coded
      )

 }