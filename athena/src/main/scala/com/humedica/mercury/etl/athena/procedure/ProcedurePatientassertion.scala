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


class ProcedurePatientassertion(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientassertion")

      columnSelect = Map(
                   "patientassertion" -> List("PATIENT_ASSERTION_KEY", "PATIENT_ID", "PATIENT_ASSERTION_VALUE", "PATIENT_ASSERTION_KEY")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //where localcode mapped in cdr.map_custom_proc AND Patientid is not null AND patientassertion.deleted_datetime IS NULL


      map = Map(
        "LOCALCODE" -> mapFrom("PATIENT_ASSERTION_KEY"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "PROCEDUREDATE" -> mapFrom("PATIENT_ASSERTION_VALUE"),
        "LOCALNAME" -> mapFrom("PATIENT_ASSERTION_KEY"),
        "CODETYPE" -> todo("")      //TODO - to be coded
      )

 }