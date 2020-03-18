package com.humedica.mercury.etl.athena.clinicalencounter

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ClinicalencounterAppointment(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("appointment:athena.util.UtilDedupedAppointment")

  columnSelect = Map(
    "appointment" -> List("APPOINTMENT_DATE", "APPOINTMENT_ID", "PATIENT_ID", "DEPARTMENT_ID", "APPOINTMENT_TYPE_ID",
      "APPOINTMENT_STATUS", "APPOINTMENT_TYPE")
  )

  afterJoin = includeIf("appointment_date is not null and appointment_id is not null and patient_id is not null " +
    "and coalesce(appointment_status, 'X') not in ('x - Cancelled','o - Open Slot', 'f - Filled')")

  map = Map(
    "DATASRC" -> literal("appointment"),
    "ARRIVALTIME" -> mapFrom("APPOINTMENT_DATE"),
    "ENCOUNTERID" -> mapFrom("APPOINTMENT_ID", prefix = "a."),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "FACILITYID" -> mapFrom("DEPARTMENT_ID"),
    "LOCALPATIENTTYPE" -> cascadeFrom(Seq("APPOINTMENT_TYPE_ID", "APPOINTMENT_TYPE"), prefix = config(CLIENT_DS_ID) + ".")
  )

}