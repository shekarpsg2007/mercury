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


class ClinicalencounterClinicalencounter(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("appointment:athena.util.UtilDedupedAppointment",
    "clinicalencounter:athena.util.UtilDedupedClinicalEncounter")

  columnSelect = Map(
    "appointment" -> List("APPOINTMENT_TYPE_ID", "APPOINTMENT_TYPE", "APPOINTMENT_ID", "APPOINTMENT_STATUS"),
    "clinicalencounter" -> List("ENCOUNTER_DATE", "CLINICAL_ENCOUNTER_ID", "PATIENT_ID", "DEPARTMENT_ID",
      "CLINICAL_ENCOUNTERTYPE", "APPOINTMENT_ID")
  )

  beforeJoin = Map(
    "appointment" -> includeIf("coalesce(appointment_status, 'X') not in ('x - Cancelled','o - Open Slot', 'f - Filled')"),
    "clinicalencounter" -> includeIf("encounter_date is not null and clinical_encounter_id is not null and patient_id is not null")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinicalencounter")
      .join(dfs("appointment"), Seq("APPOINTMENT_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("clinicalencounter"),
    "ARRIVALTIME" -> mapFrom("ENCOUNTER_DATE"),
    "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "FACILITYID" -> mapFrom("DEPARTMENT_ID"),
    "LOCALPATIENTTYPE" -> cascadeFrom(Seq("APPOINTMENT_TYPE_ID", "APPOINTMENT_TYPE", "CLINICAL_ENCOUNTERTYPE"), prefix = config(CLIENT_DS_ID) + ".")
  )

}