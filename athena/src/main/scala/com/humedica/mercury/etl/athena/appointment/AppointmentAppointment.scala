package com.humedica.mercury.etl.athena.appointment

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class AppointmentAppointment(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("appointment:athena.util.UtilDedupedAppointment",
    "appointmentnote:athena.util.UtilDedupedAppointmentnote",
    "provider:athena.util.UtilDedupedProvider")

  columnSelect = Map(
    "appointment" -> List("APPOINTMENT_DATE", "APPOINTMENT_ID", "DEPARTMENT_ID", "PATIENT_ID", "APPOINTMENT_TYPE_ID",
      "PROVIDER_ID", "SCHEDULING_PROVIDER", "APPOINTMENT_STATUS"),
    "appointmentnote" -> List("NOTE", "APPOINTMENT_ID"),
    "provider" -> List("PROVIDER_ID", "SCHEDULING_NAME")
  )

  beforeJoin = Map(
    "appointment" -> includeIf("(appointment_status is null or appointment_status not in ('x - Cancelled', 'o - Open Slot'))" +
      " and appointment_date is not null and department_id is not null and patient_id is not null"),
    "provider" -> renameColumn("PROVIDER_ID", "PROVIDER_ID_prov")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("appointment")
      .join(dfs("appointmentnote"), Seq("APPOINTMENT_ID"), "left_outer")
      .join(dfs("provider"), dfs("appointment")("SCHEDULING_PROVIDER") === dfs("provider")("SCHEDULING_NAME"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("appointment"),
    "APPOINTMENTDATE" -> mapFrom("APPOINTMENT_DATE"),
    "APPOINTMENTID" -> mapFrom("APPOINTMENT_ID"),
    "LOCATIONID" -> mapFrom("DEPARTMENT_ID"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "LOCAL_APPT_TYPE" -> mapFrom("APPOINTMENT_TYPE_ID"),
    "PROVIDERID" -> cascadeFrom(Seq("PROVIDER_ID", "PROVIDER_ID_prov")),
    "APPOINTMENT_REASON" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("NOTE"), 1, 249))
    })
  )

}