package com.humedica.mercury.etl.asent.appointmentex

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._


class AppointmentexAsappointment(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_appointment")

  columnSelect = Map("as_appointment" -> List("STARTDTTM", "ID", "LOCATIONDE", "PATIENT_MRN", "APPOINTMENTTYPEDE", "RESOURCEDE", "REASON", "APPOINTMENTSTATUSDE", "LASTCHANGEDTTM", "ENCOUNTERID"))

  afterJoin = (df: DataFrame) => {
    val fil = df.filter("APPOINTMENTSTATUSDE in ('3', '5', '9')")
    val groups = Window.partitionBy(fil("ID")).orderBy(fil("LASTCHANGEDTTM").desc)
    fil.withColumn("rn", row_number.over(groups))
      .withColumn("REASON_final", substring(fil("REASON"), 1, 249))
      .filter("rn=1")
  }

  map = Map(
    "DATASRC" -> literal("as_appointment"),
    "APPOINTMENTDATE" -> mapFrom("STARTDTTM"),
    "APPOINTMENTID" -> mapFrom("ID"),
    "LOCATIONID" -> mapFrom("LOCATIONDE"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "LOCAL_APPT_TYPE" -> mapFrom("APPOINTMENTTYPEDE"),
    "PROVIDERID" -> mapFrom("RESOURCEDE", prefix = "de."),
    "APPOINTMENT_REASON" -> mapFrom("REASON_final")
  )
}


