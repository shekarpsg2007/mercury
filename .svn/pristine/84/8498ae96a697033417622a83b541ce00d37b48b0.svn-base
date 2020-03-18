package com.humedica.mercury.etl.fdr.appointment

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._


class AppointmentAppointments(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List("Appointment:"+config("EMR")+"@Appointment", "mpitemp:fdr.mpi.MpiPatient")

    columns = List("CLIENT_DS_ID", "MPI", "APPOINTMENT_DTM", "PROV_ID", "LOCATION", "APPOINTMENT_TYPE_CUI", "REASON")

    join = (dfs: Map[String, DataFrame]) => {
      dfs("Appointment")
        .join(dfs("mpitemp"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
    }



    map = Map(
      "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
      "MPI" -> mapFrom("MPI"),
      "APPOINTMENT_DTM" -> mapFrom("APPOINTMENTDATE"),
      "PROV_ID" -> mapFrom("PROVIDERID"),
      "LOCATION" -> mapFrom("LOCATIONID"),
      "APPOINTMENT_TYPE_CUI" -> literal("CH999999"),
      "REASON" -> mapFrom("APPOINTMENT_REASON")
    )
}