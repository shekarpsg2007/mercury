package com.humedica.mercury.etl.athena.appointmentlocation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class AppointmentlocationDepartment(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("department:athena.util.UtilDedupedDepartment")

  columnSelect = Map(
    "department" -> List("DEPARTMENT_NAME", "DEPARTMENT_ID")
  )

  map = Map(
    "LOCATIONNAME" -> mapFrom("DEPARTMENT_NAME"),
    "LOCATIONID" -> mapFrom("DEPARTMENT_ID")
  )

}