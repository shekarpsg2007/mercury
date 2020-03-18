package com.humedica.mercury.etl.athena.facility

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class FacilityDepartment(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("dept:athena.util.UtilDedupedDepartment")

  columnSelect = Map(
    "dept" -> List("DEPARTMENT_ID", "DEPARTMENT_NAME", "DEPARTMENT_ZIP", "PLACE_OF_SERVICE_TYPE", "SPECIALTY_CODE")
  )

  map = Map(
    "DATASRC" -> literal("department"),
    "FACILITYID" -> mapFrom("DEPARTMENT_ID"),
    "FACILITYNAME" -> mapFrom("DEPARTMENT_NAME"),
    "FACILITYPOSTALCD" -> standardizeZip("DEPARTMENT_ZIP", zip5 = true),
    "LOCALFACILITYTYPE" -> mapFrom("PLACE_OF_SERVICE_TYPE"),
    "SPECIALTY" -> mapFrom("SPECIALTY_CODE")
  )

}