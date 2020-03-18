package com.humedica.mercury.etl.athena.immunization

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ImmunizationPatientvaccinepv(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientvaccine")

      columnSelect = Map(
                   "patientvaccine" -> List("PATIENT_ID", "CLINICAL_ENCOUNTER_ID", "ADMINISTER_NOTE", "VACCINE_ROUTE", "ADMINISTERED_DATETIME",
                               "CVX", "VACCINE_NAME")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //Include where patientvaccine.HUM_TYPE = 'PATIENTVACCINE' and (patientvaccine.status <> 'DELETED' or patientvaccine.status is null) and patientvaccine.deleted_datetime is null. Exclude if LocalImmunization Code is null


      map = Map(
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "LOCALDEFERREDREASON" -> mapFrom("ADMINISTER_NOTE"),
        "LOCALROUTE" -> mapFrom("VACCINE_ROUTE"),
        "ADMINDATE" -> mapFrom("ADMINISTERED_DATETIME"),
        "LOCALIMMUNIZATIONCD" -> mapFrom("CVX"),
        "LOCALIMMUNIZATIONDESC" -> mapFrom("VACCINE_NAME")
      )

 }