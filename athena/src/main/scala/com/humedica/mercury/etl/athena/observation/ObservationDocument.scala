package com.humedica.mercury.etl.athena.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ObservationDocument(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("document")

      columnSelect = Map(
                   "document" -> List("CLINICAL_ORDER_TYPE", "ORDER_DATETIME", "PATIENT_ID", "DEPARTMENT_ID", "CLINICAL_ENCOUNTER_ID",
                               "CLINICAL_ORDER_TYPE")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //where localcode in cdr.zcm_obstype_code (i.e. mapped in cdr.map_observation) AND Patientid is not null AND document.deleted_datetime IS NULL 


      map = Map(
        "LOCALCODE" -> mapFrom("CLINICAL_ORDER_TYPE"),
        "OBSDATE" -> mapFrom("ORDER_DATETIME"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "FACILITYID" -> mapFrom("DEPARTMENT_ID"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "LOCALRESULT" -> mapFrom("CLINICAL_ORDER_TYPE")
      )

 }