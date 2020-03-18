package com.humedica.mercury.etl.athena.patientidentifier

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class PatientidentifierPatientinsuranceremove(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientinsurance")

      columnSelect = Map(
                   "patientinsurance" -> List("SSN", "PATIENT_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where patient_relationship = 'Self' and (Entity_type is null or Entity_type = 'Person')


      map = Map(
        "IDTYPE" -> todo(""),      //TODO - to be coded
        "IDVALUE" -> todo("SSN"),      //TODO - to be coded
        "PATIENTID" -> mapFrom("PATIENT_ID")
      )

 }