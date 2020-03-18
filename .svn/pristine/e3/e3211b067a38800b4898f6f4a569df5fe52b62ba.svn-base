package com.humedica.mercury.etl.athena.insurance

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class InsuranceClaim(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("payer",
                   "clinicalencounter",
                   "patientinsurance",
                   "claim")

      columnSelect = Map(
                   "payer" -> List("INSURANCE_PACKAGE_TYPE", "INSURANCE_REPORTING_CATEGORY", "INSURANCE_REPORTING_CATEGORY", "INSURANCE_PACKAGE_ID", "INSURANCE_PACKAGE_NAME"),
                   "clinicalencounter" -> List("CLINICAL_ENCOUNTER_ID"),
                   "patientinsurance" -> List("POLICY_GROUP_NUMBER", "SEQUENCE_NUMBER", "POLICY_ID_NUMBER", "CANCELLATION_DATE", "ISSUE_DATE"),
                   "claim" -> List("CLAIM_SERVICE_DATE", "PATIENT_ID", "SERVICE_DEPARTMENT_ID", "CLAIM_APPOINTMENT_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //see join criteria **PLUS** where clinicalencounter.deleted_datetime is null, and payer.deleted_datetime is null


      map = Map(
        "INS_TIMESTAMP" -> mapFrom("CLAIM_SERVICE_DATE"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "FACILITYID" -> mapFrom("SERVICE_DEPARTMENT_ID"),
        "GROUPNBR" -> mapFrom("POLICY_GROUP_NUMBER"),
        "INSURANCEORDER" -> mapFrom("SEQUENCE_NUMBER"),
        "POLICYNUMBER" -> mapFrom("POLICY_ID_NUMBER"),
        "SOURCEID" -> mapFrom("CLAIM_APPOINTMENT_ID"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "ENROLLENDDT" -> cascadeFrom(Seq("CANCELLATION_DATE", "EXPIRATION_DATE")),
        "ENROLLSTARTDT" -> mapFrom("ISSUE_DATE"),
        "PLANTYPE" -> mapFrom("INSURANCE_PACKAGE_TYPE"),
        "PAYORCODE" -> mapFrom("INSURANCE_REPORTING_CATEGORY"),
        "PAYORNAME" -> mapFrom("INSURANCE_REPORTING_CATEGORY"),
        "PLANCODE" -> mapFrom("INSURANCE_PACKAGE_ID"),
        "PLANNAME" -> mapFrom("INSURANCE_PACKAGE_NAME")
      )

 }