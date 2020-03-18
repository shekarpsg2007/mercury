package com.humedica.mercury.etl.athena.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ProcedureTransaction(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("procedurecode",
                   "transaction",
                   "clinicalencounter",
                   "claim")

      columnSelect = Map(
                   "procedurecode" -> List("PROCEDURE_CODE_DESCRIPTION"),
                   "transaction" -> List("PROCEDURE_CODE", "CLAIM_PATIENT_ID", "CHARGE_FROM_DATE", "SERVICE_DEPARTMENT_ID", "CLAIM_REFERRING_PROVIDER_ID",
                               "CHARGE_FROM_DATE", "CLAIM_APPOINTMENT_ID", "RENDERING_PROVIDER_ID", "CLAIM_APPOINTMENT_ID", "PROCEDURE_CODE",
                               "PROCEDURE_CODE"),
                   "clinicalencounter" -> List("CLINICAL_ENCOUNTER_ID"),
                   "claim" -> List("CLAIM_APPOINTMENT_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //where transaction.transaction_type = 'CHARGE' AND clinicalencounter.deleted_datetime is null AND procedurecode.deleted_datetime is nullAND transaction.VOIDED_DATE is null


      map = Map(
        "LOCALCODE" -> mapFrom("PROCEDURE_CODE"),
        "PATIENTID" -> cascadeFrom(Seq("CLAIM_PATIENT_ID", "PATIENT_ID")),
        "PROCEDUREDATE" -> cascadeFrom(Seq("transaction.CHARGE_FROM_DATE", "claim.CLAIM_SERVICE_DATE", "transaction.CLAIM_SERVICE_DATE")),
        "FACILITYID" -> cascadeFrom(Seq("transaction.SERVICE_DEPARTMENT_ID", "claim.SERVICE_DEPARTMENT_ID")),
        "LOCALNAME" -> mapFrom("PROCEDURE_CODE_DESCRIPTION"),
        "REFERPROVIDERID" -> cascadeFrom(Seq("transaction.CLAIM_REFERRING_PROVIDER_ID", "claim.CLAIM_REFERRING_PROVIDER_ID")),
        "ACTUALPROCDATE" -> cascadeFrom(Seq("transaction.CHARGE_FROM_DATE", "claim.CLAIM_SERVICE_DATE", "transaction.CLAIM_SERVICE_DATE")),
        "ENCOUNTERID" -> todo("CLINICAL_ENCOUNTER_ID"),      //TODO - to be coded
        "ENCOUNTERID" -> todo("CLAIM_APPOINTMENT_ID"),      //TODO - to be coded
        "ENCOUNTERID" -> todo("CLAIM_APPOINTMENT_ID"),      //TODO - to be coded
        "PERFORMINGPROVIDERID" -> cascadeFrom(Seq("transaction.RENDERING_PROVIDER_ID", "claim.RENDERING_PROVIDER_ID")),
        "SOURCEID" -> cascadeFrom(Seq("transaction.CLAIM_APPOINTMENT_ID", "claim.CLAIM_APPOINTMENT_ID")),
        "MAPPEDCODE" -> mapFrom("PROCEDURE_CODE"),
        "CODETYPE" -> mapFrom("PROCEDURE_CODE")
      )

 }