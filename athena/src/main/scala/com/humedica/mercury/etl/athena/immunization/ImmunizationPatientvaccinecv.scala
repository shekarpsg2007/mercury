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


class ImmunizationPatientvaccinecv(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patientvaccine",
                   "medication",
                   "clinicalresult",
                   "document")

      columnSelect = Map(
                   "patientvaccine" -> List("PATIENT_ID", "REFUSED_REASON", "ADMINISTERED_DATETIME"),
                   "medication" -> List("RXNORM", "MEDICATION_NAME", "NDC"),
                   "clinicalresult" -> List(),
                   "document" -> List("CLINICAL_ENCOUNTER_ID", "VACCINE_ROUTE", "CVX")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //Apply inclusion criteria AFTER deduping.Include where patientvaccine.HUM_TYPE = 'CLINICALVACCINE' and (patientvaccine.status <> 'DELETED' or patientvaccine.status is null) and patientvaccine.deleted_datetime is null and document.deleted_datetime is null  AND clinicalresult.deleted_Datetime is null


      map = Map(
        "PATIENTID" -> cascadeFrom(Seq("patientvaccine.PATIENT_ID", "document.PATIENT_ID")),
        "ENCOUNTERID" -> cascadeFrom(Seq("document.CLINICAL_ENCOUNTER_ID", "patientvaccine.CLINICAL_ENCOUNTER_ID")),
        "LOCALDEFERREDREASON" -> cascadeFrom(Seq("patientvaccine.REFUSED_REASON", "document.PROVIDER_NOTE", "patientvaccine.ADMINISTER_NOTE", 
                                    "patientvaccine.PROVIDER_NOTE", "clinicalresult.PROVIDER_NOTE")),
        "LOCALROUTE" -> cascadeFrom(Seq("document.VACCINE_ROUTE", "patientvaccine.VACCINE_ROUTE")),
        "RXNORM_CODE" -> mapFrom("RXNORM"),
        "ADMINDATE" -> cascadeFrom(Seq("ADMINISTERED_DATETIME", "VIS_GIVEN_DATE", "ORDER_DATETIME")),
        "LOCALIMMUNIZATIONCD" -> cascadeFrom(Seq("document.CVX", "patientvaccine.CVX", "clinicalresult.CVX")),
        "LOCALIMMUNIZATIONDESC" -> cascadeFrom(Seq("MEDICATION_NAME", "VACCINE_NAME")),
        "LOCALNDC" -> mapFrom("NDC")
      )

 }