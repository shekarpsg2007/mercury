package com.humedica.mercury.etl.athena.labresult

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class LabresultClinicalresultobservation(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("loinc",
                   "clinicalresultobservation",
                   "clinicalresult",
                   "document")

      columnSelect = Map(
                   "loinc" -> List("LOINC_CODE"),
                   "clinicalresultobservation" -> List("CLINICAL_OBSERVATION_ID", "OBSERVATION_IDENTIFIER", "OBSERVATION_IDENTIFIER_TEXT", "TEMPLATE_ANALYTE_NAME", "OBSERVATION_UNITS",
                               "REFERENCE_RANGE", "OBSERVATION_ABNORMAL_FLAG_ID", "HUM_RESULT"),
                   "clinicalresult" -> List("CLINICAL_ORDER_TYPE", "SPECIMEN_SOURCE", "CLINICAL_ORDER_TYPE", "RESULT_STATUS", "RESULTS_REPORTED_DATETIME"),
                   "document" -> List("PATIENT_ID", "CLINICAL_ENCOUNTER_ID", "CLINICAL_ORDER_TYPE", "CLINICAL_ORDER_TYPE", "OBSERVATION_DATETIME",
                               "ORDER_DATETIME", "DOCUMENT_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //Include if clinicalresultsobservation.deleted_datetime is null AND document.document_class = 'LABRESULT' and (lower(statuscode) NOT IN ('canceled','cancelled','incomplete','partial','pending','preliminary') OR statuscode IS NULL);


      map = Map(
        "LABRESULTID" -> mapFrom("CLINICAL_OBSERVATION_ID"),
        "LOCALCODE" -> cascadeFrom(Seq("clinicalresultobservation.OBSERVATION_IDENTIFIER", "clinicalresultobservation.OBSERVATION_IDENTIFIER_TEXT", "document.CLINICAL_ORDER_TYPE", 
                                    "clinicalresult.CLINICAL_ORDER_TYPE", "clinicalresultobservation.TEMPLATE_ANALYTE_NAME")),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "LOCALNAME" -> todo("OBSERVATION_IDENTIFIER_TEXT"),      //TODO - to be coded
        "LOCALNAME" -> todo("CLINICAL_ORDER_TYPE"),      //TODO - to be coded
        "LOCALNAME" -> todo("CLINICAL_ORDER_TYPE"),      //TODO - to be coded
        "LOCALNAME" -> todo("TEMPLATE_ANALYTE_NAME"),      //TODO - to be coded
        "LOCALSPECIMENTYPE" -> mapFrom("SPECIMEN_SOURCE"),
        "LOCALTESTNAME" -> mapFrom("CLINICAL_ORDER_TYPE"),
        "LOCALTESTNAME" -> mapFrom("CLINICAL_ORDER_TYPE"),
        "LOCALUNITS" -> mapFrom("OBSERVATION_UNITS"),
        "LOCAL_LOINC_CODE" -> mapFrom("LOINC_CODE"),
        "NORMALRANGE" -> mapFrom("REFERENCE_RANGE"),
        "RESULTTYPE" -> mapFrom("OBSERVATION_ABNORMAL_FLAG_ID"),
        "STATUSCODE" -> mapFrom("RESULT_STATUS"),
        "DATEAVAILABLE" -> cascadeFrom(Seq("RESULTS_REPORTED_DATETIME", "CREATED_DATETIME")),
        "DATECOLLECTED" -> cascadeFrom(Seq("OBSERVATION_DATETIME", "SPECIMEN_COLLECTED_DATETIME")),
        "LABORDEREDDATE" -> mapFrom("ORDER_DATETIME"),
        "LABORDERID" -> mapFrom("DOCUMENT_ID"),
        "LOCALRESULT" -> mapFrom("HUM_RESULT")
      )

 }