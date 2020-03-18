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


class ProcedureDocument(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("imagingresult",
                   "clinicalencounter",
                   "clinicalresult",
                   "document")

      columnSelect = Map(
                   "imagingresult" -> List("OBSERVATION_DATETIME"),
                   "clinicalencounter" -> List("ENCOUNTER_DATE", "CLINICAL_ENCOUNTER_ID"),
                   "clinicalresult" -> List(),
                   "document" -> List("CLINICAL_ORDER_TYPE", "PATIENT_ID", "FUTURE_SUBMIT_DATETIME", "CLINICAL_ORDER_TYPE")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //ENSURE THAT ALL DEDUPE CRITERIA APPLIED BEFORE INCLUSION CRITIERA!document.deleted_datetime is null (i.e. document.status <> 'DELETED')AND clinicalresult.deleted_datetime is null AND localcode is mapped in map_custom_procAND (document.document_class in ('LABRESULT','CLINICALDOCUMENT','IMAGINGRESULT','ENCOUNTERDOCUMENT')or (document.document_class in ('ORDER', 'VACCINE') and document.Future_submit_datetime is not null and < sysdate))AND DOCUMENT.DOCUMENT_CLASS NOT IN (&LIST_EXCLUDE_DOC_CLASS) -- adding for DIPR-4213


      map = Map(
        "LOCALCODE" -> cascadeFrom(Seq("document.CLINICAL_ORDER_TYPE", "clinicalresult.CLINICAL_ORDER_TYPE")),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "PROCEDUREDATE" -> cascadeFrom(Seq("document.FUTURE_SUBMIT_DATETIME", "clinicalresult.OBSERVATION_DATETIME", "document.OBSERVATION_DATETIME", 
                                    "clinicalencounter.ENCOUNTER_DATE")),
        "LOCALNAME" -> mapFrom("CLINICAL_ORDER_TYPE"),
        "ACTUALPROCDATE" -> mapFrom("ENCOUNTER_DATE"),
        "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
        "CODETYPE" -> todo("")      //TODO - to be coded
      )

      mapExceptions = Map(
        ("H251512_ATHENA", "PROCEDUREDATE") -> cascadeFrom(Seq("OBSERVATION_DATETIME", "CREATED_DATETIME"))
      )

 }