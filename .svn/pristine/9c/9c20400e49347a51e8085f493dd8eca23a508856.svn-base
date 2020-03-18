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


class ProcedureQmresult(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("qmresult")

      columnSelect = Map(
                   "qmresult" -> List("P4P_MEASURE", "PATIENT_ID", "SATISFIED_DATE", "P4P_MEASURE", "PROVIDER_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //where nvl(src.result_status,'X') not in('NOTSATISFIED','EXCLUDED') and Localcode is mapped in CDR.Map_Custom_Proc


      map = Map(
        "LOCALCODE" -> mapFrom("P4P_MEASURE"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "PROCEDUREDATE" -> mapFrom("SATISFIED_DATE"),
        "LOCALNAME" -> mapFrom("P4P_MEASURE"),
        "PERFORMINGPROVIDERID" -> mapFrom("PROVIDER_ID"),
        "CODETYPE" -> todo("")      //TODO - to be coded
      )

 }