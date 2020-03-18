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


class ProcedureFlowsheet(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("flowsheet")

      columnSelect = Map(
                   "flowsheet" -> List("FLOW_SHEET_ELEMENT", "PATIENT_ID", "FLOW_SHEET_ELEMENT_VALUE", "FLOW_SHEET_ELEMENT")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where Localcode is mapped in CDR.Map_Custom_Proc


      map = Map(
        "LOCALCODE" -> mapFrom("FLOW_SHEET_ELEMENT"),
        "PATIENTID" -> mapFrom("PATIENT_ID"),
        "PROCEDUREDATE" -> mapFrom("FLOW_SHEET_ELEMENT_VALUE"),
        "LOCALNAME" -> mapFrom("FLOW_SHEET_ELEMENT"),
        "CODETYPE" -> todo("")      //TODO - to be coded
      )

 }