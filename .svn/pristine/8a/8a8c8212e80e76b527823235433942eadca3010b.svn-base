package com.humedica.mercury.etl.athena.labmapperdict

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class LabmapperdictDocument(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("clinicalresult",
                   "clinicalresultobservation",
                   "document")

      columnSelect = Map(
                   "clinicalresult" -> List("CLINICAL_ORDER_TYPE", "CLINICAL_ORDER_TYPE"),
                   "clinicalresultobservation" -> List("TEMPLATE_ANALYTE_NAME", "TEMPLATE_ANALYTE_NAME"),
                   "document" -> List("CLINICAL_ORDER_TYPE", "CLINICAL_ORDER_TYPE")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where document_class = 'LABRESULT' and codes are not coming from 'clinicalresultobservation'


      map = Map(
        "LOCALCODE" -> todo("CLINICAL_ORDER_TYPE"),      //TODO - to be coded
        "LOCALCODE" -> todo("CLINICAL_ORDER_TYPE"),      //TODO - to be coded
        "LOCALCODE" -> todo("TEMPLATE_ANALYTE_NAME"),      //TODO - to be coded
        "LOCALNAME" -> todo("CLINICAL_ORDER_TYPE"),      //TODO - to be coded
        "LOCALNAME" -> todo("CLINICAL_ORDER_TYPE"),      //TODO - to be coded
        "LOCALNAME" -> todo("TEMPLATE_ANALYTE_NAME")      //TODO - to be coded
      )

 }