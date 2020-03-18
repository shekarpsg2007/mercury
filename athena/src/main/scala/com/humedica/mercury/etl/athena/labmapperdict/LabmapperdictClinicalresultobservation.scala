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


class LabmapperdictClinicalresultobservation(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("clinicalresultobservation")

      columnSelect = Map(
                   "clinicalresultobservation" -> List("OBSERVATION_IDENTIFIER", "OBSERVATION_IDENTIFIER_TEXT")
      )

      //TODO - Create join


      map = Map(
        "LOCALCODE" -> cascadeFrom(Seq("OBSERVATION_IDENTIFIER", "OBSERVATION_IDENTIFIER_TEXT")),
        "LOCALNAME" -> mapFrom("OBSERVATION_IDENTIFIER_TEXT")
      )

 }