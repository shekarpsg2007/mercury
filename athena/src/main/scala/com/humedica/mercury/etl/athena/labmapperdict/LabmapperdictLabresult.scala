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


class LabmapperdictLabresult(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("labresult")

      columnSelect = Map(
                   "labresult" -> List("OBSERVATION_IDENTIFIER", "REFERENCE_RANGE", "OBSERVATION_IDENTIFIER_TEXT", "OBSERVATION_UNITS")
      )

      //TODO - Create join


      map = Map(
        "LOCALCODE" -> cascadeFrom(Seq("OBSERVATION_IDENTIFIER", "OBSERVATION_IDENTIFIER_TEXT")),
        "LOCALREFRANGE" -> mapFrom("REFERENCE_RANGE"),
        "LOCALNAME" -> mapFrom("OBSERVATION_IDENTIFIER_TEXT"),
        "LOCALUNITS" -> mapFrom("OBSERVATION_UNITS")
      )

 }