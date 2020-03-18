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


class LabmapperdictVitalsign(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("vitalsign")

      columnSelect = Map(
                   "vitalsign" -> List("HUM_KEY", "HUM_KEY", "DB_UNIT")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //include where LocalResultCode (hum_key) is mapped to LABRESULT in MAP_OBSERVATION / ZCM_OBSTYPE_CODE


      map = Map(
        "LOCALCODE" -> mapFrom("HUM_KEY"),
        "LOCALNAME" -> mapFrom("HUM_KEY"),
        "LOCALUNITS" -> mapFrom("DB_UNIT")
      )

 }