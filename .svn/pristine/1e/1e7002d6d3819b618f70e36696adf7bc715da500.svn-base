package com.humedica.mercury.etl.cerner_v2.labmapperdict

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 08/09/2018
 */


class LabmapperdictMapobservation(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("zh_v500_event_code")

      columnSelect = Map(
                   "zh_v500_event_code" -> List("EVENT_CD_DISP", "EVENT_CD_DESCR")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //Include where MAP_OBSERVATION.obstype = 'LABRESULT'


      map = Map(
        "LOCALDESC" -> mapFrom("EVENT_CD_DISP"),
        "LOCALNAME" -> mapFrom("EVENT_CD_DESCR")
      )

 }