package com.humedica.mercury.etl.cerner_v2.labmapperdict

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 08/09/2018
 */


class LabmapperdictZhv500eventcode(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:cerner_v2.labmapperdict.LabmapperdictEventsettemp")

  columnSelect = Map(
    "temptable" -> List("EVENT_CD", "EVENT_CD_DISP", "EVENT_CD_DESCR")
  )

  beforeJoin = Map(
    "temptable" -> includeIf("EVENT_CD is not null")
  )

  map = Map(
    "LOCALCODE" -> mapFrom("EVENT_CD"),
    "LOCALDESC" -> mapFrom("EVENT_CD_DISP"),
    "LOCALNAME" -> mapFrom("EVENT_CD_DESCR")
  )

}