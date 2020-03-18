package com.humedica.mercury.etl.epic_v2.medmapsource
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Functions

class MedmapsourceMedadmin(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("mmorders:epic_v2.madmapsource.MedmapsourceMedorders")

  columns = List("DATASRC", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALNDC", "HAS_NDC", "NO_NDC", "NUM_RECS")

  map = Map(
    "DATASRC" -> literal("medadminrec"),
    "LOCALMEDCODE" -> mapFrom("LOCALMEDCODE"),
    "LOCALDESCRIPTION" -> mapFrom("LOCALDESCRIPTION"),
    "LOCALNDC" -> mapFrom("LOCALNDC"),
    "HAS_NDC" -> mapFrom("HAS_NDC"),
    "NO_NDC" -> mapFrom("NO_NDC"),
    "NUM_RECS" -> mapFrom("NUM_RECS")
  )

}

// TEST
// val m = new MedmapsourceMedadmin(cfg) ; val mm = build(m) ; mm.orderBy(desc("NUM_RECS")).show
