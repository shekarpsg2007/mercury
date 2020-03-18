package com.humedica.mercury.etl.asent.appointmentlocation


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class AppointmentlocationLocationde(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_location_de")

  columnSelect = Map(
    "as_zc_location_de" -> List("ENTRYNAME", "ID")
  )


  beforeJoin = Map(
    "as_zc_location_de" -> ((df: DataFrame) => {
      df.filter("ENTRYNAME is not null AND ID is not null")
    }
      )
  )


  map = Map(
    "DATASRC" -> literal("location_de"),
    "LOCATIONNAME" -> mapFrom("ENTRYNAME"),
    "LOCATIONID" -> mapFrom("ID")
  )

}

// Test
// val apld = new AppointmentlocationLocationde (cfg); val apl = build(apld) ; apl.show; apl.count
