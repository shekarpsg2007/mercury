package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */

class ProcedureMedsrevhx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("meds_rev_hx")

  columnSelect = Map(
    "meds_rev_hx" -> List("PAT_ID", "MEDS_HX_REV_INSTANT", "MEDS_HX_REV_CSN")
  )

  beforeJoin = Map(
    "meds_rev_hx"-> ((df: DataFrame) => {
      val fil = df.filter("meds_hx_rev_instant is not null and pat_id is not null")
      val groups = Window.partitionBy(fil("PAT_ID"),fil("MEDS_HX_REV_CSN"),fil("MEDS_HX_REV_INSTANT")).orderBy(fil("MEDS_HX_REV_INSTANT").desc)
      df.withColumn("rw", row_number.over(groups))
        .filter("rw=1 and MEDS_HX_REV_INSTANT is not null")
    })
  )

  join = noJoin()

  map = Map(
    "DATASRC" -> literal("meds_rev_hx"),
    "LOCALCODE" -> literal("MEDS_REV_HX"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("MEDS_HX_REV_INSTANT"),
    "ACTUALPROCDATE" -> mapFrom("MEDS_HX_REV_INSTANT"),
    "ENCOUNTERID" -> mapFrom("MEDS_HX_REV_CSN"),
    "MAPPEDCODE" -> literal("MEDREC"),
    "CODETYPE" -> literal("CUSTOM")
  )


}

//   val b = new ProcedureMedsrevhx(cfg) ; val bb = build(b) ; bb.show(false) ; bb.count