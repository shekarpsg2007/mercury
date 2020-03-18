package com.humedica.mercury.etl.asent.observation


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationVitals(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_vitals",
    "as_zc_unit_code_de",
    "cdr.zcm_obstype_code")

  columnSelect = Map(
    "as_vitals" -> List("VITAL_ENTRY_NAME", "PERFORMED_DATETIME", "PATIENT_MRN", "VITAL_ENTRY_NAME", "STATUS_ID",
      "ENCOUNTER_ID", "VITAL_VALUE", "UNITS_ID", "CHILD_ID", "LAST_UPDATED_DATE"),
    "as_zc_unit_code_de" -> List("ENTRYNAME", "ID", "ENTRYMNEMONIC"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS")
  )


  join = (dfs: Map[String, DataFrame]) => {

    val firstJoin = dfs("as_vitals")
      .join(dfs("as_zc_unit_code_de"), dfs("as_zc_unit_code_de")("ID") === dfs("as_vitals")("UNITS_ID"), "left_outer")
    val groups = Window.partitionBy(firstJoin("CHILD_ID")).orderBy(firstJoin("LAST_UPDATED_DATE").desc)
    val addColumn = firstJoin.withColumn("rn", row_number.over(groups))
    val deduped = addColumn.filter("rn = 1").drop("rn")

    val localcode = deduped.withColumn("localcode", when(deduped("ENTRYNAME").isNotNull, concat_ws("_", deduped("VITAL_ENTRY_NAME"), deduped("ENTRYNAME")))
      .otherwise(deduped("VITAL_ENTRY_NAME")))

    localcode
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("OBSCODE") === localcode("localcode")
        && dfs("cdr.zcm_obstype_code")("GROUPID") === config(GROUPID)
        && dfs("cdr.zcm_obstype_code")("DATASRC") === lit("vitals"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    df.filter("OBSTYPE is not null and OBSTYPE != 'LABRESULT' AND STATUS_ID != '6'")
  }

  map = Map(
    "DATASRC" -> literal("vitals"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "OBSDATE" -> mapFrom("PERFORMED_DATETIME"),
    "LOCALCODE" -> mapFrom("localcode"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> ((col, df) => df.withColumn(col, coalesce(df("ENTRYMNEMONIC"), df("LOCALUNIT")))),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "STATUSCODE" -> mapFrom("STATUS_ID"),
    "LOCALRESULT" -> mapFrom("VITAL_VALUE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("LOCALCODE"), df("OBSDATE"), df("PATIENTID"), df("STATUSCODE"), df("ENCOUNTERID"), df("LOCALRESULT"), df("LOCAL_OBS_UNIT")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw=1")
      .distinct()
  }

  mapExceptions = Map(
    ("H984926_AS_ENT_MISAG", "LOCALCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("LOCALCODE")))
    })
  )
}


// Test
// val bobs = new ObservationVitals(cfg) ; val obs = build(bobs) ; obs.show(false); obs.count