package com.humedica.mercury.etl.asent.observation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationEncounters(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_problem_de",
    "as_charges",
    "as_encounters",
    "cdr.zcm_obstype_code")

  columnSelect = Map(
    "as_zc_problem_de" -> List("ENTRYNAME", "ENTRYNAME", "ENTRYNAME", "ID"),
    "as_charges" -> List("ENCOUNTER_ID", "BILLING_STATUS"),
    "as_encounters" -> List("PROBLEM_DE", "ENCOUNTER_DATE_TIME", "PATIENT_MRN", "APPOINTMENT_LOCATION_ID",
      "ENCOUNTER_ID", "LAST_UPDATED_DATE", "APPOINTMENT_STATUS_ID"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "DATATYPE", "OBSTYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "as_encounters" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("PROBLEM_DE"), df("ENCOUNTER_DATE_TIME")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1 AND PROBLEM_DE IS NOT NULL AND ENCOUNTER_DATE_TIME IS NOT NULL AND PATIENT_MRN IS NOT NULL").drop("rn")
    }),
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'encounters'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_encounters")
      .join(dfs("as_zc_problem_de"), dfs("as_zc_problem_de")("id") === dfs("as_encounters")("problem_de"), "inner")
      .join(dfs("as_charges"), Seq("encounter_id"), "left_outer")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("obscode") ===
        concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_encounters")("PROBLEM_DE")), "inner")
  }

  afterJoin = (df: DataFrame) => {
    df.filter("APPOINTMENT_STATUS_ID = '2' or (APPOINTMENT_STATUS_ID IS NULL AND BILLING_STATUS not in ('R', 'C'))")
  }

  map = Map(
    "DATASRC" -> literal("encounters"),
    "FACILITYID" -> mapFrom("APPOINTMENT_LOCATION_ID"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "OBSDATE" -> mapFrom("ENCOUNTER_DATE_TIME"),
    "LOCALCODE" -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("PROBLEM_DE")))),
    "OBSRESULT" -> mapFrom("ENTRYNAME"),
    "LOCALRESULT" -> ((col, df) => df.withColumn(col, when(df("ENTRYNAME").isNotNull && df("DATATYPE") === lit("CV"), concat_ws(".", lit(config(CLIENT_DS_ID)), df("ENTRYNAME"))).otherwise(df("ENTRYNAME")))),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("LOCALCODE"), df("OBSDATE"), df("OBSTYPE")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw=1")
  }

}