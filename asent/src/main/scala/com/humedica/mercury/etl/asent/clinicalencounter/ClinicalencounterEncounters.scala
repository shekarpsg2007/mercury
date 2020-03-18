package com.humedica.mercury.etl.asent.clinicalencounter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ClinicalencounterEncounters(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_encounters", "as_charges")

  columnSelect = Map(
    "as_encounters" -> List("VISIT_ID", "ENCOUNTER_DATE_TIME", "ENCOUNTER_ID", "PATIENT_MRN", "START_TIME", "END_TIME", "APPOINTMENT_STATUS_ID"
      , "LAST_UPDATED_DATE", "APPOINTMENT_LOCATION_ID", "ENCOUNTER_TYPE_ID", "PROVIDER_ID", "FILEID"),
    "as_charges" -> List("ENCOUNTER_ID", "BILLING_STATUS")
  )

  beforeJoin = Map(
    "as_encounters" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ENCOUNTER_ID"))
        .orderBy(df("LAST_UPDATED_DATE").desc_nulls_last, df("FILEID").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups)).filter("rn=1")
    }),
    "as_charges" -> ((df: DataFrame) => {
      df.withColumnRenamed("ENCOUNTER_ID", "ENCOUNTER_ID_ac")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_encounters")
      .join(dfs("as_charges"), dfs("as_charges")("ENCOUNTER_ID_ac") === dfs("as_encounters")("ENCOUNTER_ID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    df.filter("coalesce(APPOINTMENT_STATUS_ID,'2') = '2' and coalesce(BILLING_STATUS,'null') not in ('R','C')")
  }

  map = Map(
    "DATASRC" -> literal("encounters"),
    "ARRIVALTIME" -> mapFrom("ENCOUNTER_DATE_TIME"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "ADMITTIME" -> mapFrom("START_TIME"),
    "DISCHARGETIME" -> mapFrom("END_TIME", nullIf = Seq("1901-01-01 00:00:00.0", "1900-01-01 00:00:00.0")),
    "FACILITYID" -> mapFrom("APPOINTMENT_LOCATION_ID"),
    "LOCALPATIENTTYPE" -> mapFrom("ENCOUNTER_TYPE_ID", nullIf = Seq(null), prefix = config(CLIENT_DS_ID) + "."),
    "LOCALENCOUNTERTYPE" -> mapFrom("ENCOUNTER_TYPE_ID"),
    "VISITID" -> mapFrom("VISIT_ID"),
    "ATTENDINGPHYSICIAN" -> mapFrom("PROVIDER_ID")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ENCOUNTERID")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw=1 and PATIENTID is not null and ENCOUNTERID is not null")
  }

}
