package com.humedica.mercury.etl.asent.encounterprovider

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._


class EncounterproviderAscharges(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_charges")

  columnSelect = Map(
    "as_charges" -> List("BILLING_LOCATION_ID", "ENCOUNTER_DATE_TIME", "SCHEDULING_PROVIDER_ID", "PATIENT_MRN", "BILLING_LOCATION_ID",
      "ENCOUNTER_ID")
  )


  map = Map(
    "DATASRC" -> literal("as_charges"),
    "ENCOUNTERTIME" -> mapFrom("ENCOUNTER_DATE_TIME"),
    "PROVIDERID" -> mapFrom("SCHEDULING_PROVIDER_ID", nullIf = Seq("0")),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "FACILITYID" -> mapFrom("BILLING_LOCATION_ID"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PROVIDERROLE" -> literal("Scheduling provider")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ENCOUNTERID"), df("PROVIDERID")).orderBy(df("ENCOUNTERTIME").desc_nulls_last, df("FACILITYID").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn=1 AND ENCOUNTERTIME IS NOT NULL AND PATIENTID IS NOT NULL AND PROVIDERID IS NOT NULL")

  }
}


