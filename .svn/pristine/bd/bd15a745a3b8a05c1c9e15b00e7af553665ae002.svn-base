package com.humedica.mercury.etl.athena.patientaddress

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.athena.util.UtilSplitTable

/**
  * Auto-generated on 09/21/2018
  */


class PatientaddressPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient:athena.util.UtilDedupedPatient", "cdr.map_predicate_values")

  columnSelect = Map(
    "patient" -> List("PATIENT_LAST_SEEN_DATE", "REGISTRATION_DATE", "PATIENT_ID", "HUM_STATE", "ZIP", "ADDRESS",
      "ADDRESS_2", "CITY")
  )

  beforeJoin = Map(
    "patient" -> ((df: DataFrame) => {
      df.withColumn("ZIPCODE",
        when(df("ZIP") === "00000", null)
          .otherwise(df("ZIP")))
    })
  )

  map = Map(
    "DATASRC" -> literal("patient"),
    "ADDRESS_DATE" -> cascadeFrom(Seq("PATIENT_LAST_SEEN_DATE", "REGISTRATION_DATE")),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "STATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("HUM_STATE"), 1, 2))
    }),
    "ZIPCODE" -> standardizeZip("ZIPCODE"),
    "ADDRESS_LINE1" -> mapFrom("ADDRESS"),
    "ADDRESS_LINE2" -> mapFrom("ADDRESS_2"),
    "CITY" -> mapFrom("CITY")
  )

  afterMap = (df: DataFrame) => {
    df.filter("coalesce(ADDRESS_LINE1, ADDRESS_LINE2 ,CITY, STATE, ZIPCODE) is not null" +
      " and address_date IS NOT NULL")
  }

}