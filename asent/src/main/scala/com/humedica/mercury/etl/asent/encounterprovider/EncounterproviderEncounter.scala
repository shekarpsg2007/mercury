package com.humedica.mercury.etl.asent.encounterprovider

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class EncounterproviderEncounter(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "ce:asent.clinicalencounter.ClinicalencounterEncounters"
  )

  columnSelect = Map(
    "ce" -> List("ENCOUNTERID", "FACILITYID", "PATIENTID", "ADMITTINGPHYSICIAN", "ATTENDINGPHYSICIAN", "ARRIVALTIME")
  )

  beforeJoin = Map(
    "ce" -> ((df: DataFrame) => {
      val groupsAttend = Window.partitionBy(df("ENCOUNTERID"), df("ATTENDINGPHYSICIAN")).orderBy(df("ATTENDINGPHYSICIAN").desc_nulls_last)
      val addColumns = df.withColumn("ATTENDID", when(df("ATTENDINGPHYSICIAN") === "0", null).otherwise(df("ATTENDINGPHYSICIAN")))
        .withColumn("ATTENDROW", row_number.over(groupsAttend))
      addColumns.filter("ATTENDROW = 1 and ATTENDID is not null")
    })
  )

  join = noJoin()

  map = Map(
    "DATASRC" -> literal("encounter"),
    "ENCOUNTERTIME" -> mapFrom("ARRIVALTIME"),
    "PROVIDERID" -> mapFrom("ATTENDID"),
    "PROVIDERROLE" -> literal("ATTEND")
  )
  
}
