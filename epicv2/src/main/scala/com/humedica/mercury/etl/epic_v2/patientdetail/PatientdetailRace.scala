package com.humedica.mercury.etl.epic_v2.patientdetail

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 01/27/2017
 */


class PatientdetailRace(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.patient.PatientTemptable")

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      df.filter("RACE is not null and RA_UPDATE_DATE is not null")
    }
      )
  )


  join = noJoin()



  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), lower(df("RACE"))).orderBy(df("RA_UPDATE_DATE").desc)
    val addColumn = df.withColumn("rownbr", row_number.over(groups))
    addColumn.filter("rownbr = 1")
  }



  map = Map(
    "DATASRC" -> literal("patreg"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PATIENTDETAILTYPE" -> literal("RACE"),
    "PATDETAIL_TIMESTAMP" ->mapFrom("RA_UPDATE_DATE"),
    "LOCALVALUE"-> mapFrom("RACE", prefix=config(CLIENT_DS_ID)+".")

  )

}