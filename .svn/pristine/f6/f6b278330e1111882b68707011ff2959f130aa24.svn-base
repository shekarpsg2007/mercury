package com.humedica.mercury.etl.epic_v2.patientidentifier

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 01/27/2017
 */


class PatientidentifierIdentityid(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List("temptable:epic_v2.patient.PatientTemptable", "cdr.map_predicate_values")

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val list_id_type = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "IDENTITY_ID", "PATIENT_ID", "IDENTITY_ID", "IDENTITY_TYPE_ID")
      df.filter("MRNID is not null and IDENTITY_SUBTYPE_ID is null and identity_type_id in (" + list_id_type + ")")

    }


      )
  )


  join = noJoin()

  map = Map(
    "DATASRC" -> literal("patreg"),
    "IDTYPE" -> literal("MRN"),
    "IDVALUE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat(df("MRNID"), lit("A")))
    }),
    "PATIENTID" -> mapFrom("PATIENTID")
  )

  mapExceptions = Map(
    ("H101623_EP2", "IDVALUE") -> mapFrom("IDENTITY_ID")
  )

}