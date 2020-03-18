package com.humedica.mercury.etl.epic_v2.patientidentifier

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cdivakaran on 3/7/17.
  */
class PatientidentifierIdentityidsubtype (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List("temptable:epic_v2.patient.PatientTemptable", "cdr.map_predicate_values")

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val list_id_type = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "IDENTITY_ID", "PATIENT_ID", "IDENTITY_ID", "IDENTITY_TYPE_ID")
      val fil = df.filter("MRNID is not null and IDENTITY_SUBTYPE_ID is not null and (concat(IDENTITY_SUBTYPE_ID,'.',IDENTITY_TYPE_ID)) in (" + list_id_type + ")")
      fil.dropDuplicates(Seq("PATIENTID", "MRNID", "IDENTITY_SUBTYPE_ID"))
          }
      )
  )



  join = noJoin()


  map = Map(
    "DATASRC" -> literal("patreg"),
    "IDTYPE" -> literal("MRN"),
    "IDVALUE" -> mapFrom("MRNID"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ID_SUBTYPE" -> mapFrom("IDENTITY_SUBTYPE_ID")
  )

}