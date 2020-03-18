package com.humedica.mercury.etl.asent.encounterprovider

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

/**
  * Created by tzentz on 7/3/17.
  */

class EncounterproviderAsorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_orders", "cdr.map_predicate_values")

  columnSelect = Map(
    "as_orders" -> List("CLINICAL_DATETIME", "APPROVING_PROVIDER_ID", "PATIENT_MRN", "ENCOUNTER_ID")
  )

  //only one table
  join = noJoin()

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ENCOUNTER_ID"), df("APPROVING_PROVIDER_ID")).orderBy(df("CLINICAL_DATETIME").desc)
    val df2 = df.withColumn("rn", row_number.over(groups))
    df2.filter("rn = '1'")
  }

  map = Map(
    "DATASRC" -> literal("as_orders"),
    "ENCOUNTERTIME" -> mapFrom("CLINICAL_DATETIME"),
    //do not pivot a row where source field for local provider id is null or  = '0';
    "PROVIDERID" -> mapFrom("APPROVING_PROVIDER_ID", nullIf = Seq("0")),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PROVIDERROLE" -> literal("Approving provider")
  )

  //Do not include if client_ds_id exists in MPV with values (AS_ORDERS, ENCOUNTERPROVIDER, AS_ORDERS, APPROVING_PROVIDER_ID)
  //WHERE &excl_encprov_ds_id <> 'N'
  afterMap = (df: DataFrame) => {
    val mpv = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERPROVIDER", "AS_ORDERS", "as_orders", "APPROVING_PROVIDER_ID")
    if (mpv == "(N)") {
      null
    }
    else {
      df.filter("PROVIDERID is not null")
    }
  }

}