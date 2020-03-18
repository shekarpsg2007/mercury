package com.humedica.mercury.etl.cerner_v2.encounterreason

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class EncounterreasonDiagnosis(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("diagnosis", "cdr.map_predicate_values")

  columnSelect = Map(
    "diagnosis" -> List("ENCNTR_ID", "PERSON_ID", "BEG_EFFECTIVE_DT_TM", "DIAGNOSIS_DISPLAY", "DIAG_DT_TM",
      "CONFIRMATION_STATUS_CD", "DIAG_TYPE_CD", "UPDT_DT_TM")
  )

  beforeJoin = Map(
    "diagnosis" -> ((df: DataFrame) => {
      val list_confirm_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS", "ENCOUNTER_REASON", "DIAGNOSIS", "CONFIRMATION_STATUS_CD")
      val list_diag_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS", "ENCOUNTER_REASON", "DIAGNOSIS", "DIAG_TYPE_CD")
      val fil = df.filter("confirmation_status_cd in (" + list_confirm_status_cd + ") or diag_type_cd in (" + list_diag_type_cd + ")")
      val groups = Window.partitionBy(fil("ENCNTR_ID"))
        .orderBy(fil("DIAG_DT_TM").desc_nulls_last, fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn")
    })
  )

  map = Map(
    "DATASRC" -> literal("diagnosis"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "REASONTIME" -> cascadeFrom(Seq("BEG_EFFECTIVE_DT_TM", "DIAG_DT_TM")),
    "LOCALREASONTEXT" -> mapFrom("DIAGNOSIS_DISPLAY")
  )

  afterMap = (df: DataFrame) => {
    df.filter("patientid is not null and localreasontext is not null")
  }

}