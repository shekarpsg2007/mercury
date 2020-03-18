package com.humedica.mercury.etl.athena.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ProcedurePatientmedication(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patientmedication:athena.util.UtilDedupedPatientMedication",
    "cdr.map_custom_proc")

  columnSelect = Map(
    "patientmedication" -> List("MEDICATION_TYPE", "PATIENT_ID", "PROC_DATE"),
    "cdr.map_custom_proc" -> List("GROUPID", "LOCALCODE", "MAPPEDVALUE", "CODETYPE", "DATASRC")
  )

  beforeJoin = Map(
    "patientmedication" -> includeIf("patient_id is not null and proc_date is not null"),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUPID) + "' and datasrc = 'patientmedication'")
        .drop("GROUPID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patientmedication")
      .join(dfs("cdr.map_custom_proc"), dfs("patientmedication")("MEDICATION_TYPE") === dfs("cdr.map_custom_proc")("LOCALCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("patientmedication"),
    "LOCALCODE" -> mapFrom("MEDICATION_TYPE"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "PROCEDUREDATE" -> mapFrom("PROC_DATE"),
    "LOCALNAME" -> mapFrom("MEDICATION_TYPE"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> mapFrom("CODETYPE")
  )

}