package com.humedica.mercury.etl.epic_v2.patientpopulation
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class PatientpopulationPatienttype(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient_type", "cdr.map_predicate_values")

  columnSelect = Map(
    "patient_type" -> List("pat_id", "patient_type_c", "line", "update_date")
  )

  beforeJoin = Map (
      "cdr.map_predicate_values" -> ((df: DataFrame) => {
        val mpvtable = mpv(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT_TYPE", "PATIENT_POPULATION", "PATIENT_TYPE", "PATIENT_TYPE_C.POPULATION_TYPE_CUI")
            .select("COLUMN_VALUE")
        mpvtable.withColumn("PATIENT_TYPE_C", expr("substr(column_value, 1, instr(column_value,'.')-1)"))
                .withColumn("POPULATION_TYPE_CUI", expr("substr(column_value, instr(column_value,'.')+1)"))
      }),
      "patient_type" -> ((df: DataFrame) => {
        val groups = Window.partitionBy(df("PAT_ID"), df("LINE")).orderBy(df("UPDATE_DATE").desc)
        val addColumn = df.withColumn("rownumber", row_number.over(groups))
        addColumn.filter("rownumber = 1")
      })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patient_type")
      .join(dfs("cdr.map_predicate_values"), Seq("PATIENT_TYPE_C"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("patient_type"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "POPULATION_TYPE_CUI" -> mapFrom("POPULATION_TYPE_CUI")
  )
}

// val p = new PatientpopulationPatienttype(cfg) ; val pp = build(p) ; pp.show
