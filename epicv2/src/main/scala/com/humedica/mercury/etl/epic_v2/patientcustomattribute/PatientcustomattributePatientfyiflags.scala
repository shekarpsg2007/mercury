package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

class PatientcustomattributePatientfyiflags (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient_fyi_flags")

  columnSelect = Map(
    "patient_fyi_flags" -> List("PATIENT_ID", "PAT_FLAG_TYPE_C", "LAST_UPDATE_INST")
  )

  join = noJoin()


  afterJoin = (df: DataFrame) => {
   val fil = df.filter("PAT_FLAG_TYPE_C in ('1027','1029','1039','1040','1038','1044')")
      val groups = Window.partitionBy(df("PATIENT_ID")).orderBy(df("LAST_UPDATE_INST").desc_nulls_last)
      val addColumn = fil.withColumn("rw", row_number.over(groups)).filter("rw = 1")
      addColumn.withColumn("ATTRIBUTEVALUE",
        when(addColumn("PAT_FLAG_TYPE_C") === "1027", lit("Medicare UHC ACO"))
        .when(addColumn("PAT_FLAG_TYPE_C") === "1029", lit("Medicare Humana ACO"))
        .when(addColumn("PAT_FLAG_TYPE_C") === "1039", lit("FL Blue ACO"))
        .when(addColumn("PAT_FLAG_TYPE_C") === "1040", lit("Medicare FL Blue ACO"))
        .when(addColumn("PAT_FLAG_TYPE_C") === "1038", lit("Medicare UHC PCPi"))
        .when(addColumn("PAT_FLAG_TYPE_C") === "1044", lit("Cigna ACO"))
        .otherwise(null))
   }


  map = Map(
   "DATASRC" -> literal("patient_fyi_flags"),
   "PATIENTID" -> mapFrom("PATIENT_ID"),
    "ATTRIBUTE_TYPE_CUI" -> literal("CH002785"),
    "ATTRIBUTE_VALUE" -> mapFrom("ATTRIBUTEVALUE")
   )

}