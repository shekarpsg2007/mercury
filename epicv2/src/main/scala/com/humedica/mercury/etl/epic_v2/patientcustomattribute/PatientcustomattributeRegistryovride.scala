package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.Constants.{CLIENT_DS_ID, GROUP}
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 01/13/2017
 */


class PatientcustomattributeRegistryovride(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("registry_ovride","cdr.map_predicate_values")


      columnSelect = Map(
        "registry_ovride" -> List("PAT_ID", "OVERRIDE_EDIT_DATE", "LINE", "REGI_EXPIRE_DATE", "REG_OVRIDE_TYPE_C")
      )



      beforeJoin = Map(
        "registry_ovride" -> ((df: DataFrame) => {
          val df1 = df.repartition(500)
          val groups = Window.partitionBy(df1("PAT_ID")).orderBy(df1("OVERRIDE_EDIT_DATE").desc, df1("LINE").desc)
          val t = df1.withColumn("rn", row_number.over(groups))
          val fil = t.filter("rn = 1 AND PAT_ID IS NOT NULL ").drop("rn")
          val current = current_date()
          val inclcol = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "REGISTRY_OVRIDE", "PATIENT_ATTRIBUTE", "REGISTRY_OVRIDE", "INCLUSION")
          fil.filter("(REGI_EXPIRE_DATE > " + current + " or REGI_EXPIRE_DATE is null) AND 'Y' = " + inclcol + "")
        })
      )

      join = noJoin()

      map = Map(
        "DATASRC" -> literal("registry_ovride"),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "ATTRIBUTE_VALUE" -> ((col:String,df:DataFrame) => df.withColumn(col,when(df("REG_OVRIDE_TYPE_C") === lit("1"), lit("Added")).otherwise(when(df("REG_OVRIDE_TYPE_C") === lit("2"), lit("Removed")).otherwise(null)))),
        "ATTRIBUTE_TYPE_CUI" -> literal("CH002787")
      )

 }