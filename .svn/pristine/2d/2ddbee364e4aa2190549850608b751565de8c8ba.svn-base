package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 01/13/2017
  *
  *
  *
 */


class PatientcustomattributeCareteamedithx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.patient.PatientTemptable"
                ,"care_team_edit_hx"
                ,"cdr.map_predicate_values")

  columnSelect = Map(
    "temptable" -> List("PATIENTID"),
    "care_team_edit_hx" -> List("PAT_ID","LINE","CHANGE_DATETIME","CHANGE_USER_ID","CHANGE_TYPE_C","PROV_ID","EFF_NEW_DT"
              ,"TERMINATION_NEW_DT","LINE_NUM")
  )

  beforeJoin = Map(
    "care_team_edit_hx" -> ((df: DataFrame) => {
      val df1 = df.repartition(500)
      val addColumn = df1.withColumn("STATUS", when(df1("PAT_ID").isNotNull && (df1("TERMINATION_NEW_DT") > current_date() || df1("TERMINATION_NEW_DT").isNull), lit(1))
                                              .otherwise(lit(2)))
      val groups = Window.partitionBy(addColumn("PAT_ID"), addColumn("PROV_ID")).orderBy(addColumn("CHANGE_DATETIME").desc, addColumn("TERMINATION_NEW_DT").desc_nulls_first, addColumn("EFF_NEW_DT").desc)
      val t = addColumn.withColumn("rn", row_number.over(groups))
      val fil = t.filter("rn = 1 AND PAT_ID IS NOT NULL").drop("rn")
      fil.groupBy(fil("PAT_ID")).agg(min(fil("STATUS")).alias("ATTRIBUTEVALUE"))
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("care_team_edit_hx"), dfs("temptable")("PATIENTID") === dfs("care_team_edit_hx")("PAT_ID"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("care_team_edit_hx"),
        "ATTRIBUTE_TYPE_CUI" -> literal("CH002786"),
        "PATIENTID" -> mapFrom("PATIENTID"),
        "ATTRIBUTE_VALUE" -> ((col: String, df: DataFrame) => {
          df.withColumn(col, when(df("ATTRIBUTEVALUE") === lit(1), lit("Yes/Current"))
                            .when(df("ATTRIBUTEVALUE") === lit(2), lit("Past/Former"))
                            .otherwise(lit("No/Never")))
        })
      )

  afterMap = (df: DataFrame) => {
    val inclcol = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CARE_TEAM_EDIT_HX", "PATIENT_ATTRIBUTE", "CARE_TEAM_EDIT_HX", "INCLUSION")
    val fil = df.filter("PATIENTID is not null and 'Y' = " + inclcol + "")
    fil.distinct()
  }

 }



