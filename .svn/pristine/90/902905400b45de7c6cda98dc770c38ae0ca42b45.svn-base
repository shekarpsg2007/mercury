package com.humedica.mercury.etl.athena.allergies

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.apache.spark.sql.expressions.Window


/**
 * Auto-generated on 09/21/2018
 */


class AllergiesAllergy(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("allergy:athena.util.UtilDedupedAllergy",
    "chart:athena.util.UtilDedupedChart")

  columnSelect = Map(
    "allergy" -> List("ALLERGY_CODE", "ALLERGY_CONCEPT_TYPE", "ONSET_DATE", "CREATED_DATETIME", "PATIENT_ID", "DEACTIVATED_DATETIME",
      "ALLERGY_NAME", "NOTE", "RXNORM_CODE", "HUM_TYPE", "CHART_ID", "ALLERGY_ID", "FILEID"),
    "chart" -> List("CHART_ID", "ENTERPRISE_ID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("allergy")
      .join(dfs("chart"), Seq("CHART_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("allergy"),
    "LOCALALLERGENCD" -> mapFrom("ALLERGY_CODE"),
    "LOCALALLERGENTYPE" -> mapFrom("ALLERGY_CONCEPT_TYPE"),
    "ONSETDATE" -> ((col: String, df: DataFrame) => {
      val df1 = safe_to_date(df, "ONSET_DATE", "ONSET_DATE", "yyyy-MM-dd", 0)
      val df2 = safe_to_date(df1, "CREATED_DATETIME", "CREATED_DATETIME", "yyyy-MM-dd", 0)
      df2.withColumn(col, coalesce(
        when(df2("ONSET_DATE") === to_date(lit("1900-01-01")), null).otherwise(df2("ONSET_DATE")),
        when(df2("CREATED_DATETIME") === to_date(lit("1900-01-01")), null).otherwise(df2("CREATED_DATETIME"))
        )
      )
    }),
    "PATIENTID" -> cascadeFrom(Seq("PATIENT_ID", "ENTERPRISE_ID")),
    "LOCALSTATUS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DEACTIVATED_DATETIME").isNotNull, lit("Inactive")).otherwise(null))
    }),
    "LOCALALLERGENDESC" -> cascadeFrom(Seq("ALLERGY_NAME", "NOTE")),
    "RXNORM_CODE" -> mapFrom("RXNORM_CODE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ALLERGY_ID"), df("HUM_TYPE"))
      .orderBy(df("FILEID"))
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and localallergencd is not null and onsetdate is not null and patientid is not null")
  }

}