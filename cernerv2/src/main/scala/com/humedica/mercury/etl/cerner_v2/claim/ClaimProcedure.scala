package com.humedica.mercury.etl.cerner_v2.claim

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class ClaimProcedure(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_nomenclature", "procedure", "encounter", "cdr.map_predicate_values")

  columnSelect = Map(
    "zh_nomenclature" -> List("NOMENCLATURE_ID", "SOURCE_IDENTIFIER", "SOURCE_VOCABULARY_CD"),
    "procedure" -> List("NOMENCLATURE_ID", "PROCEDURE_ID", "PROC_DT_TM", "UNITS_OF_SERVICE",
      "END_EFFECTIVE_DT_TM", "ENCNTR_ID", "UPDT_DT_TM"),
    "encounter" -> List("PERSON_ID", "ENCNTR_ID"),
    "cdr.map_predicate_values" -> List("GROUPID", "DATA_SRC", "ENTITY", "TABLE_NAME", "COLUMN_NAME", "COLUMN_VALUE", "CLIENT_DS_ID")
  )

  beforeJoin = Map(
    "procedure" -> ((df: DataFrame) => {
      df.filter("procedure_id is not null and proc_dt_tm is not null")
    }),
    "encounter" -> ((df: DataFrame) => {
      df.filter("person_id is not null")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("procedure")
      .join(dfs("encounter"), Seq("ENCNTR_ID"), "inner")
      .join(dfs("zh_nomenclature"), Seq("NOMENCLATURE_ID"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("procedure"),
    "CLAIMID" -> mapFrom("PROCEDURE_ID"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "SERVICEDATE" -> mapFrom("PROC_DT_TM"),
    "LOCALCPT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
         when(df("NOMENCLATURE_ID") === lit("0"), null)
        .when(length(df("SOURCE_IDENTIFIER")) === 5, df("SOURCE_IDENTIFIER"))
        .otherwise(null))
    }),
    "QUANTITY" -> mapFrom("UNITS_OF_SERVICE"),
    "MAPPEDCPT" -> ((col: String, df: DataFrame) => {
      val list_snomed = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SNOMED", "CLAIM", "ZH_NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      df.withColumn(col, when(df("NOMENCLATURE_ID") === lit("0") || df("SOURCE_VOCABULARY_CD").isin(list_snomed: _*), null)
        .otherwise(df("SOURCE_IDENTIFIER")))
    }),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "TO_DT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(substring(df("END_EFFECTIVE_DT_TM"), 1, 4) === lit("2100"), null)
        .otherwise(df("END_EFFECTIVE_DT_TM")))
    })
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("CLAIMID"), df("LOCALCPT"), df("LOCALCPTMOD1"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}