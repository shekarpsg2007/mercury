package com.humedica.mercury.etl.asent.labresult

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class LabresultResults(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_unit_code_de",
    "as_zc_result_status_de",
    "as_zc_qo_classification_de",
    "as_results",
    "cdr.map_predicate_values",
    "as_zh_source_de")

  columnSelect = Map(
    "as_zh_source_de" -> List("ENTRYNAME", "ID"),
    "as_zc_unit_code_de" -> List("ENTRYMNEMONIC", "ID"),
    "as_zc_result_status_de" -> List("ENTRYNAME", "ID"),
    "as_zc_qo_classification_de" -> List("ENTRYNAME", "ID"),
    "as_results" -> List("RESULT_CLINICAL_DATE", "PARENT_RESULT_ID", "RESULT_ID", "PATIENT_MRN", "ENCOUNTER_ID",
      "RESULT_NAME", "SOURCE_DE", "UNITS", "REFERENCE_RANGE", "ABNORMAL_FLAG",
      "RESULT_STATUS_ID", "RESULT_CLINICAL_DATE", "PERFORMED_DATE", "RESULT_DATE", "CURRENT_ID",
      "ANSWER_DE_T", "UNITS_DE", "QO_CLASSIFICATION_DE", "RESULT_VALUE", "LAST_UPDATED_DATE",
      "CHILD_RESULT_ID")
  )

  beforeJoin = Map(
    "as_results" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PARENT_RESULT_ID")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last, df("RESULT_DATE").desc_nulls_last, df("CHILD_RESULT_ID").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1 and RESULT_STATUS_ID NOT IN ('6','3')").drop("rn")
        .withColumn("LOCALRESULT", coalesce(df("ANSWER_DE_T"), df("RESULT_VALUE")))
    }),
    "as_zc_result_status_de" -> ((df: DataFrame) => {
      df.withColumnRenamed("ENTRYNAME", "ENTRYNAME_res")
    }),
    "as_zh_source_de" -> ((df: DataFrame) => {
      df.withColumnRenamed("ENTRYNAME", "ENTRYNAME_src")
    }),
    "as_zc_qo_classification_de" -> ((df: DataFrame) => {
      df.withColumnRenamed("ENTRYNAME", "ENTRYNAME_cl")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_results")
      .join(dfs("as_zc_qo_classification_de"), dfs("as_zc_qo_classification_de")("ID") === dfs("as_results")("qo_classification_de"), "left_outer")
      .join(dfs("as_zc_result_status_de"), dfs("as_zc_result_status_de")("ID") === dfs("as_results")("result_status_id"), "left_outer")
      .join(dfs("as_zc_unit_code_de"), dfs("as_zc_unit_code_de")("ID") === dfs("as_results")("units_de"), "left_outer")
      .join(dfs("as_zh_source_de"), dfs("as_zh_source_de")("ID") === dfs("as_results")("SOURCE_DE"), "left_outer")

  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PARENT_RESULT_ID")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last, df("RESULT_DATE").desc_nulls_last, df("CHILD_RESULT_ID").desc_nulls_last)
    val dedup = df.withColumn("rn2", row_number.over(groups)).filter("rn2 = 1")

    dedup.withColumn("LOCALRESULT_25", when(!dedup("LOCALRESULT").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
      when(locate(" ", dedup("LOCALRESULT"), 25) === 0, expr("substr(LOCALRESULT,1,length(LOCALRESULT))"))
        .otherwise(expr("substr(LOCALRESULT,1,locate(' ', LOCALRESULT, 25))"))).otherwise(null))
      .withColumn("LOCALRESULT_NUMERIC", when(dedup("LOCALRESULT").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), dedup("LOCALRESULT")).otherwise(null))

  }


  map = Map(
    "DATASRC" -> literal("results"),
    "LABRESULTID" -> mapFrom("PARENT_RESULT_ID"),
    "LABORDERID" -> mapFrom("CURRENT_ID"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "DATECOLLECTED" -> ((col, df) => {
      val useResClinDate = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "RESULTS", "LABRESULT", "AS_RESULTS", "RESULT_CLINICAL_DATE")
      df.withColumn(col, when(lit(useResClinDate) === lit("'Y'"), df("RESULT_CLINICAL_DATE")).otherwise(df("PERFORMED_DATE")))
    }),
    "DATEAVAILABLE" -> mapFrom("RESULT_DATE"),
    "RESULTSTATUS" -> mapFrom("ENTRYNAME_res"),
    "LOCALRESULT" -> mapFrom("LOCALRESULT"),
    "LOCALRESULT_NUMERIC" -> mapFrom("LOCALRESULT_NUMERIC"),
    "LOCALRESULT_INFERRED" -> extract_value(),
    "LOCALUNITS_INFERRED" -> labresults_extract_uom(),
    "LOCALCODE" -> mapFrom("RESULT_ID"),
    "LOCALNAME" -> mapFrom("RESULT_NAME"),
    "NORMALRANGE" -> mapFrom("REFERENCE_RANGE"),
    "LOCALUNITS" -> ((col, df) => df.withColumn(col, trim(lower(coalesce(df("ENTRYMNEMONIC"), when(df("UNITS_DE") === lit("0"), df("UNITS")).otherwise(df("UNITS_DE"))))))),
    "LOCALSPECIMENTYPE" -> mapFrom("ENTRYNAME_src"),
    "LOCALTESTNAME" -> mapFrom("ENTRYNAME_cl"),
    "RELATIVEINDICATOR" -> labresults_extract_relativeindicator(),
    "STATUSCODE" -> mapFrom("RESULT_STATUS_ID"),
    "LABRESULT_DATE" -> mapFrom("RESULT_DATE"),
    "RESULTTYPE" -> labresults_extract_resulttype("LOCALRESULT_25", "ABNORMAL_FLAG")

  )
}