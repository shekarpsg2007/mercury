package com.humedica.mercury.etl.asent.procedure

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ProcedureOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_qo_classification_de",
    "as_orders",
    "as_results",
    "cdr.map_custom_proc",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "as_zc_qo_classification_de" -> List("ENTRYNAME", "CPT4CODE", "ID"),
    "as_orders" -> List("ORDER_ITEM_ID", "CLINICAL_DATETIME", "PATIENT_MRN", "ENCOUNTER_ID", "ORDER_STATUS_ID",
      "CPT4CODE", "ORDERING_PROVIDER_ID", "LAST_UPDATED_DATE", "SOURCE_ID", "DONEFUZZYSORTAS"),
    "as_results" -> List("RESULT_DATE", "PATIENT_MRN", "QO_CLASSIFICATION_DE", "ENCOUNTER_ID", "LAST_UPDATED_DATE"),
    "cdr.map_custom_proc" -> List("LOCALCODE", "GROUPID", "DATASRC", "MAPPEDVALUE")
  )

  beforeJoin = Map(
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUPID) + "' AND DATASRC = 'orders'").withColumnRenamed("LOCALCODE", "LOCALCODE_mcp")
    }),
    "as_results" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("QO_CLASSIFICATION_DE"))
        .orderBy(df("LAST_UPDATED_DATE").desc, df("RESULT_DATE").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val fil = addColumn.filter("rn=1").drop("rn")
      fil.withColumnRenamed("ENCOUNTER_ID", "ENCOUNTER_ID_res").withColumnRenamed("PATIENT_MRN", "PATIENT_MRN_res")
    }),
    "as_orders" -> ((df: DataFrame) => {
      val fil = df.filter("ORDER_ITEM_ID is not null AND CLINICAL_DATETIME != '20111111'")
      fil.withColumnRenamed("CPT4CODE", "CPT4CODE_ord").withColumnRenamed("LAST_UPDATED_DATE", "LAST_UPDATED_DATE_ord")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_orders")
      .join(dfs("as_zc_qo_classification_de"), dfs("as_zc_qo_classification_de")("id") === dfs("as_orders")("order_item_id"), "left_outer")
      .join(dfs("cdr.map_custom_proc"), dfs("cdr.map_custom_proc")("LOCALCODE_mcp") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_orders")("order_item_id")), "left_outer")
      .join(dfs("as_results"), dfs("as_orders")("order_item_id") === dfs("as_results")("qo_classification_de")
        && (dfs("as_results")("PATIENT_MRN_res") === dfs("as_orders")("PATIENT_MRN"))
        && (dfs("as_results")("ENCOUNTER_ID_res") === dfs("as_orders")("encounter_id")), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val incl_status_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "INCLUSION_STATUS", "PROCEDUREDO", "AS_ORDERS", "ORDER_STATUS_ID")
    val fil = df.filter("ORDER_STATUS_ID in (" + incl_status_id + ") AND (LOCALCODE_mcp is not null OR (length(CPT4CODE)= 5 OR (length(CPT4CODE) > 5 AND CPT4CODE rlike '^\\d{5}')) OR CPT4CODE_ord is not null)")
    val groups2 = Window.partitionBy(fil("ENCOUNTER_ID"), fil("CLINICAL_DATETIME"), fil("ORDER_ITEM_ID"), fil("MAPPEDVALUE"))
      .orderBy(fil("LAST_UPDATED_DATE_ord").desc_nulls_last)
    val addColumn2 = fil.withColumn("rn2", row_number.over(groups2))
    addColumn2.filter("rn2 = 1").drop("rn2")
  }
  
  afterJoinExceptions = Map(
    "H984926_AS ENT" -> ((df: DataFrame) => {
      val fil = df.filter("(ORDER_STATUS_ID = '3' AND (LOCALCODE_mcp is not null OR (length(CPT4CODE)= 5 OR (length(CPT4CODE) > 5 AND CPT4CODE rlike '^\\d{5}')) OR CPT4CODE_ord is not null))" +
        "or (ORDER_STATUS_ID in (3,20) and MAPPEDVALUE = 'EYEEXAM')")
      val groups2 = Window.partitionBy(fil("ENCOUNTER_ID"), fil("CLINICAL_DATETIME"), fil("ORDER_ITEM_ID"), fil("MAPPEDVALUE"))
        .orderBy(fil("LAST_UPDATED_DATE_ord").desc_nulls_last)
      val addColumn2 = fil.withColumn("rn2", row_number.over(groups2))
      addColumn2.filter("rn2 = 1").drop("rn2")
    })
  )
  
  map = Map(
    "DATASRC" -> literal("orders"),
    "LOCALCODE" -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("ORDER_ITEM_ID")))),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "PROCEDUREDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ORDER_ITEM_ID").isin(mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "PROCEDURE", "AS_ORDERS", "ORDER_ITEM_ID")) && df("DONEFUZZYSORTAS").notEqual("1900-01-01"), df("DONEFUZZYSORTAS")).otherwise(df("CLINICAL_DATETIME")))
    }),
    "LOCALNAME" -> mapFrom("ENTRYNAME"),
    "ORDERINGPROVIDERID" -> mapFrom("ORDERING_PROVIDER_ID"),
    "ACTUALPROCDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ORDER_ITEM_ID").isin(mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "PROCEDURE", "AS_ORDERS", "ORDER_ITEM_ID")) && df("DONEFUZZYSORTAS").notEqual("1900-01-01"), df("DONEFUZZYSORTAS")).otherwise(df("CLINICAL_DATETIME")))
    }),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PERFORMINGPROVIDERID" -> mapFrom("ORDERING_PROVIDER_ID"),
    "SOURCEID" -> mapFrom("SOURCE_ID"),
    "MAPPEDCODE" -> ((col, df) => df.withColumn(col, when(df("LOCALCODE_mcp").isNotNull, df("MAPPEDVALUE"))
      .otherwise(coalesce(substring(df("CPT4CODE"), 1, 5), df("CPT4CODE_ord"))))),
    "CODETYPE" -> ((col, df) => df.withColumn(col, when(df("LOCALCODE_mcp").isNotNull, lit("CUSTOM")).otherwise(
      when(substring(df("CPT4CODE"), 1, 5).rlike("^[0-9]{4}[0-9A-Z]$"), lit("CPT4")).otherwise(
        when(substring(df("CPT4CODE"), 1, 5).rlike("^[A-Z]{1,1}[0-9]{4}$"), lit("HCPCS")).otherwise(
          when(substring(df("CPT4CODE"), 1, 5).rlike("^[0-9]{2,2}\\.[0-9]{1,2}$"), lit("ICD9")).otherwise(null))))))
  )

  afterMap = (df: DataFrame) => {
    df.dropDuplicates("LOCALCODE", "PATIENTID", "PROCEDUREDATE", "LOCALNAME", "ORDERINGPROVIDERID", "ACTUALPROCDATE", "ENCOUNTERID", "PERFORMINGPROVIDERID", "SOURCEID", "MAPPEDCODE")
  }

}