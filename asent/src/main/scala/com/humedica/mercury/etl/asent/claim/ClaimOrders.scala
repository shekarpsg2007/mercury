package com.humedica.mercury.etl.asent.claim

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ClaimOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_orders",
    "as_results",
    "as_zc_qo_classification_de",
    "cdr.map_predicate_values",
    "as_charges",
    "as_zc_charge_code_de")

  columnSelect = Map(
    "as_orders" -> List("ENCOUNTER_ID", "PATIENT_MRN", "CPT4CODE", "CPT4CODE", "ORDER_ITEM_ID", "CLINICAL_DATETIME", "ORDER_STATUS_ID", "LAST_UPDATED_DATE"),
    "as_results" -> List("RESULT_DATE", "PATIENT_MRN", "ENCOUNTER_ID", "QO_CLASSIFICATION_DE"),
    "as_zc_qo_classification_de" -> List("ID", "CPT4CODE"),
    "as_charges" -> List("CHARGE_CODE_ID", "BILLING_STATUS", "ENCOUNTER_ID"),
    "as_zc_charge_code_de" -> List("ID", "CPT4CODE", "ENTRYCODE")
  )

  beforeJoin = Map(
    "as_orders" -> ((df: DataFrame) => {
      val list_order_status_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLAIM_ORDERS", "CLAIM", "AS_ORDERS", "ORDER_STATUS_ID")
      val fil = df.filter("ENCOUNTER_ID is not null and PATIENT_MRN is not null and ORDER_STATUS_ID in (" + list_order_status_id + ")")
      fil.withColumnRenamed("ENCOUNTER_ID", "ENCOUNTER_ID_ord").withColumnRenamed("PATIENT_MRN", "PATIENT_MRN_ord")

    }),
    "as_zc_qo_classification_de" -> ((df: DataFrame) => {
      df.withColumnRenamed("CPT4CODE", "CPT4CODE_zc")
    }),
    "as_charges" -> ((df: DataFrame) => {
      df.withColumnRenamed("ENCOUNTER_ID", "ENCOUNTER_ID_c")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_orders")
      .join(dfs("as_results"), dfs("as_results")("PATIENT_MRN") === dfs("as_orders")("PATIENT_MRN_ord") && dfs("as_results")("ENCOUNTER_ID") === dfs("as_orders")("ENCOUNTER_ID_ord") && dfs("as_results")("QO_CLASSIFICATION_DE") === dfs("as_orders")("ORDER_ITEM_ID"), "left_outer")
      .join(dfs("as_zc_qo_classification_de"), dfs("as_zc_qo_classification_de")("ID") === dfs("as_orders")("ORDER_ITEM_ID"), "left_outer")
  }

  afterJoin = (df1: DataFrame) => {
    val df = df1.repartition(1000)
    val addColumn = df.withColumn("CPT_COL", coalesce(df("CPT4CODE_zc"), df("CPT4CODE"))).withColumn("EXCL_COL", concat_ws("", coalesce(df("CPT4CODE"), lit("X")), df("ENCOUNTER_ID_ord")))
    val fil1 = addColumn.filter("length(CPT_COL) = 5 or (length(CPT_COL) > 5 and rlike(CPT_COL, '^\\d{5}'))")

    val c = table("as_charges").filter("BILLING_STATUS not in ('R','C')")
    val zc2 = table("as_zc_charge_code_de").filter("CPT4CODE is not null")
    val joined = c.join(zc2, c("CHARGE_CODE_ID") === zc2("ID"), "left_outer").withColumn("FIL_COL", concat_ws("", coalesce(zc2("ENTRYCODE"), lit("Y")), c("ENCOUNTER_ID_c")))

    val fil2 = fil1.join(joined, fil1("EXCL_COL") === joined("FIL_COL"), "left_anti")

    val groups = Window.partitionBy(fil2("ENCOUNTER_ID_ord"), fil2("CPT4CODE")).orderBy(fil2("LAST_UPDATED_DATE").desc_nulls_last)
    val addColumn2 = fil2.withColumn("rn", row_number.over(groups))
    addColumn2.filter("rn = 1")
  }

  map = Map(
    "DATASRC" -> literal("orders"),
    "CLAIMID" -> mapFrom("ENCOUNTER_ID_ord"),
    "PATIENTID" -> mapFrom("PATIENT_MRN_ord"),
    "SERVICEDATE" -> ((col: String, df: DataFrame) => {
      val useResDate = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROCEDUREDATE_ORDERS", "PROCEDUREDO", "AS_RESULTS", "RESULT_DATE")
      df.withColumn(col, when(lit("'Y'") === useResDate, coalesce(df("RESULT_DATE"), df("CLINICAL_DATETIME"))).otherwise(df("CLINICAL_DATETIME")))
    }),
    "LOCALCPT" -> mapFrom("CPT4CODE"),
    "MAPPEDCPT" -> mapFrom("CPT4CODE"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID_ord")
  )

  afterMap = (df: DataFrame) => {
    df.filter("SERVICEDATE is not null")
  }

}