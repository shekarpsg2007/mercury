package com.humedica.mercury.etl.cerner_v2.microbioresult

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class MicrobioresultOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("mic_sus_med_result", "orders", "mic_task_log", "zh_code_value", "cdr.map_predicate_values")

  columnSelect = Map(
    "mic_sus_med_result" -> List("TASK_LOG_ID", "RESULT_SEQ", "DETAIL_SUS_SEQ", "UPDT_DT_TM", "PANEL_MEDICATION_CD",
      "STATUS_CD", "RESULT_CD"),
    "orders" -> List("PERSON_ID", "ENCNTR_ID", "ORDER_ID", "UPDT_DT_TM", "ACTIVITY_TYPE_CD"),
    "mic_task_log" -> List("TASK_DT_TM", "TASK_LOG_ID", "ORGANISM_CD", "ORDER_ID", "UPDT_DT_TM"),
    "zh_code_value" -> List("CODE_VALUE", "CODE_SET", "DESCRIPTION")
  )

  beforeJoin = Map(
    "orders" -> ((df: DataFrame) => {
      val list_activity_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "MBRESULT", "ORDERS", "ACTIVITY_TYPE_CD")
      val fil = df.filter("activity_type_cd in (" + list_activity_type_cd + ")")
      val groups = Window.partitionBy(fil("ORDER_ID")).orderBy(fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM")
    }),
    "mic_task_log" -> ((df: DataFrame) => {
      val fil = df.filter("coalesce(organism_cd, '0') <> '0'")
      val groups = Window.partitionBy(fil("TASK_LOG_ID")).orderBy(fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM")
    }),
    "mic_sus_med_result" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("TASK_LOG_ID"), df("RESULT_SEQ"), df("DETAIL_SUS_SEQ"))
        .orderBy(df("UPDT_DT_TM").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    val zh_org = table("zh_code_value")
      .filter("code_set = '1021'")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_org")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_org")
      .drop("CODE_SET")
    val zh_anti = table("zh_code_value")
      .filter("code_set = '1011'")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_anti")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_anti")
      .drop("CODE_SET")
    val zh_sts = table("zh_code_value")
      .filter("code_set = '1901'")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_sts")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_sts")
      .drop("CODE_SET")
    val zh_sens = table("zh_code_value")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_sens")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_sens")

    dfs("orders")
      .join(dfs("mic_task_log"), Seq("ORDER_ID"), "inner")
      .join(dfs("mic_sus_med_result"), Seq("TASK_LOG_ID"), "left_outer")
      .join(zh_org, dfs("mic_task_log")("ORGANISM_CD") === zh_org("CODE_VALUE_org"), "left_outer")
      .join(zh_anti, dfs("mic_sus_med_result")("PANEL_MEDICATION_CD") === zh_anti("CODE_VALUE_anti"), "left_outer")
      .join(zh_sts, dfs("mic_sus_med_result")("STATUS_CD") === zh_sts("CODE_VALUE_sts"), "left_outer")
      .join(zh_sens, dfs("mic_sus_med_result")("RESULT_CD") === zh_sens("CODE_VALUE_sens"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val sens_cd_set = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "LOCALSENSITIVITY", "MBRESULT", "ZH_CODE_VALUE", "CODE_SET")
    val groups = Window.partitionBy(df("TASK_LOG_ID"), df("RESULT_SEQ"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    val dedupe = df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
    dedupe.groupBy(dedupe("PERSON_ID"), dedupe("ENCNTR_ID"), dedupe("ORDER_ID"), dedupe("TASK_DT_TM"), dedupe("ORGANISM_CD"),
      dedupe("DESCRIPTION_org"), dedupe("TASK_LOG_ID"), dedupe("RESULT_SEQ"), dedupe("PANEL_MEDICATION_CD"), dedupe("UPDT_DT_TM"))
      .agg(
        max(dedupe("PANEL_MEDICATION_CD")).alias("LOCALANTIBIOTICCODE"),
        max(dedupe("DESCRIPTION_anti")).alias("LOCALANTIBIOTICNAME"),
        max(dedupe("DESCRIPTION_sts")).alias("LOCALRESULTSTATUS"),
        max(when(dedupe("CODE_SET") === lit("64"), dedupe("RESULT_CD"))
            .otherwise(null))
          .alias("LOCALSENSITIVITY"),
        max(when(dedupe("CODE_SET").isin(sens_cd_set: _*), dedupe("DESCRIPTION_sens"))
            .otherwise(null))
          .alias("LOCALSENSITIVITYNAME"),
        max(when(dedupe("CODE_SET") === lit("1025"), dedupe("DESCRIPTION_sens"))
            .otherwise(null))
          .alias("LOCALSENSITIVITY_VALUE")
      )
  }

  map = Map(
    "DATASRC" -> literal("orders"),
    "DATEAVAILABLE" -> mapFrom("TASK_DT_TM"),
    "MBRESULT_DATE" -> mapFrom("TASK_DT_TM"),
    "MBPROCRESULTID" -> concatFrom(Seq("TASK_LOG_ID", "RESULT_SEQ"), delim = "_"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALANTIBIOTICCODE" -> mapFrom("LOCALANTIBIOTICCODE", prefix = config(CLIENT_DS_ID) + "."),
    "LOCALORGANISMCODE" -> mapFrom("ORGANISM_CD", prefix = config(CLIENT_DS_ID) + "."),
    "LOCALORGANISMNAME" -> mapFrom("DESCRIPTION_org"),
    "LOCALSENSITIVITY" -> mapFrom("LOCALSENSITIVITY", prefix = config(CLIENT_DS_ID) + "."),
    "MBPROCORDERID" -> mapFrom("ORDER_ID")
  )

}