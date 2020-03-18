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


class MicrobioresultClinicalevent(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "ce_susceptibility", "ce_microbiology", "zh_code_value", "cdr.map_predicate_values")

  columnSelect = Map(
    "clinical_event" -> List("PERSON_ID", "ENCNTR_ID", "ORDER_ID", "EVENT_ID", "RESULT_STATUS_CD", "EVENT_CLASS_CD", "UPDT_DT_TM"),
    "ce_susceptibility" -> List("ANTIBIOTIC_CD", "SUSCEPTIBILITY_STATUS_CD", "RESULT_CD", "RESULT_TEXT_VALUE", "EVENT_ID",
      "MICRO_SEQ_NBR", "CE_UPDT_DT_TM"),
    "ce_microbiology" -> List("VALID_FROM_DT_TM", "EVENT_ID", "ORGANISM_CD", "MICRO_SEQ_NBR", "UPDT_DT_TM"),
    "zh_code_value" -> List("CODE_VALUE", "CODE_SET", "DESCRIPTION")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_result_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "MBORDER", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      val list_event_class_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "MBORDER", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      val fil = df.filter("order_id <> '0' and event_class_cd in (" + list_event_class_cd + ")")
      val groups = Window.partitionBy(fil("ORDER_ID"), fil("EVENT_ID"))
        .orderBy(fil("UPDT_DT_TM").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1 and (result_status_cd is null or result_status_cd not in (" + list_result_status_cd + "))")
        .drop("rn", "UPDT_DT_TM")
    }),
    "ce_microbiology" -> ((df: DataFrame) => {
      df.filter("coalesce(organism_cd, '0') <> '0'")
    }),
    "ce_susceptibility" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("EVENT_ID"), df("MICRO_SEQ_NBR"))
        .orderBy(df("CE_UPDT_DT_TM").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    val sens_cd_set = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "LOCALSENSITIVITY", "MBRESULT", "ZH_CODE_VALUE", "CODE_SET")

    val zh = table("zh_code_value")
      .filter("code_set = '1011'")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_anti")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_anti")
      .drop("CODE_SET")
    val zh1 = table("zh_code_value")
      .filter("code_set = '1021'")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_org")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_org")
      .drop("CODE_SET")
    val zh2 = table("zh_code_value")
      .filter("code_set in (" + sens_cd_set + ")")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_result")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_result")
      .drop("CODE_SET")
    val zh3 = table("zh_code_value")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_sus")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_sus")
      .drop("CODE_SET")

    dfs("clinical_event")
      .join(dfs("ce_microbiology"), Seq("EVENT_ID"), "inner")
      .join(dfs("ce_susceptibility"), Seq("EVENT_ID", "MICRO_SEQ_NBR"), "left_outer")
      .join(zh, dfs("ce_susceptibility")("ANTIBIOTIC_CD") === zh("CODE_VALUE_anti"), "left_outer")
      .join(zh1, dfs("ce_microbiology")("ORGANISM_CD") === zh1("CODE_VALUE_org"), "left_outer")
      .join(zh2, dfs("ce_susceptibility")("RESULT_CD") === zh2("CODE_VALUE_result"), "left_outer")
      .join(zh3, dfs("ce_susceptibility")("SUSCEPTIBILITY_STATUS_CD") === zh3("CODE_VALUE_sus"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("clinical_event"),
    "DATEAVAILABLE" -> mapFrom("VALID_FROM_DT_TM"),
    "MBRESULT_DATE" -> mapFrom("VALID_FROM_DT_TM"),
    "MBPROCRESULTID" -> concatFrom(Seq("EVENT_ID", "MICRO_SEQ_NBR"), delim = "_"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALANTIBIOTICCODE" -> mapFrom("ANTIBIOTIC_CD", prefix = config(CLIENT_DS_ID) + "."),
    "LOCALANTIBIOTICNAME" -> mapFrom("DESCRIPTION_anti"),
    "LOCALORGANISMCODE" -> mapFrom("ORGANISM_CD", prefix = config(CLIENT_DS_ID) + "."),
    "LOCALORGANISMNAME" -> mapFrom("DESCRIPTION_org"),
    "LOCALRESULSTATUS" -> mapFrom("DESCRIPTION_sus"),
    "LOCALSENSITIVITY" -> mapFrom("RESULT_CD", prefix = config(CLIENT_DS_ID) + ".", nullIf = Seq("0")),
    "LOCALSENSITIVITYNAME" -> mapFrom("DESCRIPTION_result"),
    "LOCALSENSITIVITY_VALUE" -> mapFrom("RESULT_TEXT_VALUE"),
    "MBPROCORDERID" -> mapFrom("ORDER_ID")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("MBPROCRESULTID"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}