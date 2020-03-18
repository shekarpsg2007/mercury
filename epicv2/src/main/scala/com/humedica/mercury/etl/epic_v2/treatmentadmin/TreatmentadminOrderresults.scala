package com.humedica.mercury.etl.epic_v2.treatmentadmin

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 02/01/2017
 */


class TreatmentadminOrderresults(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("orderresults",
    "generalorders",
    "cdr.zcm_treatment_type_code",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "orderresults" -> List("ORDER_PROC_ID", "LINE", "RESULT_TIME", "PAT_ID", "PAT_ENC_CSN_ID", "UPDATE_DATE", "COMPONENT_ID", "ORD_VALUE"),
    "generalorders" -> List("PROC_CODE", "ORDER_PROC_ID", "PAT_ID", "PAT_ENC_CSN_ID"),
    "cdr.zcm_treatment_type_code" -> List("GROUPID", "LOCAL_CODE", "TREATMENT_TYPE_CUI", "TREATMENT_TYPE_STD_UNITS", "LOCAL_UNIT")
  )

  beforeJoin = Map(
    "cdr.zcm_treatment_type_code" -> ((df: DataFrame) => {
      includeIf("GROUPID='"+config(GROUP)+"'")(df).drop("GROUPID")
    }),
    "generalorders" -> ((df: DataFrame) => {
      df.withColumnRenamed("PAT_ID", "PAT_ID_go")
        .withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_go")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("orderresults")
      .join(dfs("generalorders"), Seq("ORDER_PROC_ID"))
      .join(dfs("cdr.zcm_treatment_type_code"), concat(lit(config(CLIENT_DS_ID) + "."), dfs("generalorders")("PROC_CODE")) === dfs("cdr.zcm_treatment_type_code")("LOCAL_CODE"))
  }


  afterJoin = (df: DataFrame) => {
    val rdr_component_id = mpvClause(table("cdr.map_predicate_values"),config(GROUP), config(CLIENT_DS_ID), "ORDERRESULTS", "TREATMENTADMIN", "ORDERRESULTS", "COMPONENT_ID")
    val rdr_ord_value = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERRESULTS", "TREATMENTADMIN", "ORDERRESULTS", "ORD_VALUE")
    val fil = df.filter("component_id in (" + rdr_component_id + ") and ord_value in (" + rdr_ord_value + ") and order_proc_id is not null and line is not null " +
      "and result_time is not null and proc_code is not null and (coalesce(pat_id,'-1') <> '-1' or coalesce(pat_id_go,'-1') <> '-1')")
    val groups = Window.partitionBy(fil("ORDER_PROC_ID"), fil("LINE")).orderBy(fil("UPDATE_DATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")

  }


  map = Map(
    "DATASRC" -> literal("orderresults"),
    "ADMINISTERED_ID" -> concatFrom(Seq("ORDER_PROC_ID", "LINE"), delim = "."),
    "ADMINISTERED_DATE" -> mapFrom("RESULT_TIME"),
    "LOCALCODE" -> mapFrom("PROC_CODE", prefix = config(CLIENT_DS_ID) + "."),
    "PATIENTID" -> ((col:String, df: DataFrame) => {
      df.withColumn(col, when(coalesce(df("PAT_ID"), lit("-1")) === lit("-1"), df("PAT_ID_go"))
        .otherwise(df("PAT_ID")))
    }),
    "ORDER_ID" -> mapFrom("ORDER_PROC_ID"),
    "ENCOUNTERID" -> ((col:String, df: DataFrame) => {
      df.withColumn(col, when(coalesce(df("PAT_ENC_CSN_ID"), lit("-1")) === lit("-1"), df("PAT_ENC_CSN_ID_go"))
        .otherwise(df("PAT_ENC_CSN_ID")))
    }),
    "ADMINISTERED_QUANTITY" -> literal("1"),
    "LOCAL_UNIT" -> mapFrom("LOCAL_UNIT"),
    "CUI" -> mapFrom("TREATMENT_TYPE_CUI"),
    "STD_UNIT_CUI" -> mapFrom("TREATMENT_TYPE_STD_UNITS")
  )


}