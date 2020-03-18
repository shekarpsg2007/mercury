package com.humedica.mercury.etl.epic_v2.microbioorder

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.{EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 02/01/2017
 */


class MicrobioorderGeneralorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_spec_type",
    "generalorders",
    "zh_spec_source", "cdr.map_predicate_values")


  columnSelect = Map(
    "zh_spec_type" -> List("SPECIMEN_TYPE_C", "NAME"),
    "generalorders" -> List("SPECIMEN_SOURCE_C", "PROC_CODE", "ORDER_PROC_ID", "PAT_ID",
      "ORDER_DESCRIPTION", "ORDER_STATUS_C", "SPECIMN_TAKEN_TIME", "ORDER_TIME",
      "SPECIMEN_RECV_TIME", "PAT_ENC_CSN_ID", "ORDERING_DATE", "ORDER_TYPE_C", "SPECIMEN_TYPE_C", "UPDATE_DATE"),
    "zh_spec_source" -> List("NAME", "SPECIMEN_SOURCE_C")
  )

  beforeJoin = Map(
    "generalorders" -> ((df:DataFrame) => {
      val order_type_val = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "GENERALORDERS", "MBORDER", "GENERALORDERS", "ORDER_TYPE_C")
      val proc_code_val = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "GENERALORDERS", "MBORDER", "GENERALORDERS", "PROC_CODE")
      df.filter(
        "PAT_ID is not null AND PAT_ID <> '-1'" +
          "AND coalesce(SPECIMN_TAKEN_TIME,SPECIMEN_RECV_TIME,ORDER_TIME,ORDERING_DATE) is not null " +
          "AND (ORDER_TYPE_C IN (" +order_type_val+ ") OR PROC_CODE IN ("+proc_code_val+"))")
      }),
    "zh_spec_type" -> ((df:DataFrame) => {
      df.withColumnRenamed("NAME", "NAME_ZST")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("generalorders")
      .join(dfs("zh_spec_source"), Seq("SPECIMEN_SOURCE_C"), "left_outer")
      .join(dfs("zh_spec_type"), Seq("SPECIMEN_TYPE_C"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("generalorders"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID", nullIf=Seq("-1")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "MBPROCORDERID" -> mapFrom("ORDER_PROC_ID"),
    "DATECOLLECTED" -> mapFrom("SPECIMN_TAKEN_TIME"),
    "DATERECEIVED" -> mapFrom("SPECIMEN_RECV_TIME"),
    "DATEORDERED" -> mapFrom("ORDER_TIME"),
    "MBORDER_DATE" -> cascadeFrom(Seq("SPECIMN_TAKEN_TIME", "SPECIMEN_RECV_TIME" , "ORDER_TIME", "ORDERING_DATE")),
    "LOCALSPECIMENSOURCE" -> ((col: String, df: DataFrame) => df.withColumn(col,
      coalesce(when(isnull(df("SPECIMEN_SOURCE_C")) && (df("SPECIMEN_SOURCE_C").isin("-1", "0")), null).otherwise(concat(lit(config(CLIENT_DS_ID)+"."), df("SPECIMEN_SOURCE_C")))
        ,when(df("SPECIMEN_TYPE_C").isNotNull, concat(lit(config(CLIENT_DS_ID)+".type."), df("SPECIMEN_TYPE_C"))).otherwise(null)))),
    "LOCALSPECIMENNAME" -> ((col: String, df: DataFrame) => df.withColumn(col,
      coalesce(when(isnull(df("SPECIMEN_SOURCE_C")) && (df("SPECIMEN_SOURCE_C").isin("-1", "0")), null).otherwise(df("NAME")),when(df("SPECIMEN_TYPE_C").isNotNull, df("NAME_ZST")).otherwise(null)))),
    "LOCALORDERSTATUS" -> mapFrom("ORDER_STATUS_C", nullIf=Seq("-1")),
    "LOCALORDERCODE" -> mapFrom("PROC_CODE", nullIf= Seq(null), prefix = config(CLIENT_DS_ID) + "."),
    "LOCALORDERDESC" -> mapFrom("ORDER_DESCRIPTION")
  )



  afterMap = (df:DataFrame) => {
    val groups = Window.partitionBy(df("ORDER_PROC_ID")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rownumber", row_number.over(groups))
    addColumn.filter("rownumber = 1")
  }

}