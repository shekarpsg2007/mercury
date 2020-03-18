package com.humedica.mercury.etl.epic_v2.microbioresult

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.{EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Howie Leicht - 02/23/2017
  */


class MicrobioresultGeneralorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("generalorders",
    "zh_cl_organism",
    "orderssens",
    "zh_antibiotic",
    "zh_suscept",
    "zh_lab_status")


  columnSelect = Map(
    "generalorders" -> List("PAT_ID", "ORDER_PROC_ID", "PAT_ENC_CSN_ID"),
    "zh_cl_organism" -> List("NAME", "ORGANISM_ID"),
    "orderssens" -> List("ANTIBIOTIC_C", "ORGANISM_ID", "SUSCEPT_C", "RESULT_DATE", "ORDER_PROC_ID", "PAT_ID", "PAT_ENC_CSN_ID", "LAB_STATUS_C", "SENSITIVITY_VALUE","LINE", "UPDATE_DATE"),
    "zh_antibiotic" -> List("NAME", "ANTIBIOTIC_C"),
    "zh_suscept" -> List("NAME","SUSCEPT_C"),
    "zh_lab_status" -> List("LAB_STATUS_C", "NAME")
  )


  beforeJoin = Map("orderssens" -> ((df: DataFrame) =>
  {
    df.filter("RESULT_DATE is not null")
      .withColumnRenamed("PAT_ID", "PAT_ID_OS")
      .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_OS")
  }),
    "zh_cl_organism" -> ((df: DataFrame) =>
    {
      df.withColumnRenamed("NAME","NAME_CL")
    }),
    "zh_lab_status" -> ((df: DataFrame) =>
    {
      df.withColumnRenamed("NAME","NAME_LS")
    }),
    "zh_suscept" -> ((df: DataFrame) =>
    {
      df.withColumnRenamed("NAME","NAME_SS")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("orderssens")
      .join(dfs("zh_antibiotic"),Seq("ANTIBIOTIC_C"), "left_outer")
      .join(dfs("zh_cl_organism"),Seq("ORGANISM_ID"), "left_outer")
      .join(dfs("generalorders"),Seq("ORDER_PROC_ID"), "left_outer")
      .join(dfs("zh_suscept"),Seq("suscept_c"), "left_outer")
      .join(dfs("zh_lab_status"), Seq("LAB_STATUS_C"), "left_outer")
  }


  afterJoin = (df:DataFrame) => {
    val groups = Window.partitionBy(df("ORDER_PROC_ID"), df("LINE")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rownumber", row_number.over(groups))
    addColumn.filter("rownumber = 1 and ((PAT_ID_OS is not null and PAT_ID_OS <> '-1') or (PAT_ID is not null and PAT_ID <> '-1'))")
  }


  map = Map(
    "DATASRC" -> literal("generalorders"),
    "DATEAVAILABLE" -> mapFrom("RESULT_DATE"),
    "MBPROCRESULTID" -> ((col:String, df:DataFrame)=> {
      df.withColumn(col, concat_ws("_", df("ORDER_PROC_ID"), df("LINE")))
    }),
    "PATIENTID" -> cascadeFrom(Seq("PAT_ID_OS", "PAT_ID")),
    "ENCOUNTERID" -> ((col:String, df:DataFrame)=> df.withColumn(col, when(df("PAT_ENC_CSN_ID_OS") === "-1", null)
      .otherwise(coalesce(df("PAT_ENC_CSN_ID_OS"),df("PAT_ENC_CSN_ID"))))),
    "LOCALANTIBIOTICCODE" -> ((col:String, df:DataFrame)=>df.withColumn(col, when(isnull(df("ANTIBIOTIC_C")), null)
      .otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("ANTIBIOTIC_C"))))),
    "LOCALANTIBIOTICNAME" -> mapFrom("NAME"),
    "LOCALORGANISMCODE" -> mapFrom("ORGANISM_ID", nullIf=Seq("-1", null), prefix=config(CLIENT_DS_ID)+"."),
    "LOCALORGANISMNAME" -> ((col:String, df:DataFrame)=>df.withColumn(col, when(df("ORGANISM_ID") === "-1", null)
      .otherwise(df("NAME_CL")))),
    "LOCALSENSITIVITY" -> ((col:String, df:DataFrame)=>df.withColumn(col,
      when(isnull(df("SUSCEPT_C")) && (df("SUSCEPT_C") === "-1"), null)
        .otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("SUSCEPT_C"))))),
    "LOCALRESULSTATUS" -> ((col:String, df:DataFrame)=>df.withColumn(col, when(df("LAB_STATUS_C") === "-1", null)
      .otherwise(df("NAME_LS")))),
    "LOCALSENSITIVITY_VALUE" -> mapFrom("SENSITIVITY_VALUE"),
    "MBPROCORDERID" -> mapFrom("ORDER_PROC_ID"),
    "MBRESULT_DATE" -> mapFrom("RESULT_DATE"),
    "LOCALSENSITIVITYNAME" -> mapFrom("NAME_SS")
  )
}

// TEST CODE
// val m = new MicrobioresultGeneralorders(cfg); val mbo = build(m) ; mbo.show ; mbo.count
