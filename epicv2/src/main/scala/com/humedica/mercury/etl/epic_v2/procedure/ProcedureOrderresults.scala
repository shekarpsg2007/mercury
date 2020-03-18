package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



class ProcedureOrderresults(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("orderresults",
                   "zh_clarity_comp",
                   "cdr.map_custom_proc",
                   "generalorders")

    columnSelect = Map(
      "orderresults" -> List("COMPONENT_ID", "PAT_ID", "RESULT_TIME", "LINE", "PAT_ENC_CSN_ID", "RESULT_STATUS_C", "UPDATE_DATE","ORDER_PROC_ID"),
      "zh_clarity_comp" -> List("NAME", "COMPONENT_ID"),
      "generalorders" -> List("PAT_ID","PAT_ENC_CSN_ID","ORDER_PROC_ID")
    )

  beforeJoin = Map(
    "orderresults" -> ((df: DataFrame) => {
      val fil = df.filter("COMPONENT_ID is not null and RESULT_TIME is not null and (RESULT_STATUS_C  in ('3','4', '-1') or RESULT_STATUS_C is null)")
        fil.withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_or")
           .withColumnRenamed("PAT_ID", "PAT_ID_or")
           .withColumnRenamed("ORDER_PROC_ID", "ORDER_PROC_ID_or")
    }),
    "generalorders" -> ((df: DataFrame) => {
      df.withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_go")
        .withColumnRenamed("PAT_ID", "PAT_ID_go")
        .withColumnRenamed("ORDER_PROC_ID", "ORDER_PROC_ID_go")
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'orderresults'").drop("GROUPID")
    }),
    "zh_clarity_comp" -> ((df: DataFrame) => {
      df.withColumnRenamed("COMPONENT_ID", "COMPONENT_ID_zcc")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("orderresults")
      .join(dfs("cdr.map_custom_proc"), concat(lit(config(CLIENT_DS_ID)+"."), dfs("orderresults")("COMPONENT_ID")) === dfs("cdr.map_custom_proc")("LOCALCODE"), "inner")
      .join(dfs("zh_clarity_comp"), dfs("orderresults")("COMPONENT_ID") === dfs("zh_clarity_comp")("COMPONENT_ID_zcc"), "inner")
      .join(dfs("generalorders"), dfs("orderresults")("ORDER_PROC_ID_or") === dfs("generalorders")("ORDER_PROC_ID_go"), "left_outer")
  }

      map = Map(
        "DATASRC" -> literal("orderresults"),
        "LOCALCODE" -> mapFrom("COMPONENT_ID",prefix=config(CLIENT_DS_ID)+"."),
   //     "PATIENTID" -> mapFrom("PAT_ID"),
        "PATIENTID" -> ((col: String, df: DataFrame) => {
          df.withColumn(col, coalesce(
            when(df("PAT_ID_or") === lit("-1"), null).otherwise(df("PAT_ID_or")),
            when(df("PAT_ID_go") === lit("-1"), null).otherwise(df("PAT_ID_go"))
          ))
        }),
        "PROCEDUREDATE" -> mapFrom("RESULT_TIME"),
        "LOCALNAME" -> mapFrom("NAME"),
        "PROCSEQ" -> mapFrom("LINE"),
        //"ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
        "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
          df.withColumn(col, coalesce(
            when(df("PAT_ENC_CSN_ID_or") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_or")),
            when(df("PAT_ENC_CSN_ID_go") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_go"))
          ))
        }),
        "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
        "CODETYPE" -> literal("CUSTOM")
      )


  afterMap = (df1: DataFrame) => {
    val df=df1.repartition(1000)
    val fil = df.filter("PATIENTID is not null")
    val groups = Window.partitionBy(fil("ENCOUNTERID"), fil("PATIENTID"), fil("COMPONENT_ID"), fil("RESULT_TIME")).orderBy(fil("UPDATE_DATE").desc,fil("RESULT_TIME").desc)
    val addColumn = fil.withColumn("rw", row_number.over(groups))
    addColumn.filter("rw=1")
  }


 }

// test

// val a = new ProcedureOrderresults(cfg); val b = build(a); b.show(false) ; b.count ; b.select("ENCOUNTERID").distinct.count
