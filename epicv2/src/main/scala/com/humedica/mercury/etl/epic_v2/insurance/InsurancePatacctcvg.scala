package com.humedica.mercury.etl.epic_v2.insurance

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Howie Leicht - 02-23-17
  */


class InsurancePatacctcvg(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_payor",
    "zh_beneplan",
    "coverage",
    "pat_acct_cvg",
    "zh_epp_map")


  columnSelect = Map(

    "coverage" -> List("COVERAGE_ID", "UPDATE_DATE", "SUBSCR_NUM", "GROUP_NUM"),
    "pat_acct_cvg" -> List("UPDATE_DATE", "PAT_ID", "FIN_CLASS", "LINE", "PAYOR_ID", "PLAN_ID", "FILEID", "COVERAGE_ID"),
    "zh_beneplan" -> List("BENEFIT_PLAN_ID", "BENEFIT_PLAN_NAME"),
    "zh_payor" -> List("PAYOR_ID", "PAYOR_NAME"),
    "zh_epp_map" -> List("CID", "INTERNAL_ID")
  )


  beforeJoin = Map(
    "coverage" -> ((df: DataFrame) => {
      val df1 = df.repartition(10000)
      val groups = Window.partitionBy(df1("COVERAGE_ID")).orderBy(df1("UPDATE_DATE").desc)
      val addColumn = df1.withColumn("rownumber", row_number.over(groups))
      addColumn.filter("rownumber = 1").drop("UPDATE_DATE")
    }),
    "pat_acct_cvg" -> ((df: DataFrame) => {
      val df1 = df.repartition(10000)
      df1.filter("UPDATE_DATE is not null and PAT_ID is not null and PAT_ID <> '-1'")
    }),
    "zh_epp_map" -> ((df: DataFrame) => {
      val df1 = df.dropDuplicates()
      df1.repartition(100)
    }),
    "zh_beneplan" -> ((df: DataFrame) => {
      df.repartition(100)
    }),
    "zh_payor" -> ((df: DataFrame) => {
      df.repartition(100)
    })


  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("pat_acct_cvg")
      .join(dfs("coverage"), Seq("COVERAGE_ID"), "left_outer")
      .join(dfs("zh_beneplan"), dfs("pat_acct_cvg")("PLAN_ID") === dfs("zh_beneplan")("BENEFIT_PLAN_ID"), "left_outer")
      .join(dfs("zh_payor"), Seq("PAYOR_ID"), "left_outer")
  }


  joinExceptions = Map(
    "H406239_EP2" -> ((dfs: Map[String, DataFrame]) => {
      dfs("pat_acct_cvg")
        .join(dfs("coverage"), Seq("COVERAGE_ID"), "left_outer")
        .join(dfs("zh_beneplan"), dfs("pat_acct_cvg")("PLAN_ID") === dfs("zh_beneplan")("BENEFIT_PLAN_ID"), "left_outer")
        .join(dfs("zh_epp_map"), dfs("zh_beneplan")("BENEFIT_PLAN_ID") === dfs("zh_epp_map")("CID"), "left_outer")
        .join(dfs("zh_payor"), Seq("PAYOR_ID"), "left_outer")
    })
  )


  map = Map(
    "DATASRC" -> literal("pat_acct_cvg"),
    "INS_TIMESTAMP" -> mapFrom("UPDATE_DATE"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "GROUPNBR" -> mapFrom("GROUP_NUM"),
    "INSURANCEORDER" -> mapFrom("LINE"),
    "POLICYNUMBER" -> mapFrom("SUBSCR_NUM"),
    "PLANTYPE" -> ((col: String, df: DataFrame) => df.withColumn(col, when(df("FIN_CLASS").isNotNull, concat(lit(config(CLIENT_DS_ID) + ".epic."), df("FIN_CLASS"))).otherwise(null))),
    "PAYORCODE" -> mapFrom("PAYOR_ID", nullIf = Seq("-1")),
    "PAYORNAME" -> mapFrom("PAYOR_NAME"),
    "PLANCODE" -> mapFrom("PLAN_ID", nullIf = Seq("-1")),
    "PLANNAME" -> mapFrom("BENEFIT_PLAN_NAME")
  )


  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("PATIENTID"), df1("INS_TIMESTAMP"), df1("PAYORCODE")).orderBy(df1("FILEID").desc)
    val addColumn = df1.withColumn("rownumber", row_number.over(groups))
    addColumn.filter("rownumber = 1")
  }

  mapExceptions = Map(
    ("H406239_EP2", "PLANCODE") -> mapFrom("INTERNAL_ID", nullIf = Seq("-1"))
  )

}