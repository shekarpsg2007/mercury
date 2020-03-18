package com.humedica.mercury.etl.epic_v2.insurance

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Howie Leicht - 2-27-17
  */


class InsuranceProfbilling(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_payor",
    "zh_beneplan",
    "coverage",
    "profbilling_inv",
    "profbilling_txn",
    "pat_acct_cvg",
    "zh_epp_map")


  columnSelect = Map(
    "zh_payor" -> List("PAYOR_NAME", "PAYOR_ID"),
    "zh_beneplan" -> List("BENEFIT_PLAN_NAME", "BENEFIT_PLAN_ID"),
    "coverage" -> List("GROUP_NUM", "SUBSCR_NUM", "COVERAGE_ID"),
    "profbilling_txn" -> List("ORIG_SERVICE_DATE", "PAT_ENC_CSN_ID", "TX_ID"),
    "pat_acct_cvg" -> List("PAT_ID", "ACCOUNT_ID", "FIN_CLASS", "LINE", "PAYOR_ID", "PLAN_ID", "UPDATE_DATE", "COVERAGE_ID"),
    "profbilling_inv" -> List("ACCOUNT_ID", "PAT_ID", "TX_ID"),
    "zh_epp_map" -> List("CID", "INTERNAL_ID")

  )

  beforeJoin = Map(
    "profbilling_inv" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      df1.dropDuplicates(List("ACCOUNT_ID", "PAT_ID", "TX_ID"))
    }),
    "profbilling_txn" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      df1.filter("PAT_ENC_CSN_ID is not null").dropDuplicates(List("PAT_ENC_CSN_ID", "ORIG_SERVICE_DATE", "TX_ID"))
    }),
    "pat_acct_cvg" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("PAT_ID"), df1("ACCOUNT_ID"), df1("LINE")).orderBy(df1("UPDATE_DATE").desc)
      val addColumn = df1.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1")
    }),
    "coverage" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      df1.dropDuplicates(List("COVERAGE_ID", "GROUP_NUM", "SUBSCR_NUM"))
    })

  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("pat_acct_cvg")
      .join(dfs("coverage"), Seq("COVERAGE_ID"), "inner")
      .join(dfs("zh_payor"), Seq("PAYOR_ID"), "inner")
      .join(dfs("zh_beneplan"), dfs("pat_acct_cvg")("PLAN_ID") === dfs("zh_beneplan")("BENEFIT_PLAN_ID"), "inner")
      .join(dfs("profbilling_inv"), Seq("ACCOUNT_ID", "PAT_ID"), "inner")
      .join(dfs("profbilling_txn"), Seq("TX_ID"), "inner")

  }


  joinExceptions = Map(
    "H406239_EP2" -> ((dfs: Map[String, DataFrame]) => {
      dfs("pat_acct_cvg")
        .join(dfs("coverage"), Seq("COVERAGE_ID"), "inner")
        .join(dfs("zh_payor"), Seq("PAYOR_ID"), "inner")
        .join(dfs("zh_beneplan"), dfs("pat_acct_cvg")("PLAN_ID") === dfs("zh_beneplan")("BENEFIT_PLAN_ID"), "inner")
        .join(dfs("zh_epp_map"), dfs("zh_beneplan")("BENEFIT_PLAN_ID") === dfs("zh_epp_map")("CID"), "left_outer")
        .join(dfs("profbilling_inv"), Seq("ACCOUNT_ID", "PAT_ID"), "inner")
        .join(dfs("profbilling_txn"), Seq("TX_ID"), "inner")
    })
  )


  map = Map(
    "DATASRC" -> literal("profbilling"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "INS_TIMESTAMP" -> mapFrom("ORIG_SERVICE_DATE"),
    "INSURANCEORDER" -> mapFrom("LINE"),
    "PAYORCODE" -> mapFrom("PAYOR_ID"),
    "PAYORNAME" -> mapFrom("PAYOR_NAME"),
    "PLANTYPE" -> mapFrom("FIN_CLASS", nullIf = Seq(null), prefix = config(CLIENT_DS_ID) + ".epic."),
    "PLANCODE" -> mapFrom("PLAN_ID", nullIf = Seq("-1")),
    "PLANNAME" -> mapFrom("BENEFIT_PLAN_NAME"),
    "GROUPNBR" -> mapFrom("GROUP_NUM"),
    "POLICYNUMBER" -> mapFrom("SUBSCR_NUM")
  )


  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("PATIENTID"), df1("ENCOUNTERID"), df1("PLANTYPE")).orderBy(df1("INS_TIMESTAMP").desc)
    val addColumn = df1.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1")
  }


  mapExceptions = Map(
    ("H406239_EP2", "PLANCODE") -> mapFrom("INTERNAL_ID", nullIf = Seq("-1"))
  )

}