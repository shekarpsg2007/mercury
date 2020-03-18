package com.humedica.mercury.etl.epic_v2.insurance

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

class InsuranceInptbillingacct(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "inptbilling_acct",
    "zh_beneplan",
    "zh_payor",
    //"zh_epp_map",
    "hsp_acct_pat_csn",
    "inptbilling_txn",
    "zh_epp_map"
  )


  columnSelect = Map(
    "inptbilling_acct" -> List("ACCT_FIN_CLASS_C", "PAT_ID", "COVERAGE_ID", "PRIMARY_PAYOR_ID", "PRIMARY_PLAN_ID", "HSP_ACCOUNT_ID", "ACCT_BILLSTS_HA_C", "FILEID"),
    "inptbilling_txn" -> List("PAYOR_ID", "SERVICE_DATE", "PAT_ENC_CSN_ID", "HSP_ACCOUNT_ID"),
    "zh_payor" -> List("PAYOR_NAME", "PAYOR_ID"),
    "zh_beneplan" -> List("BENEFIT_PLAN_NAME", "BENEFIT_PLAN_ID"),
    "hsp_acct_pat_csn" -> List("HSP_ACCOUNT_ID", "PAT_ENC_DATE", "PAT_ENC_CSN_ID"),
    "zh_epp_map" -> List("CID", "INTERNAL_ID")
  )


  beforeJoin = Map(
    "inptbilling_acct" -> ((df: DataFrame) => {
      val dedup = df.dropDuplicates(List("PAT_ID", "HSP_ACCOUNT_ID", "ACCT_BILLSTS_HA_C", "ACCT_FIN_CLASS_C", "COVERAGE_ID", "PRIMARY_PAYOR_ID", "PRIMARY_PLAN_ID", "FILEID"))
      dedup.filter("(ACCT_BILLSTS_HA_C is null or ACCT_BILLSTS_HA_C <> '40') and PAT_ID is not null and PAT_ID <> '-1'")
    }),
    "inptbilling_txn" -> ((df: DataFrame) => {
      val fil = df.filter("PAT_ENC_CSN_ID is not null")
      fil.dropDuplicates(List("PAT_ENC_CSN_ID", "HSP_ACCOUNT_ID", "SERVICE_DATE", "PAYOR_ID")).withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_TXN").withColumnRenamed("PAYOR_ID", "PAYOR_ID_TXN")
    }),
    "zh_epp_map" -> ((df: DataFrame) => {
      df.distinct()
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("inptbilling_acct")
      .join(dfs("zh_beneplan"), dfs("inptbilling_acct")("PRIMARY_PLAN_ID") === dfs("zh_beneplan")("BENEFIT_PLAN_ID"), "left_outer")
      .join(dfs("zh_payor"), dfs("inptbilling_acct")("PRIMARY_PAYOR_ID") === dfs("zh_payor")("PAYOR_ID"), "left_outer")
      .join(dfs("hsp_acct_pat_csn"), Seq("HSP_ACCOUNT_ID"), "left_outer")
      .join(dfs("inptbilling_txn"), Seq("HSP_ACCOUNT_ID"), "left_outer")
  }


  joinExceptions = Map(
    "H406239_EP2" -> ((dfs: Map[String, DataFrame]) => {
      dfs("inptbilling_acct")
        .join(dfs("zh_beneplan"), dfs("inptbilling_acct")("PRIMARY_PLAN_ID") === dfs("zh_beneplan")("BENEFIT_PLAN_ID"), "left_outer")
        .join(dfs("zh_epp_map"), dfs("zh_beneplan")("BENEFIT_PLAN_ID") === dfs("zh_epp_map")("CID"), "left_outer")
        .join(dfs("zh_payor"), dfs("inptbilling_acct")("PRIMARY_PAYOR_ID") === dfs("zh_payor")("PAYOR_ID"), "left_outer")
        .join(dfs("hsp_acct_pat_csn"), Seq("HSP_ACCOUNT_ID"), "left_outer")
        .join(dfs("inptbilling_txn"), Seq("HSP_ACCOUNT_ID"), "left_outer")
    })
  )

  afterJoin = (df: DataFrame) => {
    df.filter("SERVICE_DATE is not null or PAT_ENC_DATE is not null")
  }

  map = Map(
    "DATASRC" -> literal("inptbilling_acct"),
    "PAYORCODE" -> mapFrom("PRIMARY_PAYOR_ID", nullIf = Seq("-1")),
    "PLANCODE" -> mapFrom("PRIMARY_PLAN_ID", nullIf = Seq("-1")),
    "PLANTYPE" -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(df("ACCT_FIN_CLASS_C").isNotNull, concat(lit(config(CLIENT_DS_ID) + ".epic."), df("ACCT_FIN_CLASS_C"))).otherwise(null))),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "GROUPNBR" -> mapFrom("COVERAGE_ID", nullIf = Seq("-1")),
    "PAYORNAME" -> mapFrom("PAYOR_NAME"),
    "PLANNAME" -> mapFrom("BENEFIT_PLAN_NAME"),
    "INS_TIMESTAMP" -> cascadeFrom(Seq("SERVICE_DATE", "PAT_ENC_DATE")),
    "ENCOUNTERID" -> cascadeFrom(Seq("PAT_ENC_CSN_ID_TXN", "PAT_ENC_CSN_ID"), nullIf = Seq("-1")),
    "INSURANCEORDER" -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(df("PRIMARY_PAYOR_ID") === "-1", 0).otherwise(1)))
  )


  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("PATIENTID"), df1("ENCOUNTERID"), df1("PLANTYPE")).orderBy(df1("INS_TIMESTAMP").desc)
    val addColumn = df1.withColumn("rownumber", row_number.over(groups))
    addColumn.filter("rownumber = 1")

  }

  mapExceptions = Map(
    ("H262866_EP2", "PAYORCODE") -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(coalesce(df("PAYOR_ID_TXN"), df("PRIMARY_PAYOR_ID")) === "-1", null)
        .otherwise(coalesce(df("PAYOR_ID_TXN"), df("PRIMARY_PAYOR_ID"))))),
    ("H262866_EP2", "INSURANCEORDER") -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(coalesce(df("PAYOR_ID_TXN"), df("PRIMARY_PAYOR_ID")) === "-1", 0).otherwise(1))),
    ("H406239_EP2", "PLANCODE") -> mapFrom("INTERNAL_ID", nullIf = Seq("-1"))
  )
}
