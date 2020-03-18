package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.functions.{lit, udf}


class ProcedureHspacctcpt(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("inptbilling_acct",
    "hsp_acct_cpt",
    "inptbilling_txn",
    "hsp_acct_pat_csn",
    "clinicalencounter:epic_v2.clinicalencounter.ClinicalencounterEncountervisit",
    "excltxn:epic_v2.claim.ClaimExcltxn",
    "zh_cl_ub_rev_code")


  columnSelect = Map(
    "inptbilling_acct" -> List("PAT_ID", "PRIM_ENC_CSN_ID", "HSP_ACCOUNT_ID", "INST_OF_UPDATE","FILEID"),
    "hsp_acct_cpt" -> List("CPT_CODE", "CPT_CODE_DATE", "CPT_CODE_DESC", "LINE", "CPT_PERF_PROV_ID", "HSP_ACCOUNT_ID","PX_REV_CODE_ID","FILEID"),
    "hsp_acct_pat_csn" -> List("HSP_ACCOUNT_ID","PAT_ID","PAT_ENC_CSN_ID","LINE","FILEID"),
    "inptbilling_txn" -> List("PAT_ENC_CSN_ID", "TX_POST_DATE","HSP_ACCOUNT_ID","TX_ID","ORIG_REV_TX_ID","TX_TYPE_HA_C","SERVICE_DATE","FILEID"),
    "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID"),
    "zh_cl_ub_rev_code" -> List("UB_REV_CODE_ID", "REVENUE_CODE_NAME","REVENUE_CODE")
  )

  val getNull = udf(() => None: Option[String])

  beforeJoin = Map(
  "hsp_acct_cpt" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID"),df("LINE")).orderBy(df("FILEID").desc)
      val out = df.withColumn("hac_rw", row_number.over(groups))
        .withColumn("PX_REV_CODE_ID", coalesce(when(df("PX_REV_CODE_ID") === lit("-1"),lit(getNull())).otherwise(df("PX_REV_CODE_ID"))))
        .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_hac")
        .withColumnRenamed("LINE","LINE_hac")
        .filter("hac_rw=1 and CPT_CODE_DATE is not null")
      //val newout = out.filter("hac_rw=1 and CPT_CODE_DATE is not null")

    val fpiv = unpivot(Seq("CPT_CODE", "PX_REV_CODE_ID"),
      Seq("CPT4", "REV"), typeColumnName = "codetype")
    fpiv("localcode", out)
    }),
    "inptbilling_acct" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID")).orderBy(df("INST_OF_UPDATE").desc,df("FILEID").desc)
                   df.withColumn("acct_rw", row_number.over(groups))
                     .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_acct")
                     .withColumnRenamed("PRIM_ENC_CSN_ID", "PRIM_ENC_CSN_ID_acct")
                     .withColumnRenamed("PAT_ID","PAT_ID_acct")
                     .filter("acct_rw =1 and PAT_ID is not null")
    }),
    "hsp_acct_pat_csn" -> ((df: DataFrame) =>{
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID"),df("PAT_ID")).orderBy(df("LINE").asc,df("FILEID").desc)
      df.withColumn("hsp_rw", row_number.over(groups))
        .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_hsp")
        .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_hsp")
        .withColumnRenamed("PAT_ID","PAT_ID_hsp")
        .filter("hsp_rw=1")
    }),
    "inptbilling_txn" -> ((df: DataFrame) =>{
      val fil = df.filter("(PAT_ENC_CSN_ID is not null or PAT_ENC_CSN_ID <> '-1') and (ORIG_REV_TX_ID is null or ORIG_REV_TX_ID = '-1') and TX_TYPE_HA_C = '1'").drop("ORIG_REV_TX_ID")
      val gtt = table("excltxn").withColumnRenamed("ORIG_REV_TX_ID","ORIG_REV_TX_ID_excl")
      val joined = fil.join(gtt, fil("TX_ID") === gtt("ORIG_REV_TX_ID_excl"), "left_outer")
      val fil2 = joined.filter("ORIG_REV_TX_ID_excl is null")
      val groups = Window.partitionBy(fil2("HSP_ACCOUNT_ID")).orderBy(fil2("TX_POST_DATE").desc_nulls_last, fil2("SERVICE_DATE").desc_nulls_last, fil2("FILEID").desc)
      fil2.withColumn("txn_rw", row_number.over(groups))
        .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_txn")
        .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_txn")
        .filter("txn_rw=1").drop("txn_rw")
    }),
    "clinicalencounter" -> ((df: DataFrame) => {
      df.withColumnRenamed("PATIENTID", "PATIENTID_ce")
        .withColumnRenamed("ENCOUNTERID", "ENCOUNTERID_ce")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hsp_acct_cpt")
      .join(dfs("inptbilling_acct"), dfs("hsp_acct_cpt")("HSP_ACCOUNT_ID_hac") === dfs("inptbilling_acct")("HSP_ACCOUNT_ID_acct"),"inner")
      .join(dfs("hsp_acct_pat_csn"), dfs("hsp_acct_cpt")("HSP_ACCOUNT_ID_hac") === dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_hsp"),"left_outer")
      .join(dfs("inptbilling_txn"), dfs("hsp_acct_cpt")("HSP_ACCOUNT_ID_hac") === dfs("inptbilling_txn")("HSP_ACCOUNT_ID_txn"),"left_outer")
      .join(dfs("clinicalencounter"), dfs("clinicalencounter")("ENCOUNTERID_ce") === coalesce(
        when(dfs("inptbilling_txn")("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(dfs("inptbilling_txn")("PAT_ENC_CSN_ID_txn")),
        when(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID_acct")),
        when(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp"))
      ), "left_outer")
      .join(dfs("zh_cl_ub_rev_code"), dfs("hsp_acct_cpt")("PX_REV_CODE_ID") === dfs("zh_cl_ub_rev_code")("UB_REV_CODE_ID") , "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("hsp_acct_cpt"),
    "LOCALCODE" -> mapFrom("localcode"),
    "PATIENTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ID_hsp")),
        when(df("PATIENTID_ce") === lit("-1"), null).otherwise(df("PATIENTID_ce")),
        when(df("PAT_ID_acct") === lit("-1"), null).otherwise(df("PAT_ID_acct"))
      ))
    }),
    "PROCEDUREDATE" -> mapFrom("CPT_CODE_DATE"),
    "PROCSEQ" -> mapFrom("LINE_hac"),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_txn")),
        when(df("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID_acct")),
        when(df("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_hsp"))
      ))
    }),
    "SOURCEID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_txn")),
          when(df("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID_acct")),
        when(df("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_hsp"))
      ))
    }),
    "HOSP_PX_FLAG" -> literal("Y"),
    "PERFORMINGPROVIDERID" -> mapFrom("CPT_PERF_PROV_ID", nullIf= Seq("-1")),
    "CODETYPE" -> mapFrom("codetype"),
    "LOCALNAME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("codetype") === lit("CPT4"), df("CPT_CODE_DESC"))
        .when(df("codetype") === lit("REV"), df("REVENUE_CODE_NAME")))
    }),
    "MAPPEDCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("codetype") === lit("CPT4"), df("localcode"))
        .when(df("codetype") === lit("REV"), df("REVENUE_CODE")))
    })
  )

  afterMap = (df1: DataFrame) => {
    val df=df1.repartition(1000)
    val groups = Window.partitionBy(df("MAPPEDCODE"), df("PROCEDUREDATE"), df("PERFORMINGPROVIDERID"), df("ENCOUNTERID")).orderBy(df("PROCEDUREDATE").desc)
    val addColumn = df.withColumn("hac_rw", row_number.over(groups))
       .withColumn("incl_val", when(coalesce(df("ENCOUNTERID"), lit("-1")) =!= lit("-1") && coalesce(df("ENCOUNTERID_ce"),lit("-1")) === lit("-1"), "1").otherwise(null))
      addColumn.filter("hac_rw=1 and incl_val is null")
  }
}






