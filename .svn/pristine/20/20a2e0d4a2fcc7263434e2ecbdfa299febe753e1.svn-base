package com.humedica.mercury.etl.epic_v2.claim

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 01/27/2017
  */


class ClaimHspacctcpt(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("inptbilling_acct",
    "hsp_acct_cpt",
    "hsp_acct_pat_csn",
    "inptbilling_txn",
    "clinicalencounter:epic_v2.clinicalencounter.ClinicalencounterEncountervisit",
    "zh_cl_ub_rev_code",
    "excltxn:epic_v2.claim.ClaimExcltxn"
  )

  columnSelect = Map(
    "inptbilling_acct" -> List("PAT_ID", "PRIM_ENC_CSN_ID", "HSP_ACCOUNT_ID", "INST_OF_UPDATE", "FILEID"),
    "hsp_acct_cpt" -> List("HSP_ACCOUNT_ID", "CPT_CODE_DATE", "CPT_MODIFIERS", "CPT_CODE",
      "LINE", "CPT_PERF_PROV_ID","PX_REV_CODE_ID","FILEID"),
    "hsp_acct_pat_csn" -> List("HSP_ACCOUNT_ID","PAT_ID","LINE","FILEID","PAT_ENC_CSN_ID"),
    "inptbilling_txn" -> List("PAT_ENC_CSN_ID", "TX_POST_DATE","HSP_ACCOUNT_ID","TX_ID","ORIG_REV_TX_ID","TX_TYPE_HA_C","SERVICE_DATE","FILEID"),
    "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID"),
    "zh_cl_ub_rev_code" -> List("UB_REV_CODE_ID", "REVENUE_CODE")
  )


  beforeJoin = Map(
    "inptbilling_acct" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups =  Window.partitionBy(df1("HSP_ACCOUNT_ID")).orderBy(df1("INST_OF_UPDATE").desc, df1("FILEID").desc)
      val addColumn = df1.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 and PAT_ID is not null")
               .drop("rn")
               .withColumnRenamed("PAT_ID","PAT_ID_inpt")
               .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_inpt")
    }),
    "hsp_acct_cpt" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("HSP_ACCOUNT_ID"),df1("LINE")).orderBy(df1("FILEID").desc)
      val addColumn = df1.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 and CPT_CODE_DATE is not null").drop("rn")
    }),
    "inptbilling_txn" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val fil = df1.filter("(PAT_ENC_CSN_ID is not null or PAT_ENC_CSN_ID <> '-1') and (ORIG_REV_TX_ID is null or ORIG_REV_TX_ID = '-1') and TX_TYPE_HA_C = '1'").drop("ORIG_REV_TX_ID")
      val gtt = table("excltxn").withColumnRenamed("ORIG_REV_TX_ID","ORIG_REV_TX_ID_excl")
      val joined = fil.join(gtt, fil("TX_ID") === gtt("ORIG_REV_TX_ID_excl"), "left_outer")
      val fil2 = joined.filter("ORIG_REV_TX_ID_excl is null")
      val groups = Window.partitionBy(fil2("HSP_ACCOUNT_ID")).orderBy(fil2("TX_POST_DATE").desc_nulls_last, fil2("SERVICE_DATE").desc_nulls_last, fil2("FILEID").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
        .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_txn")
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_txn")
    }),
    "hsp_acct_pat_csn" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("HSP_ACCOUNT_ID"),df("PAT_ID")).orderBy(df1("LINE").asc, df1("FILEID").desc)
      val addColumn = df1.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1")
              .drop("rn")
              .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_hspacct")
              .withColumnRenamed("LINE", "LINE_hspacct")
    }),
    "clinicalencounter" -> ((df: DataFrame) => {
      df.withColumnRenamed("ENCOUNTERID", "ENCOUNTERID_ce")
        .withColumnRenamed("PATIENTID","PATIENTID_ce")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("hsp_acct_cpt")
      .join(dfs("inptbilling_acct"), dfs("hsp_acct_cpt")("HSP_ACCOUNT_ID") === dfs("inptbilling_acct")("HSP_ACCOUNT_ID_inpt"), "inner")
      .join(dfs("inptbilling_txn"), dfs("hsp_acct_cpt")("HSP_ACCOUNT_ID") === dfs("inptbilling_txn")("HSP_ACCOUNT_ID_txn"), "left_outer")
      .join(dfs("hsp_acct_pat_csn"), dfs("hsp_acct_cpt")("HSP_ACCOUNT_ID") === dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_hspacct") , "left_outer")
      .join(dfs("clinicalencounter"), dfs("clinicalencounter")("ENCOUNTERID_ce") === coalesce(when(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID") === lit("-1"), null).otherwise(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID"))
        ,when(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID"))
        ,when(dfs("inptbilling_txn")("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(dfs("inptbilling_txn")("PAT_ENC_CSN_ID_txn"))), "left_outer")
      .join(dfs("zh_cl_ub_rev_code"), dfs("hsp_acct_cpt")("PX_REV_CODE_ID") === dfs("zh_cl_ub_rev_code")("UB_REV_CODE_ID"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("hsp_acct_cpt"),
    "CLAIMID" -> mapFrom("HSP_ACCOUNT_ID"),
    "PATIENTID" ->  ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(when(df("PAT_ID") === lit("-1"), null).otherwise(df("PAT_ID"))
        ,when(df("PATIENTID_ce") === lit("-1"), null).otherwise(df("PATIENTID_ce"))
        ,when(df("PAT_ID_inpt") === lit("-1"), null).otherwise(df("PAT_ID_inpt"))))
    }),
    "SERVICEDATE" -> mapFrom("CPT_CODE_DATE"),
    "MAPPEDCPTMOD1" -> ((col: String, df: DataFrame) => {
      val mapcptmod1 = df.withColumn("MAPCPTMOD_1", split(df("CPT_MODIFIERS"), ",")(0))
      mapcptmod1.withColumn(col, when(length(trim(mapcptmod1("MAPCPTMOD_1"))) <= "2", trim(mapcptmod1("MAPCPTMOD_1"))).otherwise(null))
    }),
    "MAPPEDCPTMOD2" -> ((col: String, df: DataFrame) => {
      val mapcptmod2 = df.withColumn("MAPCPTMOD_2", split(df("CPT_MODIFIERS"), ",")(1))
      mapcptmod2.withColumn(col, when(length(trim(mapcptmod2("MAPCPTMOD_2"))) <= "2", trim(mapcptmod2("MAPCPTMOD_2"))).otherwise(null))
    }),
    "MAPPEDCPTMOD3" -> ((col: String, df: DataFrame) => {
      val mapcptmod3 = df.withColumn("MAPCPTMOD_3", split(df("CPT_MODIFIERS"), ",")(2))
      mapcptmod3.withColumn(col, when(length(trim(mapcptmod3("MAPCPTMOD_3"))) <= "2", trim(mapcptmod3("MAPCPTMOD_3"))).otherwise(null))
    }),
    "MAPPEDCPTMOD4" -> ((col: String, df: DataFrame) => {
      val mapcptmod4 = df.withColumn("MAPCPTMOD_4", split(df("CPT_MODIFIERS"), ",")(3))
      mapcptmod4.withColumn(col, when(length(trim(mapcptmod4("MAPCPTMOD_4"))) <= "2", trim(mapcptmod4("MAPCPTMOD_4"))).otherwise(null))
    }),
    "LOCALCPT" -> mapFrom("CPT_CODE"),
    "LOCALCPTMOD1" -> ((col: String, df: DataFrame) => {
      val mapcptmod1 = df.withColumn("MAPCPTMOD_1", split(df("CPT_MODIFIERS"), ",")(0))
      mapcptmod1.withColumn(col, when(length(trim(mapcptmod1("MAPCPTMOD_1"))) <= "2", trim(mapcptmod1("MAPCPTMOD_1"))).otherwise(null))
    }),
    "LOCALCPTMOD2" -> ((col: String, df: DataFrame) => {
      val mapcptmod2 = df.withColumn("MAPCPTMOD_2", split(df("CPT_MODIFIERS"), ",")(1))
      mapcptmod2.withColumn(col, when(length(trim(mapcptmod2("MAPCPTMOD_2"))) <= "2", trim(mapcptmod2("MAPCPTMOD_2"))).otherwise(null))
    }),
    "LOCALCPTMOD3" -> ((col: String, df: DataFrame) => {
      val mapcptmod3 = df.withColumn("MAPCPTMOD_3", split(df("CPT_MODIFIERS"), ",")(2))
      mapcptmod3.withColumn(col, when(length(trim(mapcptmod3("MAPCPTMOD_3"))) <= "2", trim(mapcptmod3("MAPCPTMOD_3"))).otherwise(null))
    }),
    "LOCALCPTMOD4" -> ((col: String, df: DataFrame) => {
      val mapcptmod4 = df.withColumn("MAPCPTMOD_4", split(df("CPT_MODIFIERS"), ",")(3))
      mapcptmod4.withColumn(col, when(length(trim(mapcptmod4("MAPCPTMOD_4"))) <= "2", trim(mapcptmod4("MAPCPTMOD_4"))).otherwise(null))
    }),
    "SEQ" -> mapFrom("LINE"),
    "CLAIMPROVIDERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("CPT_PERF_PROV_ID") === "-1", null).otherwise(df("CPT_PERF_PROV_ID")))
    }),
    "MAPPEDCPT" -> mapFrom("CPT_CODE"),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(when(df("PRIM_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID"))
          ,when(df("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID"))
          ,when(df("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_txn"))))
    }),
    "SOURCEID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(when(df("PRIM_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID"))
        ,when(df("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID"))
        ,when(df("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_txn"))))
    }),
    "LOCALREV" -> mapFrom("PX_REV_CODE_ID", nullIf = Seq("-1")),
    "MAPPEDREV" -> mapFrom("REVENUE_CODE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("MAPPEDCPT"), df("MAPPEDCPTMOD1"), df("MAPPEDCPTMOD2"), df("MAPPEDCPTMOD3"), df("MAPPEDCPTMOD4"), df("MAPPEDREV"), df("SERVICEDATE"), df("CLAIMPROVIDERID"), df("ENCOUNTERID"))
          .orderBy(df("CPT_CODE_DATE").desc)
    val addColumn = df.withColumn("claim_row", row_number.over(groups))
      .withColumn("incl_val", when(df("ENCOUNTERID").isNotNull && coalesce(df("ENCOUNTERID_ce"),lit("-1")) === "-1", "1").otherwise(null))
    addColumn.filter("claim_row = 1 and CLAIMID is not null and PATIENTID is not null AND SERVICEDATE is not null and incl_val is null")
  }

}