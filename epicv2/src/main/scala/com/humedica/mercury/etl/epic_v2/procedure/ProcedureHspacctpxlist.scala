package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.{EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 02/01/2017
 */


class ProcedureHspacctpxlist(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hsp_acct_px_list","encountervisit", "zh_cl_icd_px", "inptbilling_acct"
    , "cdr.map_predicate_values","hsp_acct_pat_csn","inptbilling_txn"
  ,"clinicalencounter:epic_v2.clinicalencounter.ClinicalencounterEncountervisit", "excltxn:epic_v2.claim.ClaimExcltxn")

  columnSelect = Map(
    "hsp_acct_px_list" -> List("HSP_ACCOUNT_ID", "FINAL_ICD_PX_ID", "PROC_DATE", "LINE", "FILEID", "PROC_PERF_PROV_ID","INST_OF_UPDATE"),
    "encountervisit" -> List("HSP_ACCOUNT_ID", "PAT_ID", "PAT_ENC_CSN_ID", "HOSP_ADMSN_TIME", "HOSP_DISCH_TIME"),
    "zh_cl_icd_px" -> List("ICD_PX_ID", "ICD_PX_NAME", "REF_BILL_CODE", "PROC_MASTER_NM", "REF_BILL_CODE_SET_C"),
    "hsp_acct_pat_csn" -> List("HSP_ACCOUNT_ID","PAT_ID","PAT_ENC_CSN_ID","LINE","FILEID"),
    "inptbilling_txn" -> List("PAT_ENC_CSN_ID", "TX_POST_DATE","HSP_ACCOUNT_ID","TX_ID","ORIG_REV_TX_ID","TX_TYPE_HA_C","SERVICE_DATE","FILEID"),
    "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID"),
    "inptbilling_acct" -> List("HSP_ACCOUNT_ID","CODING_STATUS_C","FILEID","HSP_ACCOUNT_NAME","INST_OF_UPDATE","PRIM_ENC_CSN_ID","PAT_ID")
  )


  beforeJoin = Map(
    "inptbilling_acct" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID")).orderBy(df("INST_OF_UPDATE").desc,df("FILEID").desc)
      df.withColumn("acct_rw", row_number.over(groups))
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_acct")
        .withColumnRenamed("PRIM_ENC_CSN_ID", "PRIM_ENC_CSN_ID_acct")
        .withColumnRenamed("PAT_ID","PAT_ID_acct")
        .filter("acct_rw =1 and lower(HSP_ACCOUNT_NAME) not like 'zzz%'")
    }),
    "hsp_acct_pat_csn" -> ((df: DataFrame) =>{
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID"),df("PAT_ID")).orderBy(df("LINE").asc,df("FILEID").desc)
      df.withColumn("hsp_rw", row_number.over(groups))
        .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_hsp")
        .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_hsp")
        .withColumnRenamed("PAT_ID","PAT_ID_hsp")
        .filter("hsp_rw=1")
    }),
    "hsp_acct_px_list" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID"),df("LINE"),df("FINAL_ICD_PX_ID")).orderBy(df("FILEID").desc)
      df.withColumn("hapl_rw", row_number.over(groups))
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_hapl")
        .withColumnRenamed("INST_OF_UPDATE", "INST_OF_UPDATE_hapl")
        .withColumnRenamed("FILEID", "FILEID_hapl")
        .withColumnRenamed("LINE", "LINE_hapl")
        .filter("hapl_rw =1")
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
    dfs("hsp_acct_px_list")
      .join(dfs("inptbilling_acct"), dfs("hsp_acct_px_list")("HSP_ACCOUNT_ID_hapl") === dfs("inptbilling_acct")("HSP_ACCOUNT_ID_acct"),"left_outer")
      .join(dfs("hsp_acct_pat_csn"), dfs("hsp_acct_px_list")("HSP_ACCOUNT_ID_hapl") === dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_hsp"),"left_outer")
      .join(dfs("inptbilling_txn"), dfs("hsp_acct_px_list")("HSP_ACCOUNT_ID_hapl") === dfs("inptbilling_txn")("HSP_ACCOUNT_ID_txn"),"left_outer")
      .join(dfs("clinicalencounter"), dfs("clinicalencounter")("ENCOUNTERID_ce") === coalesce(
        when(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID_acct")),
        when(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp")),
        when(dfs("inptbilling_txn")("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(dfs("inptbilling_txn")("PAT_ENC_CSN_ID_txn"))
      ), "left_outer")
      .join(dfs("zh_cl_icd_px"), dfs("zh_cl_icd_px")("ICD_PX_ID") === dfs("hsp_acct_px_list")("FINAL_ICD_PX_ID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
    df.filter("PROC_DATE is not null and (coalesce(CODING_STATUS_C, 'X') in (" + list_coding_status_c + ") " +
      "or 'NO_MPV_MATCHES' in (" + list_coding_status_c + "))")
  }


  map = Map(
    "DATASRC" -> literal("hsp_acct_px_list"),
    "LOCALCODE" -> mapFrom("FINAL_ICD_PX_ID"),
    "PATIENTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ID_hsp")),
        when(df("PATIENTID_ce") === lit("-1"), null).otherwise(df("PATIENTID_ce")),
        when(df("PAT_ID_acct") === lit("-1"), null).otherwise(df("PAT_ID_acct"))
      ))
    }),
    "PROCEDUREDATE" -> mapFrom("PROC_DATE"),
    "LOCALNAME" -> mapFrom("ICD_PX_NAME"),
    "PROCSEQ" -> mapFrom("LINE_hapl"),
    "ACTUALPROCDATE" -> mapFrom("PROC_DATE"),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID_acct")),
        when(df("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_hsp")),
        when(df("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_txn"))
      ))
    }),
    "SOURCEID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID_acct")),
        when(df("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_hsp")),
        when(df("PAT_ENC_CSN_ID_txn") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_txn"))
      ))
    }),
    "HOSP_PX_FLAG" -> literal("Y"),
    "PERFORMINGPROVIDERID" -> mapFrom("PROC_PERF_PROV_ID", nullIf=Seq("-1")),
    "LOCALPRINCIPLEINDICATOR" -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(df("LINE_hapl") === lit("1"), "Y").otherwise("N"))),
    "CODETYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("REF_BILL_CODE").isNotNull,
                              when(df("REF_BILL_CODE_SET_C") === lit("1"), "ICD9")
                             .when(df("REF_BILL_CODE_SET_C") === lit("2"), "ICD10")
                             .otherwise(null))
                        .when(df("PROC_MASTER_NM").contains("ICD-10"), "ICD10")
                        .when(df("PROC_MASTER_NM").contains("ICD-9"), "ICD9")
                        .otherwise(null))
    }),
    "MAPPEDCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("REF_BILL_CODE"), when(df("PROC_MASTER_NM").contains("ICD-10"), regexp_extract(df("PROC_MASTER_NM"), "[^\\.]+", 0))
                                                      .when(df("PROC_MASTER_NM").contains("ICD-9"), regexp_extract(df("PROC_MASTER_NM"), "^\\d{2}\\.?\\d{1,2}", 0))
                                                      .otherwise(df("PROC_MASTER_NM"))))
    }
      )
  )


  afterMap = (df1: DataFrame) => {
    val df=df1.repartition(1000)
    val groups = Window.partitionBy(df("MAPPEDCODE"), df("PROCEDUREDATE"), df("PERFORMINGPROVIDERID"), df("ENCOUNTERID"), df("PATIENTID")).orderBy(df("INST_OF_UPDATE_hapl").desc,df("FILEID_hapl").desc)
    val addColumn = df.withColumn("hac_rw", row_number.over(groups))
      .withColumn("incl_val", when(coalesce(df("ENCOUNTERID"), lit("-1")) =!= lit("-1") && coalesce(df("ENCOUNTERID_ce"),lit("-1")) === lit("-1"), "1").otherwise(null))
    addColumn.filter("hac_rw=1 and incl_val is null and PATIENTID is not null")
  }
}
