package com.humedica.mercury.etl.epic_v2.diagnosis

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class DiagnosisHsptxdiag (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hsp_tx_diag", "inptbilling_acct", "inptbilling_txn", "zh_clarity_edg", "zh_v_edg_hx_icd10", "zh_edg_curr_icd10",
    "zh_v_edg_hx_icd9", "zh_edg_curr_icd9",
    "temptable:epic_v2.clinicalencounter.ClinicalencounterEncountervisit",
    "cdr.map_predicate_values",
    "hsp_acct_pat_csn",
    "excltxn:epic_v2.claim.ClaimExcltxn")


  columnSelect = Map(
    "hsp_tx_diag" -> List("HSP_ACCOUNT_ID", "TX_ID", "LINE", "INST_OF_UPDATE_DTTM", "DX_ID", "POST_DATE"),
    "inptbilling_acct" -> List("HSP_ACCOUNT_ID", "INST_OF_UPDATE", "PRIM_ENC_CSN_ID", "PAT_ID", "HSP_ACCOUNT_NAME", "ADM_DATE_TIME", "CODING_STATUS_C"),
    "hsp_acct_pat_csn" -> List("HSP_ACCOUNT_ID", "PAT_ID", "LINE", "FILEID", "PAT_ENC_CSN_ID", "PAT_ENC_DATE"),
    "inptbilling_txn" -> List("PAT_ENC_CSN_ID", "ORIG_REV_TX_ID", "TX_TYPE_HA_C", "TX_ID", "HSP_ACCOUNT_ID", "TX_POST_DATE", "SERVICE_DATE", "FILEID"),
    "excltxn" -> List("ORIG_REV_TX_ID"),
    "zh_edg_curr_icd9" -> List("CODE", "DX_ID"),
    "zh_edg_curr_icd10" -> List("CODE", "DX_ID"),
    "zh_v_edg_hx_icd9" -> List("CODE", "EFF_START_DATE", "EFF_END_DATE", "DX_ID"),
    "zh_v_edg_hx_icd10" -> List("CODE", "EFF_START_DATE", "EFF_END_DATE", "DX_ID"),
    "temptable" -> List("PATIENTID", "ENCOUNTERID"),
    "zh_clarity_edg" -> List("DX_ID", "RECORD_TYPE_C", "REF_BILL_CODE", "REF_BILL_CODE_SET_C")

  )


  beforeJoin = Map(
    "hsp_tx_diag" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("HSP_ACCOUNT_ID"), df1("TX_ID"), df1("LINE")).orderBy(df1("INST_OF_UPDATE_DTTM").desc)
      df1.withColumn("hsp_tx_diag_rn", row_number.over(groups))
        .withColumnRenamed("TX_ID", "TX_ID_diag")
        .withColumnRenamed("DX_ID", "DX_ID_diag")
        .withColumnRenamed("LINE", "LINE_diag")
        .withColumnRenamed("POST_DATE", "POST_DATE_diag")
        .withColumnRenamed("INST_OF_UPDATE_DTTM", "INST_OF_UPDATE_DTTM_diag")
        .filter("hsp_tx_diag_rn = 1")
    }),
    "inptbilling_acct" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("HSP_ACCOUNT_ID")).orderBy(df1("INST_OF_UPDATE").desc)
      df1.withColumn("acct_rw", row_number.over(groups))
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_acct")
        .withColumnRenamed("PRIM_ENC_CSN_ID", "PRIM_ENC_CSN_ID_acct")
        .withColumnRenamed("PAT_ID","PAT_ID_acct")
        .filter("acct_rw =1 and lower(HSP_ACCOUNT_NAME) NOT like 'zzz%'")
    }),
    "hsp_acct_pat_csn" -> ((df1: DataFrame) =>{
      val df = df1.repartition(1000)
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID"),df("PAT_ID")).orderBy(df("LINE").asc,df("FILEID").desc)
      df.withColumn("hsp_rw", row_number.over(groups))
        .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_hsp")
        .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_hsp")
        .withColumnRenamed("PAT_ID","PAT_ID_hsp")
        .withColumnRenamed("PAT_ENC_DATE","PAT_ENC_DATE_hsp")
        .filter("hsp_rw = 1")
    }),
    "inptbilling_txn" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val fil = df1.filter("(PAT_ENC_CSN_ID is not null or PAT_ENC_CSN_ID <> '-1') and (ORIG_REV_TX_ID is null or ORIG_REV_TX_ID = '-1') and TX_TYPE_HA_C = '1'").drop("ORIG_REV_TX_ID")
      val gtt = table("excltxn").withColumnRenamed("ORIG_REV_TX_ID","ORIG_REV_TX_ID_excl")
      val joined = fil.join(gtt, fil("TX_ID") === gtt("ORIG_REV_TX_ID_excl"), "left_outer")
      val fil2 = joined.filter("ORIG_REV_TX_ID_excl is null")
      val groups = Window.partitionBy(df1("TX_ID"), fil2("HSP_ACCOUNT_ID"), fil2("PAT_ENC_CSN_ID")).orderBy(fil2("TX_POST_DATE").desc_nulls_last, fil2("SERVICE_DATE").desc_nulls_last, fil2("FILEID").desc)
      df1.withColumn("txn_rn", row_number.over(groups))
        .withColumnRenamed("tx_type_ha_c", "tx_type_ha_c_txn")
        .filter("txn_rn = 1")
    }),
    "zh_edg_curr_icd9"  -> renameColumn("CODE", "ECODE9"),
    "zh_edg_curr_icd10" -> renameColumn("CODE", "ECODE10"),
    "zh_v_edg_hx_icd9"  -> renameColumns(List(("CODE", "HCODE9"), ("EFF_START_DATE", "START9"), ("EFF_END_DATE", "END9"), ("DX_ID", "DX_ID9"))),
    "zh_v_edg_hx_icd10" -> renameColumns(List(("CODE", "HCODE10"), ("EFF_START_DATE", "START10"), ("EFF_END_DATE", "END10"), ("DX_ID", "DX_ID10"))),
    "temptable" -> ((df: DataFrame) => {
      df.withColumnRenamed("PATIENTID", "PATIENTID_ce")
        .withColumnRenamed("ENCOUNTERID", "ENCOUNTERID_ce")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hsp_tx_diag")
      .join(dfs("inptbilling_acct"), dfs("hsp_tx_diag")("HSP_ACCOUNT_ID") === dfs("inptbilling_acct")("HSP_ACCOUNT_ID_acct"), "left_outer")
      .join(dfs("inptbilling_txn"), dfs("hsp_tx_diag")("TX_ID_diag") === dfs("inptbilling_txn")("TX_ID"), "left_outer")
      .join(dfs("hsp_acct_pat_csn"), dfs("hsp_tx_diag")("HSP_ACCOUNT_ID") === dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_hsp"), "left_outer")
      .join(dfs("zh_clarity_edg"), dfs("hsp_tx_diag")("DX_ID_diag") === dfs("zh_clarity_edg")("DX_ID"), "left_outer")
      .join(dfs("zh_edg_curr_icd9"), dfs("hsp_tx_diag")("DX_ID_diag") === dfs("zh_edg_curr_icd9")("DX_ID"), "left_outer")
      .join(dfs("zh_edg_curr_icd10"), dfs("hsp_tx_diag")("DX_ID_diag") === dfs("zh_edg_curr_icd10")("DX_ID"), "left_outer")
      .join(dfs("zh_v_edg_hx_icd9"), dfs("hsp_tx_diag")("DX_ID_diag") === dfs("zh_v_edg_hx_icd9")("DX_ID9") &&
        coalesce(dfs("inptbilling_txn")("SERVICE_DATE"),dfs("inptbilling_acct")("ADM_DATE_TIME"),dfs("hsp_acct_pat_csn")("PAT_ENC_DATE_hsp"),dfs("hsp_tx_diag")("POST_DATE_diag")).between(dfs("zh_v_edg_hx_icd9")("START9"), dfs("zh_v_edg_hx_icd9")("END9"))
        , "left_outer")
      .join(dfs("zh_v_edg_hx_icd10"), dfs("hsp_tx_diag")("DX_ID_diag") === dfs("zh_v_edg_hx_icd10")("DX_ID10") &&
        coalesce(dfs("inptbilling_txn")("SERVICE_DATE"),dfs("inptbilling_acct")("ADM_DATE_TIME"),dfs("hsp_acct_pat_csn")("PAT_ENC_DATE_hsp"),dfs("hsp_tx_diag")("POST_DATE_diag")).between(dfs("zh_v_edg_hx_icd10")("START10"), dfs("zh_v_edg_hx_icd10")("END10")), "left_outer")
      .join(dfs("temptable"), dfs("temptable")("ENCOUNTERID_ce") === coalesce(
        when(dfs("inptbilling_txn")("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(dfs("inptbilling_txn")("PAT_ENC_CSN_ID")),
        when(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID_acct")),
        when(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp"))
      ), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("hsp_tx_diag"),
    "SOURCEID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID")),
        when(df("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID_acct")),
        when(df("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_hsp"))
      ))
    }),
    "DX_TIMESTAMP" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("SERVICE_DATE"),df("ADM_DATE_TIME"),df("PAT_ENC_DATE_hsp"),df("POST_DATE_diag")))
    }),
    "LOCALDIAGNOSIS" -> mapFrom("DX_ID_diag"),
    "PATIENTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ID_hsp")),
        when(df("PATIENTID_ce") === lit("-1"), null).otherwise(df("PATIENTID_ce")),
        when(df("PAT_ID_acct") === lit("-1"), null).otherwise(df("PAT_ID_acct"))
      ))
    }),
    "HOSP_DX_FLAG" -> literal("Y"),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID")),
        when(df("PRIM_ENC_CSN_ID_acct") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID_acct")),
        when(df("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_hsp"))
      ))
    }),
    "TX_ID" -> mapFrom("TX_ID_diag"),
    "PRIMARYDIAGNOSIS" -> literal("0")
  )

  afterMap = (df: DataFrame) => {

    val drop_diag = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPIC", "DIAGNOSIS", "DIAGNOSIS", "LOCALDIAGNOSIS")
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")

    val fil = df.filter("((coalesce(CODING_STATUS_C, 'X') in (" + list_coding_status_c + ") or 'NO_MPV_MATCHES' in (" + list_coding_status_c + ")))")
    val fil2 = fil.withColumn("MAPPED_ICD9", coalesce(fil("HCODE9"), fil("ECODE9")))
      .withColumn("MAPPED_ICD10", coalesce(fil("HCODE10"), fil("ECODE10")))

    val fpiv = unpivot(
      Seq("MAPPED_ICD9", "MAPPED_ICD10"),
      Seq("ICD9", "ICD10"), typeColumnName = "STANDARDCODETYPE")
    val fpiv2 = fpiv("STANDARDCODE", fil2)

    val mappeddiag = fpiv2.withColumn("MAPPEDDIAGNOSIS", when(fpiv2("RECORD_TYPE_C").isin("1", "-1") || isnull(fpiv2("RECORD_TYPE_C")), fpiv2("STANDARDCODE"))
      .when(fpiv2("REF_BILL_CODE").isin("IMO0001", "000.0"), null).otherwise(fpiv2("REF_BILL_CODE")))

    val cdtype = mappeddiag.withColumn("CODETYPE", when(mappeddiag("RECORD_TYPE_C").isin("1", "-1") || isnull(mappeddiag("RECORD_TYPE_C")), mappeddiag("STANDARDCODETYPE"))
      .when(mappeddiag("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "1", "ICD9")
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "2", "ICD10"))
    val cdtype1 = cdtype.repartition(1000)

    val ct_window = Window.partitionBy(cdtype1("PATIENTID"), cdtype1("DX_TIMESTAMP"), cdtype1("ENCOUNTERID"), cdtype1("LOCALDIAGNOSIS"))

    val fil3 = cdtype1.withColumn("ICD9_CT", sum(when(cdtype1("CODETYPE") === lit("ICD9"), 1).otherwise(0)).over(ct_window))
      .withColumn("ICD10_CT", sum(when(cdtype1("CODETYPE") === lit("ICD10"), 1).otherwise(0)).over(ct_window))

    val icdfil = fil3.withColumn("drop_icd9s",
      when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP >= from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
        when(fil3("ICD9_CT").gt(lit(0)) && fil3("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .withColumn("drop_icd10s",
        when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP < from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
          when(fil3("ICD9_CT").gt(lit(0)) && fil3("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))

    val tbl = readTable("inptbilling_txn", config).dropDuplicates("orig_rev_tx_id")
      .filter("coalesce(orig_rev_tx_id, '-1') <> '-1'")
      .withColumnRenamed("orig_rev_tx_id", "orig_rev_tx_id_tbl").select("orig_rev_tx_id_tbl")

    val df2 = icdfil.join(tbl, icdfil("TX_ID_diag") === tbl("ORIG_REV_TX_ID_tbl"), "left_outer")

    val df3 = df2.withColumn("incl_val", when(coalesce(df2("ENCOUNTERID"), lit("-1")) =!= lit("-1") && coalesce(df2("ENCOUNTERID_ce"),lit("-1")) === lit("-1"), "1").otherwise(null))
      .filter(" orig_rev_tx_id_tbl is null and tx_type_ha_c_txn = '1' and coalesce(orig_rev_tx_id, '-1') = '-1' and " +
        " dx_timestamp is not null and patientid is not null and localdiagnosis is not null and incl_val is null ")
    val df4 = df3.repartition(1000)
    val groups = Window.partitionBy(df4("PATIENTID"),df4("ENCOUNTERID"),df4("LOCALDIAGNOSIS"), df4("MAPPEDDIAGNOSIS"),df4("CODETYPE")).orderBy(df4("INST_OF_UPDATE_DTTM_diag").desc, df4("PRIMARYDIAGNOSIS").desc)
    val dedupe_fpiv = df4.withColumn("rw", row_number.over(groups))
    dedupe_fpiv.filter(" rw = 1 and ((codetype = 'ICD9' and drop_icd9s = '0') or (codetype = 'ICD10' and drop_icd10s = '0'))" )
  }
}
