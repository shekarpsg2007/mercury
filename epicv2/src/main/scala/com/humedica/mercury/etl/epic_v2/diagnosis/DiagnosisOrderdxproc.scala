package com.humedica.mercury.etl.epic_v2.diagnosis

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/27/2017
 */


class DiagnosisOrderdxproc(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("order_dx_proc", "zh_v_edg_hx_icd9","zh_edg_curr_icd9","generalorders","zh_edg_curr_icd10", "zh_v_edg_hx_icd10",
    "zh_clarity_edg", "Temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable",
    "cdr.map_predicate_values")


  columnSelect = Map(
    "order_dx_proc" -> List("PAT_ENC_CSN_ID", "DX_ID", "ORDER_PROC_ID", "PAT_ID", "UPDATE_DATE"),
    "generalorders" -> List("ORDER_PROC_ID", "ORDER_TIME"),
    "Temptable" -> List("PAT_ENC_CSN_ID", "ARRIVALTIME", "ENC_TYPE_C", "APPT_PRC_ID"),
    "zh_clarity_edg" -> List("DX_ID", "REF_BILL_CODE", "REF_BILL_CODE_SET_C", "RECORD_TYPE_C"),
    "zh_edg_curr_icd9" -> List("DX_ID", "CODE"),
    "zh_edg_curr_icd10" -> List("DX_ID", "CODE"),
    "zh_v_edg_hx_icd9" -> List("DX_ID", "CODE", "EFF_START_DATE", "EFF_END_DATE"),
    "zh_v_edg_hx_icd10" -> List("DX_ID", "CODE", "EFF_START_DATE", "EFF_END_DATE")
  )


  beforeJoin = Map(
    "order_dx_proc" -> ((df: DataFrame) => {
      val list_dx_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDER_DX_PROC", "DIAGNOSIS", "ORDER_DX_PROC", "DX_ID")
      df.filter("DX_ID is null or DX_ID not in (" + list_dx_id + ")").select("PAT_ENC_CSN_ID", "DX_ID", "ORDER_PROC_ID", "PAT_ID", "UPDATE_DATE")
    }),
    "zh_edg_curr_icd9"  -> renameColumn("CODE", "ECODE9"),
    "zh_edg_curr_icd10" -> renameColumn("CODE", "ECODE10"),
    "zh_v_edg_hx_icd9"  -> renameColumns(List(("CODE", "HCODE9"), ("EFF_START_DATE", "START9"), ("EFF_END_DATE", "END9"), ("DX_ID", "DX_ID9"))),
    "zh_v_edg_hx_icd10" -> renameColumns(List(("CODE", "HCODE10"), ("EFF_START_DATE", "START10"), ("EFF_END_DATE", "END10"), ("DX_ID", "DX_ID10"))))



  join = (dfs: Map[String,DataFrame]) => {
    dfs("generalorders")
      .join(dfs("order_dx_proc")
        .join(dfs("Temptable"), Seq("PAT_ENC_CSN_ID"), "left_outer")
        .join(dfs("zh_clarity_edg"), Seq("DX_ID"), "left_outer")
        .join(dfs("zh_edg_curr_icd9"), Seq("DX_ID"), "left_outer")
        .join(dfs("zh_edg_curr_icd10"), Seq("DX_ID"), "left_outer")
      ,Seq("ORDER_PROC_ID"), "left_outer")
      .join(dfs("zh_v_edg_hx_icd9"), dfs("order_dx_proc")("DX_ID") === dfs("zh_v_edg_hx_icd9")("DX_ID9") &&
        (coalesce(dfs("generalorders")("ORDER_TIME"),dfs("Temptable")("ARRIVALTIME")).between(dfs("zh_v_edg_hx_icd9")("START9"),dfs("zh_v_edg_hx_icd9")("END9"))), "left_outer")
      .join(dfs("zh_v_edg_hx_icd10"), dfs("order_dx_proc")("DX_ID") === dfs("zh_v_edg_hx_icd10")("DX_ID10") &&
        (coalesce(dfs("generalorders")("ORDER_TIME"),dfs("Temptable")("ARRIVALTIME")).between(dfs("zh_v_edg_hx_icd10")("START10"),dfs("zh_v_edg_hx_icd10")("END10"))), "left_outer")


  }

  map = Map(
    "DATASRC" -> literal("order_dx_proc"),
    "DX_TIMESTAMP" -> cascadeFrom(Seq("ORDER_TIME", "ARRIVALTIME")),
    "LOCALDIAGNOSIS" -> mapFrom("DX_ID", nullIf=Seq("-1")),
    "PATIENTID" -> mapFrom("PAT_ID", nullIf=Seq("-1")),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID", nullIf=Seq("-1"))
    /*
    "MAPPEDDIAGNOSIS" -> ((col: String, df: DataFrame) => {

      df.withColumn(col, when(df("RECORD_TYPE_C").isin("1", "-1") || isnull(df("RECORD_TYPE_C")), df("STANDARDCODE"))
        .when(df("REF_BILL_CODE").isin("IMO0001", "000.0"), null).otherwise(df("REF_BILL_CODE")))
    }),
    "CODETYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RECORD_TYPE_C").isin("1", "-1") || isnull(df("RECORD_TYPE_C")), df("STANDARDCODETYPE"))
        .when(df("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
        .when(df("REF_BILL_CODE_SET_C") === "1", "ICD9")
        .when(df("REF_BILL_CODE_SET_C") === "2", "ICD10")
      )
    })
    */
  )

  afterMap = (df: DataFrame) => {

    val df1 = df.repartition(1000)
    val drop_diag = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPIC", "DIAGNOSIS", "DIAGNOSIS", "LOCALDIAGNOSIS")
    val list_enc_type_c_incl = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "ENC_TYPE_C_incl")
    val list_enc_type_c_excl = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "ENC_TYPE_C_excl")

    val addColumn = df1.withColumn("MAPPED_ICD9", coalesce(df1("HCODE9"), df1("ECODE9")))
      .withColumn("MAPPED_ICD10", coalesce(df1("HCODE10"), df1("ECODE10")))

    val fpiv = unpivot(
      Seq("MAPPED_ICD9", "MAPPED_ICD10"),
      Seq("ICD9", "ICD10"), typeColumnName = "STANDARDCODETYPE")
    val fpiv2 = fpiv("STANDARDCODE", addColumn)

    val mappeddiag = fpiv2.withColumn("MAPPEDDIAGNOSIS", when(fpiv2("RECORD_TYPE_C").isin("1", "-1") || isnull(fpiv2("RECORD_TYPE_C")), fpiv2("STANDARDCODE"))
      .when(fpiv2("REF_BILL_CODE").isin("IMO0001", "000.0"), null).otherwise(fpiv2("REF_BILL_CODE")))

    val cdtype = mappeddiag.withColumn("CODETYPE", when(mappeddiag("RECORD_TYPE_C").isin("1", "-1") || isnull(mappeddiag("RECORD_TYPE_C")), mappeddiag("STANDARDCODETYPE"))
      .when(mappeddiag("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "1", "ICD9")
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "2", "ICD10"))

    val ct_window = Window.partitionBy(cdtype("PATIENTID"), cdtype("DX_TIMESTAMP"), cdtype("ENCOUNTERID"), cdtype("LOCALDIAGNOSIS"))
    val fil = cdtype.withColumn("ICD9_CT", sum(when(cdtype("CODETYPE") === lit("ICD9"), 1).otherwise(0)).over(ct_window))
      .withColumn("ICD10_CT", sum(when(cdtype("CODETYPE") === lit("ICD10"), 1).otherwise(0)).over(ct_window))

    val icdfil = fil.withColumn("drop_icd9s",
      when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP >= from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
        when(fil("ICD9_CT").gt(lit(0)) && fil("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .withColumn("drop_icd10s",
        when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP < from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
          when(fil("ICD9_CT").gt(lit(0)) && fil("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .filter("PATIENTID is not null and LOCALDIAGNOSIS is not null and DX_TIMESTAMP is not null")
      .filter("('NO_MPV_MATCHES' in (" + list_enc_type_c_incl + ") or " +
        "((coalesce(ENC_TYPE_C, 'X') in (" + list_enc_type_c_incl + ") and " +
        "(coalesce(ENC_TYPE_C, 'X') not in (" + list_enc_type_c_excl + ") or APPT_PRC_ID not in (" + list_enc_type_c_excl + ")))))")

    val groups = Window.partitionBy(icdfil("PATIENTID"), icdfil("DX_TIMESTAMP"), icdfil("ENCOUNTERID"), icdfil("LOCALDIAGNOSIS"), icdfil("MAPPEDDIAGNOSIS"), icdfil("CODETYPE")).orderBy(icdfil("UPDATE_DATE").desc)
    val dedup = icdfil.withColumn("rw", row_number.over(groups))
    dedup.filter("rw = 1 and ((codetype = 'ICD9' and drop_icd9s = '0') or (codetype = 'ICD10' and drop_icd10s = '0'))")

  }

 }

// TEST
// val d = new DiagnosisOrderdxproc(cfg) ; val diag = build(d) ; diag.show(false) ; diag.count
