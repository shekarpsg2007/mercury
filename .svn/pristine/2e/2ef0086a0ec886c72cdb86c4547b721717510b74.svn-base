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


class DiagnosisPatencdx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("pat_enc_dx", "zh_clarity_edg", "zh_edg_curr_icd9", "zh_edg_curr_icd10", "zh_v_edg_hx_icd9", "zh_v_edg_hx_icd10", "cdr.map_predicate_values")

  columnSelect = Map(
    "pat_enc_dx" -> List("CONTACT_DATE", "DX_ID", "PAT_ID", "PAT_ENC_CSN_ID", "PRIMARY_DX_YN", "UPDATE_DATE"),
    "zh_clarity_edg" -> List("DX_ID", "REF_BILL_CODE", "REF_BILL_CODE_SET_C", "RECORD_TYPE_C"),
    "zh_edg_curr_icd9" -> List("DX_ID", "CODE"),
    "zh_edg_curr_icd10" -> List("DX_ID", "CODE"),
    "zh_v_edg_hx_icd9" -> List("DX_ID", "CODE", "EFF_START_DATE", "EFF_END_DATE"),
    "zh_v_edg_hx_icd10" -> List("DX_ID", "CODE", "EFF_START_DATE", "EFF_END_DATE"))

  beforeJoin = Map(
    "pat_enc_dx" -> ((df: DataFrame) => {
      val list_dx_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PAT_ENC_DX", "DIAGNOSIS", "PAT_ENC_DX", "DX_ID")
      includeIf("DX_ID is null or DX_ID not in (" + list_dx_id + ") and CONTACT_DATE is not null " +
        "and PAT_ID is not null and PAT_ID <> '-1'")(df)
    }),
    "zh_edg_curr_icd9" -> renameColumn("CODE", "ECODE9"),
    "zh_edg_curr_icd10" -> renameColumn("CODE", "ECODE10"),
    //"zh_v_edg_hx_icd9"  -> renameColumns(List(("CODE", "HCODE9"), ("EFF_START_DATE", "START9"), ("EFF_END_DATE", "END9"))),
    //"zh_v_edg_hx_icd10" -> renameColumns(List(("CODE", "HCODE10"), ("EFF_START_DATE", "START10"), ("EFF_END_DATE", "END10"))))

    "zh_v_edg_hx_icd9" -> renameColumns(List(("CODE", "HCODE9"), ("EFF_START_DATE", "START9"), ("EFF_END_DATE", "END9"), ("DX_ID", "DX_ID9"))),
    "zh_v_edg_hx_icd10" -> renameColumns(List(("CODE", "HCODE10"), ("EFF_START_DATE", "START10"), ("EFF_END_DATE", "END10"), ("DX_ID", "DX_ID10"))))


  join = (dfs: Map[String, DataFrame]) => {
    dfs("pat_enc_dx")
      .join(dfs("zh_clarity_edg"), Seq("DX_ID"), "left_outer")
      .join(dfs("zh_edg_curr_icd9"), Seq("DX_ID"), "left_outer")
      .join(dfs("zh_edg_curr_icd10"), Seq("DX_ID"), "left_outer")
      .join(dfs("zh_v_edg_hx_icd9"), dfs("pat_enc_dx")("DX_ID") === dfs("zh_v_edg_hx_icd9")("DX_ID9") &&
        (dfs("pat_enc_dx")("CONTACT_DATE").between(dfs("zh_v_edg_hx_icd9")("START9"), dfs("zh_v_edg_hx_icd9")("END9"))), "left_outer")
      .join(dfs("zh_v_edg_hx_icd10"), dfs("pat_enc_dx")("DX_ID") === dfs("zh_v_edg_hx_icd10")("DX_ID10") &&
        (dfs("pat_enc_dx")("CONTACT_DATE").between(dfs("zh_v_edg_hx_icd10")("START10"), dfs("zh_v_edg_hx_icd10")("END10"))), "left_outer")

  }


  map = Map(
    "DATASRC" -> literal("pat_enc_dx"),
    "DX_TIMESTAMP" -> mapFrom("CONTACT_DATE"),
    "LOCALDIAGNOSIS" -> mapFrom("DX_ID", nullIf = Seq("-1")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID", nullIf = Seq("-1")),
    "PRIMARYDIAGNOSIS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PRIMARY_DX_YN") === "Y", 1)
        .when(df("PRIMARY_DX_YN") === "N", 0).otherwise(null))
    })
  )

afterMap = (df: DataFrame) => {

  val df1 = df.repartition(1000)
  val drop_diag = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPIC", "DIAGNOSIS", "DIAGNOSIS", "LOCALDIAGNOSIS")

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

  val groups = Window.partitionBy(icdfil("PATIENTID"), icdfil("DX_TIMESTAMP"), icdfil("ENCOUNTERID"), icdfil("LOCALDIAGNOSIS"), icdfil("MAPPEDDIAGNOSIS"), icdfil("CODETYPE")).orderBy(icdfil("UPDATE_DATE").desc)
  val dedupe = icdfil.withColumn("rw", row_number.over(groups))
  dedupe.filter("rw = 1 and ((codetype = 'ICD9' and drop_icd9s = '0') or (codetype = 'ICD10' and drop_icd10s = '0'))" )
}
}


// test
// val d = new DiagnosisPatencdx(cfg) ; val dg = build(d,allColumns=true) ; dg.show ; dg.count
