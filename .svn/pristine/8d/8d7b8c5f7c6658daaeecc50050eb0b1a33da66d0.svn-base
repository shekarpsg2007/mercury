package com.humedica.mercury.etl.epic_v2.diagnosis

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 01/27/2017
  */


class DiagnosisInptbillingdx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "inptbilling_dx",
    "hsp_acct_pat_csn",
    "zh_clarity_edg",
    "zh_v_edg_hx_icd9",
    "zh_edg_curr_icd9",
    "zh_v_edg_hx_icd10",
    "zh_edg_curr_icd10",
    "encountervisit:epic_v2.clinicalencounter.ClinicalencounterEncountervisit",
    "diagunion:epic_v2.diagnosis.DiagnosisTemptableInptbillingdx",
    "inptbilling_acct",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "inptbilling_dx" -> List("HSP_ACCOUNT_ID", "DX_ID", "DX_POA_YNU", "FINAL_DX_POA_C", "LINE", "INST_OF_UPDATE","FILEID"),
    "hsp_acct_pat_csn" -> List("HSP_ACCOUNT_ID", "PAT_ENC_CSN_ID", "PAT_ENC_DATE", "PAT_ID","FILEID"),
    "inptbilling_acct" -> List("HSP_ACCOUNT_NAME", "HSP_ACCOUNT_ID", "CODING_STATUS_C", "FILEID", "PRIM_ENC_CSN_ID", "ADM_DATE_TIME", "PAT_ID","INST_OF_UPDATE"),
    "encountervisit" -> List("ENCOUNTERID", "ARRIVALTIME"),
    "zh_clarity_edg" -> List("DX_ID", "REF_BILL_CODE", "REF_BILL_CODE_SET_C", "RECORD_TYPE_C"),
    "zh_edg_curr_icd9" -> List("DX_ID", "CODE"),
    "diagunion" -> List("HSP_ACCOUNT_ID_UN","PAT_ENC_CSN_ID_UN","SERVICE_DATE_UN"),
    "zh_edg_curr_icd10" -> List("DX_ID", "CODE"),
    "zh_v_edg_hx_icd9" -> List("DX_ID", "CODE", "EFF_START_DATE", "EFF_END_DATE"),
    "zh_v_edg_hx_icd10" -> List("DX_ID", "CODE", "EFF_START_DATE", "EFF_END_DATE"))

  beforeJoin = Map(
    "inptbilling_dx" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("HSP_ACCOUNT_ID"), df1("LINE")).orderBy(df1("INST_OF_UPDATE").desc_nulls_last,df1("FILEID").desc_nulls_last)

      df1.withColumn("inpt_rownumber", row_number.over(groups))
        .withColumnRenamed("INST_OF_UPDATE","INST_OF_UPDATE_DX")
        .select("HSP_ACCOUNT_ID", "DX_ID", "DX_POA_YNU", "FINAL_DX_POA_C", "LINE", "inpt_rownumber","INST_OF_UPDATE_DX")
        .filter("inpt_rownumber = 1")
    }),
    "inptbilling_acct" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID")).orderBy(df("INST_OF_UPDATE").desc,df("FILEID").desc)
      df.withColumn("acct_rw", row_number.over(groups))
        .withColumnRenamed("PAT_ID","ACCT_PAT_ID")
        .filter("acct_rw =1 and lower(HSP_ACCOUNT_NAME) not like 'zzz%'")
    }),
    "hsp_acct_pat_csn" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID")).orderBy(df("FILEID").desc)
      val dedup = df.withColumn("hsp_rw", row_number.over(groups))
      dedup.filter("hsp_rw = 1")
    }),
    "encountervisit"  -> renameColumn("ENCOUNTERID", "ENCOUNTERID_ce"),
    "zh_edg_curr_icd9"  -> renameColumn("CODE", "ECODE9"),
    "zh_edg_curr_icd10" -> renameColumn("CODE", "ECODE10"),
    "zh_v_edg_hx_icd9"  -> renameColumns(List(("CODE", "HCODE9"), ("EFF_START_DATE", "START9"), ("EFF_END_DATE", "END9"), ("DX_ID", "DX_ID9"))),
    "zh_v_edg_hx_icd10" -> renameColumns(List(("CODE", "HCODE10"), ("EFF_START_DATE", "START10"), ("EFF_END_DATE", "END10"), ("DX_ID", "DX_ID10"))))

  join = (dfs: Map[String,DataFrame]) => {
    //val getNull = udf(() => None: Option[String])
    dfs("inptbilling_dx")
      .join(dfs("hsp_acct_pat_csn"), Seq("HSP_ACCOUNT_ID"), "left_outer")
      .join(dfs("inptbilling_acct"), Seq("HSP_ACCOUNT_ID"), "left_outer")
      .join(dfs("diagunion"), dfs("inptbilling_dx")("HSP_ACCOUNT_ID") === dfs("diagunion")("HSP_ACCOUNT_ID_UN"), "left_outer")
      .join(dfs("encountervisit"), dfs("diagunion")("PAT_ENC_CSN_ID_UN") === dfs("encountervisit")("ENCOUNTERID_ce"), "left_outer")
      .join(dfs("zh_clarity_edg"), Seq("DX_ID"), "left_outer")
      .join(dfs("zh_edg_curr_icd9"), Seq("DX_ID"), "left_outer")
      .join(dfs("zh_edg_curr_icd10"), Seq("DX_ID"), "left_outer")
      .join(dfs("zh_v_edg_hx_icd9"), dfs("inptbilling_dx")("DX_ID") === dfs("zh_v_edg_hx_icd9")("DX_ID9") &&
        dfs("diagunion")("SERVICE_DATE_UN").between(dfs("zh_v_edg_hx_icd9")("START9"), dfs("zh_v_edg_hx_icd9")("END9")), "left_outer")
      .join(dfs("zh_v_edg_hx_icd10"), dfs("inptbilling_dx")("DX_ID") === dfs("zh_v_edg_hx_icd10")("DX_ID10") &&
        dfs("diagunion")("SERVICE_DATE_UN").between(dfs("zh_v_edg_hx_icd10")("START10"), dfs("zh_v_edg_hx_icd10")("END10")), "left_outer")
  }

  /*
  afterJoin = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val drop_diag = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPIC", "DIAGNOSIS", "DIAGNOSIS", "LOCALDIAGNOSIS")
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
    val fil = df1.filter("((coalesce(CODING_STATUS_C, 'X') in (" + list_coding_status_c + ") or 'NO_MPV_MATCHES' in (" + list_coding_status_c + "))) and inpt_rownumber = 1")
    val ct_window = Window.partitionBy(df1("PAT_ID"), coalesce(df1("ARRIVALTIME"), df1("PAT_ENC_DATE"), df1("ADM_DATE_TIME")),
      df1("PRIM_ENC_CSN_ID"), df1("DX_ID"))
    val addColumn = fil.withColumn("MAPPED_ICD9", coalesce(fil("HCODE9"), fil("ECODE9"))).withColumn("MAPPED_ICD10", coalesce(fil("HCODE10"), fil("ECODE10")))
    val addColumn2 = addColumn
      .withColumn("ICD9_CT", count(addColumn("MAPPED_ICD9")).over(ct_window))
      .withColumn("ICD10_CT", count(addColumn("MAPPED_ICD10")).over(ct_window))
    val fpiv = unpivot(
      Seq("MAPPED_ICD9", "MAPPED_ICD10"),
      Seq("ICD9", "ICD10"), typeColumnName = "STANDARDCODETYPE"
    )
    val fpiv2 = fpiv("STANDARDCODE", addColumn2)
    fpiv2.withColumn("drop_icd9s",
      when(lit("'Y'") === drop_diag && expr("coalesce(ARRIVALTIME, PAT_ENC_DATE, ADM_DATE_TIME) >= from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
        when(fpiv2("ICD9_CT").gt(lit(0)) && fpiv2("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .withColumn("drop_icd10s",
        when(lit("'Y'") === drop_diag && expr("coalesce(ARRIVALTIME, PAT_ENC_DATE, ADM_DATE_TIME) < from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
          when(fpiv2("ICD9_CT").gt(lit(0)) && fpiv2("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
  }
  */

  map = Map(
    "DATASRC" -> literal("inptbilling_dx"),
    "DX_TIMESTAMP" -> mapFrom("SERVICE_DATE_UN"),
    "LOCALDIAGNOSIS" -> mapFrom("DX_ID", nullIf=Seq("-1")),
    "PATIENTID" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, coalesce(when(df("PAT_ID") === lit("-1"), lit(null)).otherwise(df("PAT_ID")),
        when(df("ACCT_PAT_ID")  === lit("-1"), lit(null)).otherwise(df("ACCT_PAT_ID"))))
    }),
    "HOSP_DX_FLAG" -> literal("Y"),
    "SOURCEID" -> mapFrom("PAT_ENC_CSN_ID_UN"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID_UN"),
    "LOCALPRESENTONADMISSION" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, when(df("FINAL_DX_POA_C").isNotNull, concat(lit(config(CLIENT_DS_ID)+"."), df("FINAL_DX_POA_C")))
        .otherwise(df("DX_POA_YNU")))
    }),
    "PRIMARYDIAGNOSIS" -> ((col:String,df:DataFrame) => df.withColumn(col, when(df("LINE") === "1", "1").otherwise("0")))
  )

  /*
  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("PATIENTID"), df1("ENCOUNTERID"), df1("DX_TIMESTAMP"), df1("LOCALDIAGNOSIS"), df1("MAPPEDDIAGNOSIS"),df1("CODETYPE")).orderBy(df1("INST_OF_UPDATE_DX").desc)
    val dedup = df1.withColumn("rownumber", row_number.over(groups))
      .withColumn("incl_val", when(coalesce(df("ENCOUNTERID"), lit("-1")) =!= lit("-1") && coalesce(df("ENCOUNTERID_ce"),lit("-1")) === lit("-1"), "1").otherwise(null))
    val dedup2 = dedup.filter("rownumber = 1 and DX_TIMESTAMP is not null and PATIENTID is not null and LOCALDIAGNOSIS is not null")
    dedup2.filter("incl_val is null and ( (codetype = 'ICD9' and drop_icd9s = '0') or (codetype = 'ICD10' and drop_icd10s = '0') )" )
  }
  */

  afterMap = (df: DataFrame) => {

    val df1 = df.repartition(1000)
    val drop_diag = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPIC", "DIAGNOSIS", "DIAGNOSIS", "LOCALDIAGNOSIS")
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")

    val fil = df1.filter("((coalesce(CODING_STATUS_C, 'X') in (" + list_coding_status_c + ") or 'NO_MPV_MATCHES' in (" + list_coding_status_c + "))) and inpt_rownumber = 1")
    val fil2 = fil.withColumn("MAPPED_ICD9", coalesce(fil("HCODE9"), fil("ECODE9")))
                  .withColumn("MAPPED_ICD10", coalesce(fil("HCODE10"), fil("ECODE10")))

    val fpiv = unpivot(
      Seq("MAPPED_ICD9", "MAPPED_ICD10"),
      Seq("ICD9", "ICD10"), typeColumnName = "STANDARDCODETYPE"
    )
    val fpiv2 = fpiv("STANDARDCODE", fil2)

    val mappeddiag = fpiv2.withColumn("MAPPEDDIAGNOSIS",
                           when(fpiv2("RECORD_TYPE_C").isin("1", "-1") || isnull(fpiv2("RECORD_TYPE_C")), fpiv2("STANDARDCODE"))
                          .when(fpiv2("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
                          .otherwise(fpiv2("REF_BILL_CODE")))

    val cdtype = mappeddiag.withColumn("CODETYPE",  when(mappeddiag("RECORD_TYPE_C").isin("1", "-1") || isnull(mappeddiag("RECORD_TYPE_C")), mappeddiag("STANDARDCODETYPE"))
      .when(mappeddiag("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "1", "ICD9")
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "2", "ICD10"))

    val ct_window = Window.partitionBy(cdtype("PATIENTID"), cdtype("DX_TIMESTAMP"), cdtype("ENCOUNTERID"), cdtype("LOCALDIAGNOSIS"))

    val fil3 = cdtype.withColumn("ICD9_CT", sum(when(cdtype("CODETYPE") === lit("ICD9"), 1).otherwise(0)).over(ct_window))
      .withColumn("ICD10_CT", sum(when(cdtype("CODETYPE") === lit("ICD10"), 1).otherwise(0)).over(ct_window))

    val icdfil = fil3.withColumn("drop_icd9s",
      when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP >= from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
        when(fil3("ICD9_CT").gt(lit(0)) && fil3("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .withColumn("drop_icd10s",
        when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP < from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
          when(fil3("ICD9_CT").gt(lit(0)) && fil3("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))

  /*  val tbl = readTable("inptbilling_txn", config).dropDuplicates("orig_rev_tx_id")
      .filter("coalesce(orig_rev_tx_id, '-1') <> '-1'")
      .withColumnRenamed("orig_rev_tx_id", "orig_rev_tx_id_tbl")

    val df2 = icdfil.join(tbl, icdfil("TX_ID_diag") === tbl("ORIG_REV_TX_ID_tbl"), "left_outer")
    */

    val df2 = icdfil.withColumn("incl_val", when(coalesce(icdfil("ENCOUNTERID"), lit("-1")) =!= lit("-1") && coalesce(icdfil("ENCOUNTERID_ce"),lit("-1")) === lit("-1"), "1").otherwise(null))
      .filter("dx_timestamp is not null and patientid is not null and localdiagnosis is not null and incl_val is null ")

    val groups = Window.partitionBy(df2("PATIENTID"),df2("ENCOUNTERID"),df2("DX_TIMESTAMP"),df2("LOCALDIAGNOSIS"), df2("MAPPEDDIAGNOSIS"),df2("CODETYPE")).orderBy(df2("INST_OF_UPDATE_DX").desc,df2("PRIMARYDIAGNOSIS").desc)
    val dedupe_addcol = df2.withColumn("rn", row_number.over(groups))
    dedupe_addcol.filter("rn = 1 and ((codetype = 'ICD9' and drop_icd9s = '0') or (codetype = 'ICD10' and drop_icd10s = '0'))" )
  }
}

// TEST
// val d = new DiagnosisInptbillingdx(cfg) ; val diag = build(d) ; diag.show ; diag.count