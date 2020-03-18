package com.humedica.mercury.etl.fdr.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class ProcedureBackend(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  tables = List("Procedure:"+config("EMR")+"@Procedure", "cdr.ref_cpt4", "cdr.ref_hcpcs",
    "cdr.ref_icd9_px","cdr.ref_icd0_px", "cdr.ref_ub04_rev_codes", "mpitemp:fdr.mpi.MpiPatient")


  columns = List("GROUPID", "DATASRC", "FACILITYID", "ENCOUNTERID", "PATIENTID", "SOURCEID", "PERFORMINGPROVIDERID",
    "REFERPROVIDERID", "PROCEDUREDATE","PROCEDURECODESOURCETABLE", "LOCALCODE", "LOCALNAME", "CODETYPE","PROCSEQ",
    "MAPPEDCODE", "LOCALBILLINGPROVIDERID", "ORDERINGPROVIDERID", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "LOCALPRINCIPLEINDICATOR",
    "HOSP_PX_FLAG", "PERFORMING_MSTRPROVID", "ACTUALPROCDATE", "PROC_END_DATE", "ORIG_MAPPEDCODE", "ORIG_CODETYPE",
    "ROW_SOURCE", "MODIFIED_DATE")


  beforeJoin = Map(
    "cdr.ref_cpt4" -> renameColumn("PROCEDURE_CODE", "PROCEDURE_CODE_cpt"),
    "cdr.ref_hcpcs" -> renameColumn("PROCEDURE_CODE", "PROCEDURE_CODE_hp"),
    "cdr.ref_icd9_px" -> renameColumn("PROCEDURE_CODE", "PROCEDURE_CODE_i9"),
    "cdr.ref_icd0_px" -> renameColumn("PROCEDURE_CODE", "PROCEDURE_CODE_i0")
  )




  join = (dfs: Map[String, DataFrame]) => {
    dfs("Procedure")
      .join(dfs("cdr.ref_cpt4"), upper(regexp_replace(dfs("Procedure")("MAPPEDCODE"), "\\.", "")) === dfs("cdr.ref_cpt4")("PROCEDURE_CODE_cpt"), "left_outer")
      .join(dfs("cdr.ref_hcpcs"), upper(regexp_replace(dfs("Procedure")("MAPPEDCODE"), "\\.", "")) === dfs("cdr.ref_hcpcs")("PROCEDURE_CODE_hp"), "left_outer")
      .join(dfs("cdr.ref_icd9_px"), upper(regexp_replace(dfs("Procedure")("MAPPEDCODE"), "\\.", "")) === dfs("cdr.ref_icd9_px")("PROCEDURE_CODE_i9"), "left_outer")
      .join(dfs("cdr.ref_icd0_px"), upper(regexp_replace(dfs("Procedure")("MAPPEDCODE"), "\\.", "")) === dfs("cdr.ref_icd0_px")("PROCEDURE_CODE_i0"), "left_outer")
      .join(dfs("cdr.ref_ub04_rev_codes"), lpad(upper(regexp_replace(dfs("Procedure")("MAPPEDCODE"), "\\.", "")), 4, "0") === upper(regexp_replace(dfs("cdr.ref_ub04_rev_codes")("REV_CODE"), "\\.", "")), "left_outer")
      .join(dfs("mpitemp"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
  }



  afterJoin = (df: DataFrame) => {
    val df1 = df.withColumn("IN_REV_DICT", df("REV_CODE"))
      .withColumn("CLEAN_MC", upper(regexp_replace(trim(df("MAPPEDCODE")), "\\.", "")))
      .withColumn("CT_MOD",
         when(df("CODETYPE").isNotNull && (!upper(df("CODETYPE")).isin("CPT4", "HCPCS", "ICD9", "ICD10")), df("CODETYPE"))
        .when(upper(df("CODETYPE")) === "CPT4",
           when(df("MAPPEDCODE").rlike("^[0-9]{4}[0-9A-Z]$"), "CPT4")
          .when(df("PROCEDURE_CODE_hp").isNotNull && df("MAPPEDCODE").rlike("^[A-Z][0-9]{4}$"), "HCPCS")
          .when(df("PROCEDURE_CODE_i9").isNotNull && df("MAPPEDCODE").rlike("^[0-9]{2,2}\\.?[0-9]{1,2}$"), "ICD9")
          .when(df("PROCEDURE_CODE_i0").isNotNull && df("MAPPEDCODE").rlike("^[0-9A-Z]{1}[0-9A-Z]{6}$"), "ICD10")
          .otherwise("UNKNOWN"))
        .when(df("CODETYPE") === "HCPCS",
           when(df("MAPPEDCODE").rlike("^[A-Z][0-9]{4}$"), "HCPCS")
          .when(df("PROCEDURE_CODE_cpt").isNotNull && df("MAPPEDCODE").rlike("^[0-9]{4}[0-9A-Z]$"), "CPT4")
          .when(df("PROCEDURE_CODE_i9").isNotNull && df("MAPPEDCODE").rlike("^[0-9]{2,2}\\.?[0-9]{1,2}$"), "ICD9")
          .when(df("PROCEDURE_CODE_i0").isNotNull && df("MAPPEDCODE").rlike("^[0-9A-Z]{1}[0-9A-Z]{6}$"), "ICD10")
          .otherwise("UNKNOWN"))
        .when(df("CODETYPE") === "ICD9",
           when(df("MAPPEDCODE").rlike("^[0-9]{2,2}\\.?[0-9]{1,2}$"), "ICD9")
          .when(df("PROCEDURE_CODE_cpt").isNotNull && df("MAPPEDCODE").rlike("^[0-9]{4}[0-9A-Z]$"), "CPT4")
          .when(df("PROCEDURE_CODE_hp").isNotNull && df("MAPPEDCODE").rlike("^[A-Z][0-9]{4}$"), "HCPCS")
          .when(df("PROCEDURE_CODE_i0").isNotNull && df("MAPPEDCODE").rlike("^[0-9A-Z]{1}[0-9A-Z]{6}$"), "ICD19")
          .otherwise("UNKNOWN"))
        .when(df("CODETYPE") === "ICD10",
           when(df("MAPPEDCODE").rlike("^[0-9A-Z]{1}[0-9A-Z]{6}$"), "ICD10")
          .when(df("PROCEDURE_CODE_cpt").isNotNull && df("MAPPEDCODE").rlike("^[0-9]{4}[0-9A-Z]$"), "CPT4")
          .when(df("PROCEDURE_CODE_hp").isNotNull && df("MAPPEDCODE").rlike("^[A-Z][0-9]{4}$"), "HCPCS")
          .when(df("PROCEDURE_CODE_i9").isNotNull && df("MAPPEDCODE").rlike("^[0-9]{2,2}\\.?[0-9]{1,2}$"), "ICD9")
          .otherwise("UNKNOWN"))
        .when(isnull(df("CODETYPE")) || !upper(df("CODETYPE")).isin("CPT4", "HCPCS", "ICD10", "ICD9"),
           when(df("MAPPEDCODE").rlike("^[0-9]{4}[0-9A-Z]$"), "CPT4")
          .when(df("MAPPEDCODE").rlike("^[A-Z][0-9]{4}$"), "HCPCS")
          .when(df("MAPPEDCODE").rlike("^[0-9]{2,2}\\.?[0-9]{1,2}$"), "ICD9")
          .when(df("MAPPEDCODE").rlike("^[0-9A-Z]{1}[0-9A-Z]{6}$"), "ICD10")
          .otherwise("UNKNOWN")))
    df1.withColumn("MC_MOD",
       when((df1("CT_MOD") === "ICD9") && length(df1("CLEAN_MC")).between(3,4), concat_ws(".", substring(df1("CLEAN_MC"),1,2), expr("substr(CLEAN_MC,3)")))
      .when(df1("CT_MOD") === "ICD9", df1("CLEAN_MC"))
      .when(df1("CT_MOD") === "REV" && df1("IN_REV_DICT").isNotNull, df1("IN_REV_DICT"))
      .otherwise(df1("MAPPEDCODE")))

  }




  map = Map(
    "CODETYPE" -> mapFrom("CT_MOD"),
    "MAPPEDCODE" -> mapFrom("MC_MOD"),
    "GRP_MPI" -> mapFrom("MPI")
  )

  //val es = new ProcedureBackend(cfg)
  //val sproc = build(es, allColumns=true)


}

