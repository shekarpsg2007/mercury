package com.humedica.mercury.etl.fdr.diagnosis


import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



class DiagnosisBackend(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("Diagnosis:"+config("EMR")+"@Diagnosis", "cdr.map_predicate_values", "cdr.ref_diag_codes",
    "Clinicalencounter:"+config("EMR")+"@Clinicalencounter", "mpitemp:fdr.mpi.MpiPatient")


  columns = List("GROUPID", "DATASRC", "PATIENTID", "ENCOUNTERID", "FACILITYID", "DX_TIMESTAMP", "LOCALDIAGNOSIS", "PRIMARYDIAGNOSIS",
    "LOCALPRESENTONADMISSION", "MAPPEDDIAGNOSIS","SOURCEID", "LOCALDIAGNOSISSTATUS", "MAPPEDDIAGNOSISSTATUS", "LOCALDIAGNOSISPROVIDERID",
    "LOCALADMITFLG", "LOCALDISCHARGEFLG", "LOCALACTIVEIND", "RESOLUTIONDATE", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "HOSP_DX_FLAG", "CODETYPE",
    "ORIG_MAPPEDDIAGNOSIS", "ORIG_CODETYPE")


  columnSelect = Map(
    "Clinicalencounter" -> List("ENCOUNTERID", "DISCHARGETIME", "ARRIVALTIME")
  )


  beforeJoin = Map(
    "cdr.map_predicate_values" -> ((df: DataFrame) => {
      df.filter("ENTITY = 'DIAG_SKIP_FORMAT' and TABLE_NAME = 'DIAGNOSIS' " +
        "and GROUPID = '" + config(GROUP) + "' and CLIENT_DS_ID = '" + config(CLIENT_DS_ID) + "'")
        .withColumnRenamed("GROUPID", "GROUPID_mpv").drop("CLIENT_DS_ID")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("Diagnosis")
      .join(dfs("cdr.ref_diag_codes"),
        upper(regexp_replace(dfs("Diagnosis")("MAPPEDDIAGNOSIS"), "\\.", "")) === dfs("cdr.ref_diag_codes")("DIAGNOSIS_CODE"), "left_outer")
      .join(dfs("Clinicalencounter"), Seq("ENCOUNTERID"), "left_outer")
      .join(dfs("cdr.map_predicate_values"), dfs("Diagnosis")("DATASRC") === dfs("cdr.map_predicate_values")("DATA_SRC"), "left_outer")
      .join(dfs("mpitemp"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
  }



  afterJoin = (df: DataFrame) => {
      val df1 = df.withColumn("CT_MOD",
          when(df("CODETYPE").isNotNull && !df("CODETYPE").isin("ICD9", "ICD10"), df("CODETYPE"))
         .when(df("CODETYPE") === "ICD9",
             when((df("IS_ICD9") === "Y") && (df("IS_ICD10") === "N"), "ICD9")
            .when((df("IS_ICD10") === "Y") && (df("IS_ICD9") === "N"), "ICD10")
            .when(!length(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", "")).between(3,5), "UNKNOWN")
            .when(!(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),1,1).rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$").isNotNull ||
              upper(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),1,1)).isin("E", "V")), "UNKNOWN")
            .otherwise(df("CODETYPE")))
         .when(df("CODETYPE") === "ICD10",
             when((df("IS_ICD9") === "N") && (df("IS_ICD10") === "Y"), "ICD10")
            .when((df("IS_ICD9") === "Y") && (df("IS_ICD10") === "N"), "ICD9")
               .when(!length(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", "")).between(3,7), "UNKNOWN")
            .when(!upper(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),1,1)).rlike("[A-Z]"), "UNKNOWN")
            .when(isnull(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),2,1).rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$")), "UNKNOWN")
            .otherwise(df("CODETYPE")))
         .when(!length(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", "")).between(3,7), "OTHER")
         .when((df("IS_ICD9") === "Y") && (df("IS_ICD10") === "N"), "ICD9")
         .when((df("IS_ICD10") === "Y") && (df("IS_ICD9") === "N"), "ICD10")
         .when(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),1,1).rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$").isNotNull &&
           length(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", "")).between(3,5), "ICD9")
         .when(upper(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),1,1)).rlike("[A-Z]") &&
           (!upper(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),1,1)).isin("E", "V")) &&
           length(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", "")).between(3,7), "ICD10")
         .when(!upper(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),1,1)).isin("E", "V"), "OTHER")
         .when(isnull(df("GROUPID_mpv")) && (upper(substring(df("MAPPEDDIAGNOSIS"),1,1)) === "E") &&
          (locate(".", df("LOCALDIAGNOSIS")).isin(4,5) || locate(".", df("MAPPEDDIAGNOSIS")).isin(4,5)),
            when((regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", "") === regexp_replace(df("LOCALDIAGNOSIS"), "\\.", "")) &&
              (locate(".", df("LOCALDIAGNOSIS")) === 4), "ICD10")
           .when((regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", "") === regexp_replace(df("LOCALDIAGNOSIS"), "\\.", "")) &&
             (locate(".", df("LOCALDIAGNOSIS")) === 5), "ICD9")
           .when(locate(".", df("MAPPEDDIAGNOSIS")) === 4, "ICD10")
           .when(locate(".", df("MAPPEDDIAGNOSIS")) === 5, "ICD9") )
         .when(upper(substring(regexp_replace(df("MAPPEDDIAGNOSIS"), "\\.", ""),1,1)).isin("E", "V") &&
           (coalesce(df("DISCHARGETIME"), df("ARRIVALTIME"), df("DX_TIMESTAMP")) < "20151001"), "ICD9")
         .otherwise("UNKNOWN")
      )
      .withColumn("CLEAN_MD", upper(regexp_replace(trim(df("MAPPEDDIAGNOSIS")), "\\.", "")))
      df1.withColumn("MD_MOD",
         when(df1("CT_MOD") === "ICD9",
           when((upper(substring(df1("CLEAN_MD"),1,1)) === "E") && (length(df1("CLEAN_MD")) === 5),
            concat_ws(".", substring(df1("CLEAN_MD"),1,4), substring(df1("CLEAN_MD"),5,1)))
          .when((upper(substring(df1("CLEAN_MD"),1,1)) === "E") && (length(df1("CLEAN_MD")) === 4), substring(df1("CLEAN_MD"),1,4))
          .when(upper(substring(df1("CLEAN_MD"),1,1)).rlike("[0-9V]") && length(df1("CLEAN_MD")).between(3,5),
            concat_ws(".", substring(df1("CLEAN_MD"),1,3), substring(df1("CLEAN_MD"),4,2)))
          .when(length(df1("CLEAN_MD")) leq 3, substring(df1("CLEAN_MD"),1,3))
          .otherwise(df1("MAPPEDDIAGNOSIS"))
        )
        .when(df1("CT_MOD") === "ICD10",
           when(length(df1("CLEAN_MD")).between(4,7), concat_ws(".", substring(df1("CLEAN_MD"),1,3), substring(df1("CLEAN_MD"),4,4)))
          .when(length(df1("CLEAN_MD")) leq 3, substring(df1("CLEAN_MD"),1,3))
          .otherwise(df1("MAPPEDDIAGNOSIS"))
      ))
    }


  map = Map(
    "PATIENTID" -> mapFrom("PATIENTID"),
    "CODETYPE" -> mapFrom("CT_MOD"),
    "MAPPEDDIAGNOSIS" -> mapFrom("MD_MOD"),
    "GRP_MPI" -> mapFrom("MPI")
  )

  //val es = new DiagnosisBackend(cfg)
  //val sdiag = build(es, allColumns=true)


}
