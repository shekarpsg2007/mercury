package com.humedica.mercury.etl.cerner_v2.diagnosis

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 08/09/2018
  */


class DiagnosisDiagnosis(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("diagnosis",
    "zh_nomenclature",
    "cdr.ref_snomed_icd9",
    "cdr.ref_snomed_icd10",
    "cdr.map_predicate_values",
    "tempce:cerner_v2.clinicalencounter.ClinicalencounterEncounter"
  )

  columnSelect = Map(
    "zh_nomenclature" -> List("SOURCE_IDENTIFIER","SOURCE_VOCABULARY_CD","PRINCIPLE_TYPE_CD","NOMENCLATURE_ID","CONCEPT_CKI"),
    "diagnosis" -> List("BEG_EFFECTIVE_DT_TM", "NOMENCLATURE_ID", "PERSON_ID", "ACTIVE_STATUS_CD", "DIAG_TYPE_CD","DIAG_DT_TM",
      "CONFIRMATION_STATUS_CD", "DIAG_TYPE_CD", "DIAG_PRSNL_ID", "ENCNTR_ID", "PRESENT_ON_ADMIT_CD",
      "DIAG_PRIORITY","UPDT_DT_TM"),
    "cdr.ref_snomed_icd9" -> List("SNOMED_ID","ICD9CM_CODE"),
    "cdr.ref_snomed_icd10" -> List("SNOMED_ID","ICD10_CODE"),
    "tempce" -> List("ENCOUNTERID","CLIENT_DS_ID","DISCHARGETIME","ARRIVALTIME","ADMITTIME")
  )

  beforeJoin = Map(
    "zh_nomenclature" -> ((df: DataFrame) => {
      val list_i9_source_vocabulary_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_ICD9", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_i10_source_vocabulary_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_ICD10", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_sm_source_vocabulary_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_SNOMED", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_prin_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS", "DIAGNOSIS", "DIAGNOSIS", "PRINCIPLE_TYPE_CD")

      val fil = df.filter("PRINCIPLE_TYPE_CD in ("+ list_prin_type_cd + ") AND " +
        "(SOURCE_VOCABULARY_CD in (" + list_i9_source_vocabulary_cd + ") or SOURCE_VOCABULARY_CD in (" + list_i10_source_vocabulary_cd + ") " +
        "or SOURCE_VOCABULARY_CD in (" + list_sm_source_vocabulary_cd + "))")
      fil.withColumn("CODE_TYPE", when(concat_ws("", lit("'"),fil("SOURCE_VOCABULARY_CD"),lit("'")).isin(list_i9_source_vocabulary_cd), lit("ICD9"))
        .when(concat_ws("", lit("'"),fil("SOURCE_VOCABULARY_CD"),lit("'")).isin(list_i10_source_vocabulary_cd), lit("ICD10"))
        .when(concat_ws("", lit("'"),fil("SOURCE_VOCABULARY_CD"),lit("'")).isin(list_sm_source_vocabulary_cd), lit("SNOMED")))
        .withColumn("CONCEPTCKI", regexp_replace(fil("CONCEPT_CKI"), "SNOMED!",""))
        .withColumnRenamed("NOMENCLATURE_ID","ZH_NOMENCLATURE_ID")
    }),
    "diagnosis" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val list_diag_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS", "DIAGNOSIS", "DIAGNOSIS_EXCLU", "DIAG_TYPE_CD")
      val fil = df1.filter("PERSON_ID is not null and NOMENCLATURE_ID <> 0 and " +
        "('NO_MPV_MATCHES' in (" +list_diag_type_cd + ") or ('NO_MPV_MATCHES' not in (" + list_diag_type_cd + ") and DIAG_TYPE_CD not in (" + list_diag_type_cd + ")))") //'&client_ds_id..' || dx.diag_type_cd not in (&list_diag_type_cd)))
      val ce = table("tempce").repartition(1000)
      val joined = fil.join(ce, fil("ENCNTR_ID")=== ce("ENCOUNTERID"), "left_outer")
      joined.withColumn("DIAG_CODETYPE", when(from_unixtime(unix_timestamp(coalesce(joined("DISCHARGETIME"),joined("ARRIVALTIME"),joined("ADMITTIME"),coalesce(joined("BEG_EFFECTIVE_DT_TM"),joined("DIAG_DT_TM"))))) < to_date(lit("2015-10-01")), lit("ICD9"))
        .when(from_unixtime(unix_timestamp(coalesce(joined("DISCHARGETIME"),joined("ARRIVALTIME"),joined("ADMITTIME"),coalesce(joined("BEG_EFFECTIVE_DT_TM"),joined("DIAG_DT_TM"))))) >= to_date(lit("2015-10-01")), lit("ICD10"))
      )
    }),
    "cdr.ref_snomed_icd9" -> ((df: DataFrame) => {
      val tbl1 = df.withColumn("SNO_CODETYPE", lit("ICD9"))
        .withColumnRenamed("ICD9CM_CODE", "ICD_DX")
        .select("SNOMED_ID","ICD_DX","SNO_CODETYPE")
      val tbl2 = table("cdr.ref_snomed_icd10").withColumn("SNO_CODETYPE", lit("ICD10"))
        .withColumnRenamed("ICD10_CODE", "ICD_DX")
        .select("SNOMED_ID","ICD_DX","SNO_CODETYPE")
      tbl1.union(tbl2)
        .select("SNOMED_ID","ICD_DX","SNO_CODETYPE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("diagnosis")
      .join(dfs("zh_nomenclature"), dfs("diagnosis")("NOMENCLATURE_ID") === dfs("zh_nomenclature")("ZH_NOMENCLATURE_ID"), "inner")
      .join(dfs("cdr.ref_snomed_icd9"), (dfs("zh_nomenclature")("CONCEPTCKI") === dfs("cdr.ref_snomed_icd9")("SNOMED_ID"))
        && (dfs("diagnosis")("DIAG_CODETYPE") === dfs("cdr.ref_snomed_icd9")("SNO_CODETYPE"))
        && (dfs("zh_nomenclature")("CODE_TYPE") === lit("SNOMED")), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("diagnosis"),
    "DX_TIMESTAMP" -> cascadeFrom(Seq("BEG_EFFECTIVE_DT_TM", "DIAG_DT_TM")),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "LOCALACTIVEIND" -> mapFrom("ACTIVE_STATUS_CD", prefix=config(CLIENT_DS_ID)+"."),
    "LOCALADMITFLG" -> mapFrom("DIAG_TYPE_CD", prefix=config(CLIENT_DS_ID)+"."),
    "LOCALDIAGNOSISSTATUS" -> mapFrom("CONFIRMATION_STATUS_CD", nullIf=Seq("0"), prefix=config(CLIENT_DS_ID)+"."),
    "LOCALDISCHARGEFLG" -> mapFrom("DIAG_TYPE_CD", prefix=config(CLIENT_DS_ID)+"."),
    "LOCALDIAGNOSISPROVIDERID" -> mapFrom("DIAG_PRSNL_ID", nullIf=Seq("0")),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALPRESENTONADMISSION" -> mapFrom("PRESENT_ON_ADMIT_CD", nullIf=Seq("0"), prefix=config(CLIENT_DS_ID)+"."),
    "PRIMARYDIAGNOSIS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DIAG_PRIORITY") === lit("1"), lit("1")).otherwise(lit("0")))
    })
  )

  afterMap = (df: DataFrame) => {
    val list_confirm_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS", "DIAGNOSIS", "DIAGNOSIS", "CONFIRMATION_STATUS_CD")
    val list_diag_active_status_cd  = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS", "DIAGNOSIS", "DIAGNOSIS", "ACTIVE_STATUS_CD")

    val df1 = df.withColumn("LOCAL_DIAG",concat_ws("", df("CODE_TYPE"), lit("."), df("NOMENCLATURE_ID")))
    val groups1 = Window.partitionBy(df1("PATIENTID"),df1("ENCOUNTERID"),df1("LOCAL_DIAG"),df1("DX_TIMESTAMP"))
      .orderBy(df1("UPDT_DT_TM").desc_nulls_last, df1("PRIMARYDIAGNOSIS").desc_nulls_last)
    val addColumn1 = df1.withColumn("rownum", row_number.over(groups1))

    val fil1 = addColumn1.filter("nvl(confirmation_status_cd,'X') in (" + list_confirm_status_cd + ") and concat('" + config(CLIENT_DS_ID) + ".', ACTIVE_STATUS_CD) in (" + list_diag_active_status_cd + ")")

    val tbl1 = fil1.withColumn("LOCALDIAGNOSIS", fil1("LOCAL_DIAG"))
      .withColumn("CODETYPE", fil1("CODE_TYPE"))
      .withColumn("MAPPEDDIAGNOSIS", when(fil1("CODE_TYPE") === lit("SNOMED"), fil1("CONCEPTCKI")).otherwise(fil1("SOURCE_IDENTIFIER")))
      .filter("rownum = 1 and MAPPEDDIAGNOSIS not like '%, %'")

    val tbl2 = fil1.withColumn("LOCALDIAGNOSIS", when(fil1("CODE_TYPE") === lit("SNOMED"), concat_ws("", lit("SNOMED."), fil1("NOMENCLATURE_ID"))))
      .withColumn("CODETYPE", fil1("SNO_CODETYPE"))
      .withColumn("MAPPEDDIAGNOSIS", when(fil1("CODE_TYPE") === lit("SNOMED"), fil1("ICD_DX")))
      .filter("rownum = 1 and MAPPEDDIAGNOSIS is not null")

    tbl1.union(tbl2)
  }

}

//val es = new DiagnosisDiagnosis(cfg) ; val diagd = build(es,allColumns=true)