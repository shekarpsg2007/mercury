package com.humedica.mercury.etl.cerner_v2.diagnosis

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Engine
import scala.collection.JavaConverters._


class DiagnosisProblem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_nomenclature",
                "problem",
                "cdr.ref_snomed_icd9",
                "cdr.ref_snomed_icd10",
                "cdr.map_predicate_values"
  )

  columnSelect = Map(
    "zh_nomenclature" -> List("SOURCE_IDENTIFIER","SOURCE_VOCABULARY_CD","PRINCIPLE_TYPE_CD","NOMENCLATURE_ID","CONCEPT_CKI"),
    "problem" -> List("NOMENCLATURE_ID", "ONSET_DT_TM", "PERSON_ID", "LIFE_CYCLE_STATUS_CD", "CONFIRMATION_STATUS_CD", "UPDT_DT_TM", "LIFE_CYCLE_DT_TM", "BEG_EFFECTIVE_DT_TM", "ACTIVE_IND", "CANCEL_REASON_CD", "ACTIVE_STATUS_CD"),
    "cdr.ref_snomed_icd9" -> List("SNOMED_ID","ICD9CM_CODE"),
    "cdr.ref_snomed_icd10" -> List("SNOMED_ID","ICD10_CODE")
  )

  beforeJoin = Map(
    "zh_nomenclature" -> ((df: DataFrame) => {
      val list_i9_source_vocabulary_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_ICD9", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_i10_source_vocabulary_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_ICD10", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_sm_source_vocabulary_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_SNOMED", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_combined = list_i9_source_vocabulary_cd ++ list_i10_source_vocabulary_cd ++ list_sm_source_vocabulary_cd
      val list_prin_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS", "DIAGNOSIS", "DIAGNOSIS", "PRINCIPLE_TYPE_CD")

      val fil = df.filter(df("PRINCIPLE_TYPE_CD").isin(list_prin_type_cd: _*) && df("SOURCE_VOCABULARY_CD").isin(list_combined: _*))
      fil.withColumn("CODE_TYPE", when(fil("SOURCE_VOCABULARY_CD").isin(list_i9_source_vocabulary_cd: _*), lit("ICD9"))
                                  .when(fil("SOURCE_VOCABULARY_CD").isin(list_i10_source_vocabulary_cd: _*), lit("ICD10"))
                                  .when(fil("SOURCE_VOCABULARY_CD").isin(list_sm_source_vocabulary_cd: _*), lit("SNOMED")))
         .withColumn("CONCEPTCKI", regexp_replace(fil("CONCEPT_CKI"), "SNOMED!",""))
         .withColumnRenamed("NOMENCLATURE_ID","ZH_NOMENCLATURE_ID")

    }),
    "problem"  -> ((df: DataFrame) => {
      val df1 = df.repartition(500)
      val fil = df1.filter("PERSON_ID IS NOT NULL AND NOMENCLATURE_ID <> 0 AND ACTIVE_IND != '0'")
      fil.withColumn("PROB_CODETYPE", when(from_unixtime(unix_timestamp(coalesce(fil("BEG_EFFECTIVE_DT_TM"),fil("ONSET_DT_TM"),fil("LIFE_CYCLE_DT_TM")))) < to_date(lit("2015-10-01")), lit("ICD9"))
                                     .when(from_unixtime(unix_timestamp(coalesce(fil("BEG_EFFECTIVE_DT_TM"),fil("ONSET_DT_TM"),fil("LIFE_CYCLE_DT_TM")))) >= to_date(lit("2015-10-01")), lit("ICD10"))
                    )
         .withColumn("ONSET_DATE", coalesce(fil("ONSET_DT_TM"),fil("LIFE_CYCLE_DT_TM")))
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
    dfs("problem")
      .join(dfs("zh_nomenclature"), dfs("problem")("NOMENCLATURE_ID") === dfs("zh_nomenclature")("ZH_NOMENCLATURE_ID"), "inner")
      .join(dfs("cdr.ref_snomed_icd9"), (dfs("zh_nomenclature")("CONCEPTCKI") === dfs("cdr.ref_snomed_icd9")("SNOMED_ID"))
              && (dfs("problem")("PROB_CODETYPE") === dfs("cdr.ref_snomed_icd9")("SNO_CODETYPE")), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("problem"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "LOCALACTIVEIND" -> mapFrom("LIFE_CYCLE_STATUS_CD", prefix = config(CLIENT_DS_ID) + "."),
    "LOCALDIAGNOSISSTATUS" -> mapFrom("CONFIRMATION_STATUS_CD", prefix = config(CLIENT_DS_ID) + "."),
    "RESOLUTIONDATE" -> ((col: String, df: DataFrame) => {
      val list_life_cycle_status_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "RESOLUTIONDATE", "DIAGNOSIS", "PROBLEM", "LIFE_CYCLE_STATUS_CD")
      df.withColumn(col, when(concat_ws(".", lit(config(CLIENT_DS_ID)), df("LIFE_CYCLE_STATUS_CD")).isin(list_life_cycle_status_cd: _*), null)
                        .otherwise(coalesce(df("LIFE_CYCLE_DT_TM"), df("UPDT_DT_TM"))))
    })
  )


  afterMap = (df: DataFrame) => {
    val list_pb_confirmation_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROBLEM", "DIAGNOSIS", "PROBLEM", "CONFIRMATION_STATUS_CD")
    val list_pb_cancel_reason_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROBLEM", "DIAGNOSIS", "PROBLEM", "CANCEL_REASON_CD")
    val list_pb_active_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROBLEM", "DIAGNOSIS", "PROBLEM", "ACTIVE_STATUS_CD")
    val list_life_cycle_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "RESOLUTIONDATE", "DIAGNOSIS", "PROBLEM", "LIFE_CYCLE_STATUS_CD")

    val fil1 = df.filter("nvl(CONFIRMATION_STATUS_CD, 'X') in (" + list_pb_confirmation_status_cd + ") and  " +
      "(CANCEL_REASON_CD is null or CANCEL_REASON_CD not in (" + list_pb_cancel_reason_cd + ")) and  " +
      " concat('" + config(CLIENT_DS_ID) + ".', ACTIVE_STATUS_CD) in (" + list_pb_active_status_cd +  ")")
                 .withColumn("LOCAL_DIAG",concat_ws(".", df("CODE_TYPE"), df("NOMENCLATURE_ID")))

    val fil2 = fil1
      .withColumn("MAPPEDDIAGNOSIS_SNO", when(fil1("CODE_TYPE") === lit("SNOMED"), fil1("CONCEPTCKI")).otherwise(fil1("SOURCE_IDENTIFIER")))
      .withColumn("MAPPEDDIAGNOSIS_ICD", when(fil1("CODE_TYPE") === lit("SNOMED"), fil1("ICD_DX")))
    val groups1 = Window.partitionBy(fil2("PATIENTID"),fil2("LOCAL_DIAG"),substring(fil2("ONSET_DATE"),1,10), fil2("MAPPEDDIAGNOSIS_SNO"), fil2("MAPPEDDIAGNOSIS_ICD")) 
      .orderBy(fil2("UPDT_DT_TM").desc_nulls_last, fil2("BEG_EFFECTIVE_DT_TM").desc_nulls_last)
    val groups2 = Window.partitionBy(fil2("PATIENTID"),fil2("LOCAL_DIAG"),substring(fil2("BEG_EFFECTIVE_DT_TM"),1,10), fil2("MAPPEDDIAGNOSIS_SNO"), fil2("MAPPEDDIAGNOSIS_ICD"))
      .orderBy(fil2("UPDT_DT_TM").desc_nulls_last)
    val addColumn1 = fil2.withColumn("rn_onsetdt", row_number.over(groups1))
                         .withColumn("rn_begdt", row_number.over(groups2))


    val tbl1 = addColumn1.withColumn("DX_TIMESTAMP", addColumn1("ONSET_DATE"))
                         .withColumn("LOCALDIAGNOSIS", addColumn1("LOCAL_DIAG"))
                         .withColumn("CODETYPE", addColumn1("CODE_TYPE"))
                         .withColumn("MAPPEDDIAGNOSIS", addColumn1("MAPPEDDIAGNOSIS_SNO"))
                         .filter("rn_onsetdt = 1 and ONSET_DATE is not null")


    val tbl2 = addColumn1.withColumn("DX_TIMESTAMP", addColumn1("ONSET_DATE"))
                         .withColumn("LOCALDIAGNOSIS", when(addColumn1("CODE_TYPE") === lit("SNOMED"), concat_ws("", lit("SNOMED."), addColumn1("NOMENCLATURE_ID"))))
                         .withColumn("CODETYPE", addColumn1("SNO_CODETYPE"))
                         .withColumn("MAPPEDDIAGNOSIS", addColumn1("MAPPEDDIAGNOSIS_ICD"))                         
                         .filter("rn_onsetdt = 1 and ONSET_DATE is not null and MAPPEDDIAGNOSIS is not null")


    val tbl3 = addColumn1.withColumn("DX_TIMESTAMP", addColumn1("BEG_EFFECTIVE_DT_TM"))
                         .withColumn("LOCALDIAGNOSIS", addColumn1("LOCAL_DIAG"))
                         .withColumn("CODETYPE", addColumn1("CODE_TYPE"))
                         .withColumn("MAPPEDDIAGNOSIS", addColumn1("MAPPEDDIAGNOSIS_SNO"))                         
                         .filter("rn_begdt = 1 and BEG_EFFECTIVE_DT_TM is not null " +
                                "and not (rn_onsetdt = 1 and ONSET_DATE is not null and BEG_EFFECTIVE_DT_TM = ONSET_DATE) " +
                                "and LOCALACTIVEIND in (" + list_life_cycle_status_cd + ")")

    val tbl4 = addColumn1.withColumn("DX_TIMESTAMP", addColumn1("BEG_EFFECTIVE_DT_TM"))
                         .withColumn("LOCALDIAGNOSIS", when(addColumn1("CODE_TYPE") === lit("SNOMED"), concat_ws("", lit("SNOMED."), addColumn1("NOMENCLATURE_ID"))))
                         .withColumn("CODETYPE", addColumn1("SNO_CODETYPE"))
                         .withColumn("MAPPEDDIAGNOSIS", addColumn1("MAPPEDDIAGNOSIS_ICD"))                         
                         .filter("rn_begdt = 1 and BEG_EFFECTIVE_DT_TM is not null and MAPPEDDIAGNOSIS is not null " +
                            "and not (rn_onsetdt = 1 and ONSET_DATE is not null and BEG_EFFECTIVE_DT_TM = ONSET_DATE) " +
                            "and LOCALACTIVEIND in (" + list_life_cycle_status_cd + ")")

    val un = tbl1.union(tbl2).union(tbl3).union(tbl4) 
    val cols = Engine.schema.getStringList("Diagnosis").asScala.map(_.split("-")(0).toUpperCase())
    un.select(cols.map(col): _*).distinct()

  }

  mapExceptions = Map(
    ("H984442_CR2", "LOCALDIAGNOSIS") -> cascadeFrom(Seq("NOMENCLATURE_ID", "CONCEPT_CKI"))
  )

}