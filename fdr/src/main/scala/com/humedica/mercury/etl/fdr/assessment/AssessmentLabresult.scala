package com.humedica.mercury.etl.fdr.assessment

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class AssessmentLabresult (config: Map[String, String]) extends EntitySource(config: Map[String, String]){


  tables=List("Backend:fdr.assessment.AssessmentLabbackend","cdr.map_smoking_status", "cdr.map_smoking_cessation", "cdr.map_tobacco_use", "cdr.map_tobacco_cessation", "cdr.map_bmi_followup", "cdr.map_bp_followup", "cdr.map_depression_followup",
    "cdr.map_aco_exclusion", "cdr.map_alcohol", "cdr.map_exercise_level", "cdr.map_cad_plan_of_care", "cdr.map_clin_pathways", "cdr.map_diab_eye_exam", "cdr.map_hospice_ind",
    "mpitemp:fdr.mpi.MpiPatient")


  columns=List("CLIENT_DS_ID", "MPI", "SOURCE", "ASSESSMENT_TYPE", "ASSESSMENT_DTM", "TEXT_VALUE", "NUMERIC_VALUE", "QUALITATIVE_VALUE")

  beforeJoin = Map(
    "cdr.map_smoking_status" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").withColumnRenamed("MNEMONIC", "MNEMONIC_smoking")
        .withColumnRenamed("CUI", "CUI_smoking")
    }),
    "cdr.map_smoking_cessation" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'")
        .withColumnRenamed("CUI", "CUI_cessation").withColumnRenamed("LOCAL_CODE", "LOCAL_CODE_cessation")
    }),
    "cdr.map_tobacco_use" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'")
        .withColumnRenamed("CUI", "CUI_tobacco").withColumnRenamed("LOCALCODE", "LOCALCODE_tobacco")
    }),
    "cdr.map_tobacco_cessation" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_tob_cessation")
        .withColumnRenamed("CUI", "CUI_tob_cessation")
    }),
    "cdr.map_bmi_followup" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_bmi")
        .withColumnRenamed("CUI", "CUI_bmi")
    }),
    "cdr.map_bp_followup" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_bp")
        .withColumnRenamed("CUI", "CUI_bp")
    }),
    "cdr.map_depression_followup" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_depression")
        .withColumnRenamed("CUI", "CUI_depression")
    }),
    "cdr.map_aco_exclusion" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_aco")
        .withColumnRenamed("CUI", "CUI_aco")
    }),
    "cdr.map_alcohol" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("MNEMONIC", "MNEMONIC_alcohol")
        .withColumnRenamed("CUI", "CUI_alcohol")
    }),
    "cdr.map_exercise_level" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("MNEMONIC", "MNEMONIC_exercise")
        .withColumnRenamed("CUI", "CUI_exercise")
    }),
    "cdr.map_cad_plan_of_care" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_cad")
        .withColumnRenamed("CUI", "CUI_cad")
    }),
    "cdr.map_clin_pathways" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_clin")
        .withColumnRenamed("CUI", "CUI_clin")
    }),
    "cdr.map_diab_eye_exam" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCAL_CODE", "LOCAL_CODE_diab")
        .withColumnRenamed("HTS_CODE", "HTS_CODE_diab")
    }),
    "cdr.map_hospice_ind" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("MNEMONIC", "MNEMONIC_hospice")
        .withColumnRenamed("CUI", "CUI_hospice")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("Backend")
      .join(dfs("cdr.map_smoking_status"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_smoking_status")("MNEMONIC_smoking"), "left_outer")
      .join(dfs("cdr.map_smoking_cessation"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_smoking_cessation")("LOCAL_CODE_cessation"), "left_outer")
      .join(dfs("cdr.map_tobacco_use"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_tobacco_use")("LOCALCODE_tobacco"), "left_outer")
      .join(dfs("cdr.map_tobacco_cessation"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_tobacco_cessation")("LOCALCODE_tob_cessation"), "left_outer")
      .join(dfs("cdr.map_bmi_followup"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_bmi_followup")("LOCALCODE_bmi"), "left_outer")
      .join(dfs("cdr.map_bp_followup"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_bp_followup")("LOCALCODE_bp"), "left_outer")
      .join(dfs("cdr.map_depression_followup"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_depression_followup")("LOCALCODE_depression"), "left_outer")
      .join(dfs("cdr.map_aco_exclusion"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_aco_exclusion")("LOCALCODE_aco"), "left_outer")
      .join(dfs("cdr.map_alcohol"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_alcohol")("MNEMONIC_alcohol"), "left_outer")
      .join(dfs("cdr.map_exercise_level"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_exercise_level")("MNEMONIC_exercise"), "left_outer")
      .join(dfs("cdr.map_cad_plan_of_care"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_cad_plan_of_care")("LOCALCODE_cad"), "left_outer")
      .join(dfs("cdr.map_clin_pathways"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_clin_pathways")("LOCALCODE_clin"), "left_outer")
      .join(dfs("cdr.map_diab_eye_exam"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_diab_eye_exam")("LOCAL_CODE_diab"), "left_outer")
      .join(dfs("cdr.map_hospice_ind"), dfs("Backend")("LOCALRESULT") === dfs("cdr.map_hospice_ind")("MNEMONIC_hospice"), "left_outer")
      .join(dfs("mpitemp"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
  }



  map = Map(
    "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
    "MPI" -> mapFrom("MPI"),
    "SOURCE" -> mapFrom("DATASRC"),
    "ASSESSMENT_TYPE" -> ((col: String, df: DataFrame) => df.withColumn(col, when(isnull(df("MAPPEDCODE")), "CH999999").otherwise(df("MAPPEDCODE")))),
    "ASSESSMENT_DTM" -> cascadeFrom(Seq("DATECOLLECTED", "DATEAVAILABLE")),
    "TEXT_VALUE" -> ((col: String, df: DataFrame) => df.withColumn(col, substring(df("LOCALRESULT"),1,100))),
    "NUMERIC_VALUE" -> cascadeFrom(Seq("NORMAlIZEDVALUE", "LOCALRESULT_INFERRED", "LOCALRESULT_NUMERIC")),
    "QUALITATIVE_VALUE" -> ((col: String, df: DataFrame) => df.withColumn(col,
      when(df("MNEMONIC_smoking").isNotNull, df("CUI_smoking"))
        .when(df("LOCAL_CODE_cessation").isNotNull, df("CUI_cessation"))
        .when(df("LOCALCODE_tobacco").isNotNull, df("CUI_tobacco"))
        .when(df("LOCALCODE_tob_cessation").isNotNull, df("CUI_tob_cessation"))
        .when(df("LOCALCODE_bmi").isNotNull, df("CUI_bmi"))
        .when(df("LOCALCODE_bp").isNotNull, df("CUI_bp"))
        .when(df("LOCALCODE_depression").isNotNull, df("CUI_depression"))
        .when(df("LOCALCODE_aco").isNotNull, df("CUI_aco"))
        .when(df("MNEMONIC_alcohol").isNotNull, df("CUI_alcohol"))
        .when(df("MNEMONIC_exercise").isNotNull, df("CUI_exercise"))
        .when(df("LOCALCODE_cad").isNotNull, df("CUI_cad"))
        .when(df("LOCALCODE_clin").isNotNull, df("CUI_clin"))
        .when(df("LOCAL_CODE_diab").isNotNull, df("HTS_CODE_diab"))
        .when(df("MNEMONIC_hospice").isNotNull, df("CUI_hospice"))
        .otherwise(null)
    ))
  )


  afterMap = (df: DataFrame) => {
    df.filter("TEXT_VALUE is not null and length(TEXT_VALUE) <=100")
  }


}