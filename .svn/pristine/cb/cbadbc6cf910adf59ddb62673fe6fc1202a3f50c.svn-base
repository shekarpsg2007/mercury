package com.humedica.mercury.etl.athena.diagnosis

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class DiagnosisClinicalencounterdiagnosis(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "clinicalencounterdiagnosis",
    "clinicalencounterdxicd9",
    "clinicalencounterdxicd10",
    "icdcodeall",
    "dedupCE:athena.util.UtilDedupedClinicalEncounter",
    //"splitTable:athena.util.UtilSplitTable",
    "fileIdDates:athena.util.UtilFileIdDates"
  )

  columnSelect = Map(
    "clinicalencounterdiagnosis" -> List("FILEID","CLINICAL_ENCOUNTER_DX","DELETED_DATETIME", "CLINICAL_ENCOUNTER_ID", "CREATED_DATETIME", "CLINICAL_ENCOUNTER_DX_ID", "ICD_CODE_ID",
      "STATUS", "DIAGNOSIS_CODE", "SNOMED_CODE"),
    "clinicalencounterdxicd9" -> List("FILEID", "CLINICAL_ENCOUNTER_DXICD9_ID","DELETED_DATETIME", "CLINICAL_ENCOUNTER_DX_ID", "DIAGNOSIS_CODE"),
    "clinicalencounterdxicd10" -> List("FILEID", "ICD_CODE_ID","DIAGNOSIS_CODE_SET", "CLINICAL_ENCOUNTER_DX_ID", "DIAGNOSIS_CODE"),
    "icdcodeall" -> List("FILEID", "ICD_CODE_ID", "EFFECTIVE_DATE", "EXPIRATION_DATE", "DIAGNOSIS_CODE_SET", "UNSTRIPPED_DIAGNOSIS_CODE")
  )

  beforeJoin = Map(
    "clinicalencounterdiagnosis" -> ((df: DataFrame) => {
      val fileIdDates = table("fileIdDates")
      val fil = df.filter("DELETED_DATETIME is null")
      val joined = fil.join(fileIdDates, Seq("FILEID"), "left_outer")
      val groups = Window.partitionBy(joined("CLINICAL_ENCOUNTER_DX")).orderBy(joined("FILEDATE").desc_nulls_last, joined("FILEID").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    }),
    "clinicalencounterdxicd9" -> ((df: DataFrame) => {
      val fileIdDates = table("fileIdDates")
      val fil = df.filter("DELETED_DATETIME is null")
        .withColumnRenamed("DIAGNOSIS_CODE","DIAGNOSIS_CODE_icd9")
      val joined = fil.join(fileIdDates, Seq("FILEID"), "left_outer")
      val groups = Window.partitionBy(joined("CLINICAL_ENCOUNTER_DXICD9_ID")).orderBy(joined("FILEDATE").desc_nulls_last, joined("FILEID").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    }),
    "clinicalencounterdxicd10" -> ((df: DataFrame) => {
      val fileIdDates = table("fileIdDates")
      val fil = df.filter("DELETED_DATETIME is null")
        .withColumnRenamed("DIAGNOSIS_CODE","DIAGNOSIS_CODE_icd10")      
      val joined = fil.join(fileIdDates, Seq("FILEID"), "left_outer")
      val groups = Window.partitionBy(joined("CLINICAL_ENCOUNTER_DXICD10_ID")).orderBy(joined("FILEDATE").desc_nulls_last, joined("FILEID").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    }),
    "icdcodeall" -> ((df: DataFrame) => {
      val fileIdDates = table("fileIdDates")
      val fil = df.filter("DIAGNOSIS_CODE_SET = 'ICD10'")
      val joined = fil.join(fileIdDates, Seq("FILEID"), "left_outer")
        .withColumnRenamed("ICD_CODE_ID", "ICD_CODE_ID_dict")
      val groups = Window.partitionBy(joined("ICD_CODE_ID")).orderBy(joined("FILEDATE").desc_nulls_last, joined("FILEID").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    })    
  )
  
  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinicalencounterdiagnosis")
      .join(dfs("dedupCE"), Seq("CLINICAL_ENCOUNTER_ID"), "left_outer")
      //.join(dfs("splitTable"), Seq(""), "inner") // leaving out for now, to be handled in temp table creation
      .join(dfs("clinicalencounterdxicd9"), Seq("CLINICAL_ENCOUNTER_DX_ID"), "left_outer")
      .join(dfs("clinicalencounterdxicd10"), Seq("CLINICAL_ENCOUNTER_DX_ID"), "left_outer")
      .join(dfs("icdcodeall"), dfs("icdcodeall")("ICD_CODE_ID_dict") === dfs("clinicalencounterdiagnosis")("ICD_CODE_ID")
        && from_unixtime(unix_timestamp(dfs("clinicalencounterdiagnosis")("CREATED_DATETIME"))).between( 
        from_unixtime(unix_timestamp(dfs("icdcodeall")("EFFECTIVE_DATE"))), from_unixtime(unix_timestamp(coalesce(dfs("icdecodeall")("EXPIRATION_DATE"),current_timestamp)))),
        "left_outer")
  }
  
  afterJoin = (df: DataFrame) => {
    val addColumns = df.withColumn("CEDX_SNOMED", df("SNOMED_CODE"))
      .withColumn("CEDX_CODE", df("DIAGNOSIS_CODE"))
      .withColumn("MAPPED_DX", when(coalesce(df("DIAGNOSIS_CODE"), df("DIAGNOSIS_CODE_icd9")).isNotNull && coalesce(df("DIAGNOSIS_CODE_icd9"), lit("X")) =!= lit("undefined"),
        coalesce(df("DIAGNOSIS_CODE"), df("DIAGNOSIS_CODE_icd9"))).otherwise(df("SNOMED_CODE")))
      .filter("ENCOUNTER_DATE is not null and PATIENT_ID is not null")
    val insert1 = addColumns.filter("CEDX_SNOMED is not null and upper(CEDX_SNOMED) <> 'UNDEFINED'")
        .withColumn("LOCALDIAGNOSIS", addColumns("CEDX_SNOMED"))
        .withColumn("MAPPEDDIAGNOSIS", addColumns("CEDX_SNOMED"))
        .withColumn("CODETYPE", lit("SNOMED"))
        .select("PATIENT_ID","CLINICAL_ENCOUNTER_ID","ENCOUNTER_DATE","DEPARTMENT_ID","LOCALDIAGNOSIS", "MAPPEDDIAGNOSIS", "CODETYPE")
    val insert2 = addColumns.filter("CEDX_CODE is not null and upper(CEDX_CODE) <> 'UNDEFINED'")
        .withColumn("LOCALDIAGNOSIS", addColumns("CEDX_CODE"))
        .withColumn("MAPPEDDIAGNOSIS", addColumns("CEDX_CODE"))
        .withColumn("CODETYPE", lit(null))
        .select("PATIENT_ID","CLINICAL_ENCOUNTER_ID","ENCOUNTER_DATE","DEPARTMENT_ID","LOCALDIAGNOSIS", "MAPPEDDIAGNOSIS", "CODETYPE")          
    val insert3 = addColumns.filter("DIAGNOSIS_CODE_icd9 is not null and upper(DIAGNOSIS_CODE_icd9) <> 'UNDEFINED'")
        .withColumn("LOCALDIAGNOSIS", addColumns("DIAGNOSIS_CODE_icd9"))
        .withColumn("MAPPEDDIAGNOSIS", addColumns("DIAGNOSIS_CODE_icd9"))
        .withColumn("CODETYPE", lit(null))
        .select("PATIENT_ID","CLINICAL_ENCOUNTER_ID","ENCOUNTER_DATE","DEPARTMENT_ID","LOCALDIAGNOSIS", "MAPPEDDIAGNOSIS", "CODETYPE")   
    val insert4 = addColumns.filter("ICD_CODE_ID_dict is not null and upper(ICD_CODE_ID_dict) <> 'UNDEFINED'")
        .withColumn("LOCALDIAGNOSIS", addColumns("ICD_CODE_ID_dict"))
        .withColumn("MAPPEDDIAGNOSIS", addColumns("UNSTRIPPED_DIAGNOSIS_CDOE"))        
        .withColumn("CODETYPE", addColumns("DIAGNOSIS_CODE_SET"))
        .select("PATIENT_ID","CLINICAL_ENCOUNTER_ID","ENCOUNTER_DATE","DEPARTMENT_ID","LOCALDIAGNOSIS", "MAPPEDDIAGNOSIS", "CODETYPE")          
    insert1.unionAll(insert2).unionAll(insert3).unionAll(insert4)    
  }
  
  map = Map(
    "DATASRC" -> literal("clinicalencounterdiagnosis"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "ENCOUNTERID" -> mapFrom("CLINICAL_ENCOUNTER_ID"),
    "DX_TIMESTAMP" -> mapFrom("ENCOUNTER_DATE"),
    "FACILITYID" -> mapFrom("DEPARTMENT_ID")
    //"LOCALDIAGNOSISSTATUS" -> mapFrom("STATUS"),
    //"MAPPEDDIAGNOSIS" -> ((col: String, df: DataFrame) => {
    //  df.withColumn(col, when(coalesce(df("DIAGNOSIS_CODE"), df("DIAGNOSIS_CODE_icd9")).isNotNull && coalesce(df("DIAGNOSIS_CODE_icd9"), lit("X")) =!= lit("undefined"),
    //    coalesce(df("DIAGNOSIS_CODE"), df("DIAGNOSIS_CODE_icd9"))).otherwise(df("SNOMED_CODE")))}),    
    )

}
