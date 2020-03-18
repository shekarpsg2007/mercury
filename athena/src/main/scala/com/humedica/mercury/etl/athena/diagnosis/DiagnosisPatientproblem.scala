package com.humedica.mercury.etl.athena.diagnosis

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class DiagnosisPatientproblem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "patientproblem",
    "patient",
    "chart",
    //"splitTable:athena.util.UtilSplitTable",
    "fileIdDates:athena.util.UtilFileIdDates",
    "cdr.ref_snomed_icd9",
    "cdr.ref_snomed_icd10"
  )

  columnSelect = Map(
    "patientproblem" -> List("FILEID", "HUM_TYPE", "PATIENT_PROBLEM_ID", "DIAGNOSIS_CODE", "PATIENT_ID", "SNOMED_CODE", "ONSET_DATE", "CHART_ID", "DELETED_DATETIME"),
    "patient" -> List("FILEID", "PATIENT_ID", "ENTERPRISE_ID"),
    "chart" -> List("CHART_ID", "ENTERPRISE_ID", "CREATED_DATETIME")
  )

  beforeJoin = Map(
    "patientproblem" -> ((df: DataFrame) => {
      val fileIdDates = table("fileIdDates")
      val fil = df.filter("DELETED_DATETIME is null and HUM_TYPE <> 'PATIENTPROBLEMLIST_FREETEXT'")
      val joined = fil.join(fileIdDates, Seq("FILEID"), "left_outer")
      val groups = Window.partitionBy(joined("PATIENT_PROBLEM_ID")).orderBy(joined("FILEDATE").desc_nulls_last, joined("FILEID").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    }),
    "patient" -> ((df: DataFrame) => {
      val fileIdDates = table("fileIdDates")
      val joined = df.join(fileIdDates, Seq("FILEID"), "left_outer")
      val groups = Window.partitionBy(joined("ENTERPRISE_ID")).orderBy(joined("FILEDATE").desc_nulls_last, joined("FILEID").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
        .withColumnRenamed("PATIENT_ID", "PATIENT_ID_p")
    }),
    "cdr.ref_snomed_icd9" -> ((df: DataFrame) => {
      df.withColumnRenamed("SNOMED_ID", "SNOMED_ID9")
    }),
    "cdr.ref_snomed_icd10" -> ((df: DataFrame) => {
      df.withColumnRenamed("SNOMED_ID", "SNOMED_ID10")
    })    
  )
  
  join = (dfs: Map[String, DataFrame]) => {
    dfs("patientproblem")
      .join(dfs("chart"), Seq("CHART_ID"), "left_outer")
      .join(dfs("patient"), Seq("ENTERPRISE_ID"), "left_outer")
      //.join(dfs("splitTable"), Seq(""), "inner") // leaving out for now, to be handled in temp table creation
      .join(dfs("cdr.ref_snomed_icd9"), dfs("patientproblem")("SNOMED_CODE") === dfs("cdr.ref_snomed_icd9")("SNOMED_ID9") &&
        when(from_unixtime(unix_timestamp(coalesce(dfs("patientproblem")("ONSET_DATE"), lit("1900-01-01")))) =!= from_unixtime(unix_timestamp(lit("1900-01-01"))), 
          from_unixtime(unix_timestamp(dfs("patientproblem")("ONSET_DATE"))))
        .otherwise(from_unixtime(unix_timestamp(dfs("chart")("CREATED_DATETIME")))) < from_unixtime(unix_timestamp(lit("2015-10-01"))), "left_outer")
      .join(dfs("cdr.ref_snomed_icd10"), dfs("patientproblem")("SNOMED_CODE") === dfs("cdr.ref_snomed_icd10")("SNOMED_ID10") &&
        when(from_unixtime(unix_timestamp(coalesce(dfs("patientproblem")("ONSET_DATE"), lit("1900-01-01")))) =!= from_unixtime(unix_timestamp(lit("1900-01-01"))), 
          from_unixtime(unix_timestamp(dfs("patientproblem")("ONSET_DATE"))))
        .otherwise(from_unixtime(unix_timestamp(dfs("chart")("CREATED_DATETIME")))) >= from_unixtime(unix_timestamp(lit("2015-10-01"))), "left_outer")
  }
  
  afterJoin = (df: DataFrame) => {
    val addColumns = df.withColumn("DX_TIMESTAMP", 
      when(from_unixtime(unix_timestamp(coalesce(df("ONSET_DATE"), lit("1900-01-01")))) =!= from_unixtime(unix_timestamp(lit("1900-01-01"))), 
        from_unixtime(unix_timestamp(df("ONSET_DATE"))))
      .otherwise(from_unixtime(unix_timestamp(df("CREATED_DATETIME")))))
      .withColumn("PATIENTID", coalesce(df("PATIENT_ID"),df("PATIENT_ID_p")))
      .withColumn("CODE_TYPE_ICD", when(df("HUM_TYPE") === lit("ICD9PATIENTPROBLEM"), lit("ICD9")).when(df("HUM_TYPE") === lit("ICD10PATIENTPROBLEM"), lit("ICD10"))
        .otherwise(df("HUM_TYPE")))
      .withColumn("CODE_TYPE_SNOMED", when(df("SNOMED_ID9").isNotNull, lit("ICD9")).when(df("SNOMED_ID10").isNotNull, lit("ICD10")))
      .withColumn("MAPPEDDIAGNOSIS_sno", coalesce(df("ICD9CM_CODE"),df("ICD10_CODE")))
       
    val insert1 = addColumns.filter("DIAGNOSIS_CODE is not null")
        .withColumn("LOCALDIAGNOSIS", addColumns("DIAGNOSIS_CODE"))
        .withColumn("MAPPEDDIAGNOSIS", addColumns("DIAGNOSIS_CODE"))
        .withColumn("CODETYPE", addColumns("CODE_TYPE_ICD"))
        .select("PATIENTID","DX_TIMESTAMP","LOCALDIAGNOSIS","MAPPEDDIAGNOSIS","CODETYPE")
    val insert2 = addColumns.filter("SNOMED_CODE is not null")
        .withColumn("LOCALDIAGNOSIS", addColumns("SNOMED_CODE"))
        .withColumn("MAPPEDDIAGNOSIS", addColumns("SNOMED_CODE"))
        .withColumn("CODETYPE", addColumns("CODE_TYPE_ICD"))
        .select("PATIENTID","DX_TIMESTAMP","LOCALDIAGNOSIS","MAPPEDDIAGNOSIS","CODETYPE")        
    val insert3 = addColumns.filter("SNOMED_CODE is not null and MAPPEDDIAGNOSIS_sno is not null")
        .withColumn("LOCALDIAGNOSIS", addColumns("SNOMED_CODE"))
        .withColumn("MAPPEDDIAGNOSIS", addColumns("MAPPEDDIAGNOSIS_sno"))
        .withColumn("CODETYPE", addColumns("CODE_TYPE_SNOMED"))
        .select("PATIENTID","DX_TIMESTAMP","LOCALDIAGNOSIS","MAPPEDDIAGNOSIS","CODETYPE")          
   
    insert1.unionAll(insert2).unionAll(insert3).filter("DX_TIMESTAMP is not null and PATIENTID is not null")    
  }
  
  map = Map(
    "DATASRC" -> literal("patientproblem")
  )
}