package com.humedica.mercury.etl.athena.diagnosis

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class DiagnosisClaimdiagnosis(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "claimdiagnosis",
    "icdcodeall",
    "fileIdDates:athena.util.UtilFileIdDates",
    //"splitTable:athena.util.UtilSplitTable",
    "dedupedClaim:athena.util.UtilDedupedClaim",
    "dedupedTranscl:athena.util.UtilDedupedTransactionClaim",
    "dedupedCE:athena.util.UtilDedupedClinicalEncounter"
  )

  columnSelect = Map(
    "claimdiagnosis" -> List("FILEID","CLAIM_DIAGNOSIS_ID", "DELETED_DATETIME", "CLAIM_ID", "DIAGNOSIS_CODE", "DIAGNOSIS_CODESET_NAME", "ICD_CODE_ID"),
    "icdcodeall" -> List("FILEID", "ICD_CODE_ID", "DIAGNOSIS_CODE_SET", "EFFECTIVE_DATE", "EXPIRATION_DATE", "UNSTRIPPED_DIAGNOSIS_CODE")
  )

  beforeJoin = Map(
    "claimdiagnosis" -> ((df: DataFrame) => {
      val fileIdDates = table("fileIdDates")
      val fil = df.filter("DELETED_DATETIME is null")
      val joined = fil.join(fileIdDates, Seq("FILEID"), "left_outer")
      val groups = Window.partitionBy(joined("CLAIM_DIAGNOSIS_ID")).orderBy(joined("FILEDATE").desc_nulls_last, joined("FILEID").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    }),
    "icdcodeall" -> ((df: DataFrame) => {
      val fileIdDates = table("fileIdDates")  
      val fil = df.filter("DIAGNOSIS_CODE_SET = 'ICD_10'")
        .withColumnRenamed("ICD_CODE_ID", "ICD_CODE_ID_dict")
      val joined = fil.join(fileIdDates, Seq("FILEID"), "left_outer")
      val groups = Window.partitionBy(joined("ICD_CODE_ID")).orderBy(joined("FILEDATE").desc_nulls_last, joined("FILEID").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    }),
    "dedupedClaim" -> ((df: DataFrame) => {
      df.withColumnRenamed("CLAIM_APPOINTMENT_ID", "CLAIM_APPOINTMENT_ID_clm").withColumnRenamed("CLAIM_SERVICE_DATE","CLAIM_SERVICE_DATE_clm")
        .withColumnRenamed("RENDERING_PROVIDER_ID","RENDERING_PROVIDER_ID_clm")
    })
  )
  
  join = (dfs: Map[String, DataFrame]) => {
    val dedupedCE1 = table("dedupedCE").withColumnRenamed("CLINICAL_ENCOUNTER_ID", "CLINICAL_ENCOUNTER_ID1") 
    val dedupedCE2 = table("dedupedCE").withColumnRenamed("CLINICAL_ENCOUNTER_ID", "CLINICAL_ENCOUNTER_ID2")  
    dfs("claimdiagnosis")
      .join(dfs("dedupedClaim"), Seq("CLAIM_ID"), "left_outer")
      .join(dfs("dedupedTranscl"), Seq("CLAIM_ID"), "left_outer")
      //.join(dfs("splitTable"), Seq(""), "inner") // leaving out for now, to be handled in temp table creation
      .join(dedupedCE1, Seq("CLAIM_ID"), "left_outer")
      .join(dedupedCE2, dedupedCE2("APPOINTMENT_ID") === coalesce(dfs("dedupedClaim")("CLAIM_APPOINTMENT_ID_clm"),dfs("dedupedTranscl")("CLAIM_APPOINTMENT_ID")), "left_outer")
      .join(dfs("icdcodeall"), dfs("icdcodeall")("ICD_CODE_ID_dict") === dfs("claimdiagnosis")("ICD_CODE_ID")
        && from_unixtime(unix_timestamp(coalesce(dfs("claimdiagnosis")("CLAIM_SERVICE_DATE"), dfs("dedupedClaim")("CLAIM_SERVICE_DATE_clm")))).between( 
        from_unixtime(unix_timestamp(dfs("icdcodeall")("EFFECTIVE_DATE"))), from_unixtime(unix_timestamp(coalesce(dfs("icdecodeall")("EXPIRATION_DATE"),current_timestamp)))),
        "left_outer")
  }
  
  afterJoin = (df: DataFrame) => {
    df.withColumn("ENC_ID_clm", concat(lit("a."), df("CLAIM_APPOINTMENT_ID_clm")))
  }
  
  map = Map(
    "DATASRC" -> literal("claimdiagnosis"),
    "PATIENTID" -> cascadeFrom(Seq("PATIENT_ID","CLAIM_PATIENT_ID")),
    //"ENCOUNTERID" -> cascadeFrom(Seq("CLINICAL_ENCOUNTER_ID1","CLINICAL_ENCOUNTER_ID2", concat(lit("a."),"CLAIM_APPOINTMENT_ID_clm"))),
    "ENCOUNTERID" -> cascadeFrom(Seq("CLINICAL_ENCOUNTER_ID1","CLINICAL_ENCOUNTER_ID2", "ENC_ID_clm")),
    "DX_TIMESTAMP" -> cascadeFrom(Seq("CLAIM_SERVICE_DATE_clm", "CLAIM_SERVICE_DATE")),
    "LOCALDIAGNOSIS" -> cascadeFrom(Seq("ICD_CODE_ID_dict", "DIAGNOSIS_CODE")),
    "LOCALDIAGNOSISPROVIDERID" -> cascadeFrom(Seq("RENDERING_PROVIDER_ID_clm", "RENDERING_PROVIDER_ID")),
    "SOURCEID" -> cascadeFrom(Seq("CLAIM_ID","TRANSACTION_ID")),
    "MAPPEDDIAGNOSIS" -> cascadeFrom(Seq("UNSTRIPPED_DIAGNOSIS_CODE","DIAGNOSIS_CODE")),
    "CODETYPE" -> cascadeFrom(Seq("DIAGNOSIS_CODE_SET","DIAGNOSIS_CODESET_NAME"))
    )

  afterMap = (df: DataFrame) => {
    df.filter("DX_TIMESTAMP is not null and LOCALDIAGNOSIS is not null and MAPPEDDIAGNOSIS is not null and PATIENTID is not null")
  }
}