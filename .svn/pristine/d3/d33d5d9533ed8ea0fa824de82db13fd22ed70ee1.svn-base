package com.humedica.mercury.etl.crossix.medicalclaim

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class MedicalclaimMedicalclaimv2(config: Map[String,String], count:Int) extends EntitySource(config: Map[String,String]) {

  tables = List("medicalclaim.medical_claim"+count, "diagnosis", "place_of_service", "drg", "facility_claim_std_cost", "physician_claim_std_cost",
    "ndc", "procedure_code", "procedure_modifier", "provider", "revenue")

  columns = List("ADJUSTED_HCCC_CODE", "ADJUSTED_CLAIM_IND", "AMT_COB_SAVINGS", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_STANDARD_COST",
    "COB_CODE", "D_DISCHARGE_STATUS_CODE", "D_PLACE_OF_SERVICE_CODE", "DIAGNOSIS_1_CODE", "DIAGNOSIS_2_CODE", "DIAGNOSIS_3_CODE",
    "DIAGNOSIS_4_CODE", "DIAGNOSIS_5_CODE", "DIAGNOSIS_6_CODE", "DIAGNOSIS_7_CODE", "DIAGNOSIS_8_CODE", "DIAGNOSIS_9_CODE",
    "DRG_CODE", "ENCOUNTER_CODE", "ICD_VERSION_CODE", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID", "LOCATION_OF_SERVICE_CODE",
    "LSRD_CLAIM_NBR", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID", "NDC_CODE", "PRESENT_ON_ADMISSION_1_CODE", "PRESENT_ON_ADMISSION_2_CODE",
    "PRESENT_ON_ADMISSION_3_CODE", "PRESENT_ON_ADMISSION_4_CODE", "PRESENT_ON_ADMISSION_5_CODE", "PRESENT_ON_ADMISSION_6_CODE",
    "PRESENT_ON_ADMISSION_7_CODE", "PRESENT_ON_ADMISSION_8_CODE", "PRESENT_ON_ADMISSION_9_CODE", "PROCEDURE_1_CODE",
    "PROCEDURE_2_CODE", "PROCEDURE_3_CODE", "PROCEDURE_4_CODE", "PROCEDURE_5_CODE", "PROCEDURE_6_CODE", "PROCEDURE_CODE",
    "PROCEDURE_MODIFIER_CODE", "PROVIDER_CATEGORY_CODE", "PROVIDER_KEY", "PROVIDER_NPI", "QUANTITY_UNITS", "REVENUE_CODE",
    "SERVICE_FROM_DATE", "SERVICE_LINE_NBR", "SERVICE_POST_DATE", "SERVICE_THRU_DATE", "STANDARD_COST_YEAR")

  columnSelect = Map(
    "medicalclaim.medical_claim"+count -> List("ADJUSTED_HCCC_CODE", "ADJUSTED_CLAIM_IND", "AMT_COB_SAVINGS", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE",
      "COB_CODE","DISCHARGE_STATUS_CODE", "ENCOUNTER_CODE", "CLM_LOC_CD", "LSRD_CLAIM_NBR", "LSRD_INDIVIDUAL_ID",
      "LSRD_MEMBER_SYSTEM_ID","PRESENT_ON_ADMISSION_1_CODE", "PRESENT_ON_ADMISSION_2_CODE", "PRESENT_ON_ADMISSION_3_CODE",
      "PRESENT_ON_ADMISSION_4_CODE", "PRESENT_ON_ADMISSION_5_CODE", "PRESENT_ON_ADMISSION_6_CODE","PRESENT_ON_ADMISSION_7_CODE",
      "PRESENT_ON_ADMISSION_8_CODE", "PRESENT_ON_ADMISSION_9_CODE", "PROVIDER_KEY", "QUANTITY_UNITS", "SERVICE_FROM_DATE",
      "SERVICE_LINE_NBR", "SERVICE_POST_DATE", "SERVICE_THRU_DATE","NDC_KEY", "PLACE_OF_SERVICE_KEY", "HEADER_PROCEDURE_1_KEY",
      "HEADER_PROCEDURE_2_KEY", "HEADER_PROCEDURE_3_KEY", "HEADER_PROCEDURE_4_KEY", "HEADER_PROCEDURE_5_KEY", "HEADER_PROCEDURE_6_KEY",
      "SERVICE_PROCEDURE_KEY", "PROCEDURE_MODIFIER_1_KEY", "REVENUE_KEY", "HEADER_DIAGNOSIS_1_KEY", "HEADER_DIAGNOSIS_2_KEY",
      "HEADER_DIAGNOSIS_3_KEY", "HEADER_DIAGNOSIS_4_KEY", "HEADER_DIAGNOSIS_5_KEY", "HEADER_DIAGNOSIS_6_KEY", "HEADER_DIAGNOSIS_7_KEY",
      "HEADER_DIAGNOSIS_8_KEY", "HEADER_DIAGNOSIS_9_KEY", "DRG_KEY", "COSMOS_SITE_CODE"),
    "diagnosis" -> List("DIAGNOSIS_KEY", "CODE", "ICD_VER_CD"),
    "drg" -> List("DRG_KEY", "CODE"),
    "facility_claim_std_cost" -> List("AMT_STANDARD_COST", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID",
      "SERVICE_FROM_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "SERVICE_LINE_NBR", "LSRD_MEMBER_SYSTEM_ID", "STANDARD_COST_YEAR"),
    "physician_claim_std_cost" -> List("AMT_STANDARD_COST", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID",
      "SERVICE_FROM_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "SERVICE_LINE_NBR", "LSRD_MEMBER_SYSTEM_ID", "STANDARD_COST_YEAR"),
    "ndc" -> List("NDC_KEY", "CODE"),
    "place_of_service" -> List("PLACE_OF_SERVICE_KEY", "AMA_CODE"),
    "procedure_code" -> List("PROCEDURE_KEY", "CODE"),
    "procedure_modifier" -> List("PROCEDURE", "CODE"),
    "provider" -> List("PROVIDER_KEY", "PROVIDER_CATEGORY_CODE", "NPI_NBR"),
    "revenue" -> List("REVENUE_KEY", "CODE")
  )


  beforeJoin = Map(
    "facility_claim_std_cost" -> renameColumns(List(("AMT_STANDARD_COST", "FACILITY_AMT_STANDARD_COST"), ("IHCIS_TYPE_OF_SERVICE_KEY", "FACILITY_IHCIS_TYPE_OF_SERVICE_KEY"),("INPATIENT_CONF_ID", "FACILITY_INPATIENT_CONF_ID"),("STANDARD_COST_YEAR", "FACILITY_STANDARD_COST_YEAR"))),
    "physician_claim_std_cost" -> renameColumns(List(("AMT_STANDARD_COST", "PHYSICIAN_AMT_STANDARD_COST"), ("IHCIS_TYPE_OF_SERVICE_KEY", "PHYSICIAN_IHCIS_TYPE_OF_SERVICE_KEY"),("INPATIENT_CONF_ID", "PHYSICIAN_INPATIENT_CONF_ID"),("STANDARD_COST_YEAR", "PHYSICIAN_STANDARD_COST_YEAR"))),
    "ndc" -> renameColumns(List(("CODE", "NDC_CODE"))),
    "revenue" -> renameColumns(List(("CODE", "REVENUE_CODE"))),
    "drg" -> renameColumns(List(("CODE", "DRG_CODE"))),
    "procedure_modifier" -> renameColumns(List(("CODE", "PROCEDURE_MODIFIER_CODE")))
  )

  join = (dfs: Map[String, DataFrame]) => {

    println("FILE ===> "+"medicalclaim.medical_claim"+count)

    var diagnosis = dfs("diagnosis")
    var drg = dfs("drg")
    var facility_claim_std_cost = dfs("facility_claim_std_cost")
    var physician_claim_std_cost = dfs("physician_claim_std_cost")
    var ndc = dfs("ndc")
    var place_of_service = dfs("place_of_service")
    var procedure_code = dfs("procedure_code")
    var procedure_modifier = dfs("procedure_modifier")
    var provider = dfs("provider")
    var revenue = dfs("revenue")
    var mc = dfs("medicalclaim.medical_claim"+count)
    mc
            .join(diagnosis.as("diagnosis1").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_1_KEY").withColumnRenamed("CODE","DIAGNOSIS_1_CODE"), Seq("HEADER_DIAGNOSIS_1_KEY"), "left_outer")
            .join(diagnosis.as("diagnosis2").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_2_KEY").withColumnRenamed("CODE","DIAGNOSIS_2_CODE").drop("ICD_VER_CD"), Seq("HEADER_DIAGNOSIS_2_KEY"), "left_outer")
            .join(diagnosis.as("diagnosis3").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_3_KEY").withColumnRenamed("CODE","DIAGNOSIS_3_CODE").drop("ICD_VER_CD"), Seq("HEADER_DIAGNOSIS_3_KEY"), "left_outer")
            .join(diagnosis.as("diagnosis4").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_4_KEY").withColumnRenamed("CODE","DIAGNOSIS_4_CODE").drop("ICD_VER_CD"), Seq("HEADER_DIAGNOSIS_4_KEY"), "left_outer")
            .join(diagnosis.as("diagnosis5").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_5_KEY").withColumnRenamed("CODE","DIAGNOSIS_5_CODE").drop("ICD_VER_CD"), Seq("HEADER_DIAGNOSIS_5_KEY"), "left_outer")
            .join(diagnosis.as("diagnosis6").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_6_KEY").withColumnRenamed("CODE","DIAGNOSIS_6_CODE").drop("ICD_VER_CD"), Seq("HEADER_DIAGNOSIS_6_KEY"), "left_outer")
            .join(diagnosis.as("diagnosis7").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_7_KEY").withColumnRenamed("CODE","DIAGNOSIS_7_CODE").drop("ICD_VER_CD"), Seq("HEADER_DIAGNOSIS_7_KEY"), "left_outer")
            .join(diagnosis.as("diagnosis8").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_8_KEY").withColumnRenamed("CODE","DIAGNOSIS_8_CODE").drop("ICD_VER_CD"), Seq("HEADER_DIAGNOSIS_8_KEY"), "left_outer")
            .join(diagnosis.as("diagnosis9").withColumnRenamed("DIAGNOSIS_KEY","HEADER_DIAGNOSIS_9_KEY").withColumnRenamed("CODE","DIAGNOSIS_9_CODE").drop("ICD_VER_CD"), Seq("HEADER_DIAGNOSIS_9_KEY"), "left_outer")
            .join(drg, Seq("DRG_KEY"), "left_outer")
            .join(facility_claim_std_cost, Seq("SERVICE_FROM_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "SERVICE_LINE_NBR", "LSRD_MEMBER_SYSTEM_ID"), "left_outer")
            .join(physician_claim_std_cost, Seq("SERVICE_FROM_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "SERVICE_LINE_NBR", "LSRD_MEMBER_SYSTEM_ID"), "left_outer")
            .join(ndc, Seq("NDC_KEY"), "left_outer")
            .join(place_of_service, Seq("PLACE_OF_SERVICE_KEY"), "left_outer")
            .join(procedure_code.as("procedure_code1").withColumnRenamed("PROCEDURE_KEY","HEADER_PROCEDURE_1_KEY").withColumnRenamed("CODE","PROCEDURE_1_CODE"), Seq("HEADER_PROCEDURE_1_KEY"), "left_outer")
            .join(procedure_code.as("procedure_code2").withColumnRenamed("PROCEDURE_KEY","HEADER_PROCEDURE_2_KEY").withColumnRenamed("CODE","PROCEDURE_2_CODE"), Seq("HEADER_PROCEDURE_2_KEY"), "left_outer")
            .join(procedure_code.as("procedure_code3").withColumnRenamed("PROCEDURE_KEY","HEADER_PROCEDURE_3_KEY").withColumnRenamed("CODE","PROCEDURE_3_CODE"), Seq("HEADER_PROCEDURE_3_KEY"), "left_outer")
            .join(procedure_code.as("procedure_code4").withColumnRenamed("PROCEDURE_KEY","HEADER_PROCEDURE_4_KEY").withColumnRenamed("CODE","PROCEDURE_4_CODE"), Seq("HEADER_PROCEDURE_4_KEY"), "left_outer")
            .join(procedure_code.as("procedure_code5").withColumnRenamed("PROCEDURE_KEY","HEADER_PROCEDURE_5_KEY").withColumnRenamed("CODE","PROCEDURE_5_CODE"), Seq("HEADER_PROCEDURE_5_KEY"), "left_outer")
            .join(procedure_code.as("procedure_code6").withColumnRenamed("PROCEDURE_KEY","HEADER_PROCEDURE_6_KEY").withColumnRenamed("CODE","PROCEDURE_6_CODE"), Seq("HEADER_PROCEDURE_6_KEY"), "left_outer")
            .join(procedure_code.as("procedure_code7").withColumnRenamed("PROCEDURE_KEY","SERVICE_PROCEDURE_KEY").withColumnRenamed("CODE","PROCEDURE_CODE"), Seq("SERVICE_PROCEDURE_KEY"), "left_outer")
            .join(procedure_modifier, mc("PROCEDURE_MODIFIER_1_KEY") === procedure_modifier("PROCEDURE"), "left_outer")
            .join(provider, Seq("PROVIDER_KEY"), "left_outer")
            .join(revenue, Seq("REVENUE_KEY"), "left_outer")
  }

  def parseCrossixDate(col: String, df: DataFrame) = {
    from_unixtime(unix_timestamp(trim(df(col)), "yyyy-MM-dd"), "yyyy/MM/dd")
  }

  map = Map(
    "LSRD_CLAIM_NBR" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("LSRD_CLAIM_NBR"), 256))),
    "LSRD_MEMBER_SYSTEM_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("LSRD_MEMBER_SYSTEM_ID"), 256))),
    "AMT_STANDARD_COST" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "1", df("FACILITY_AMT_STANDARD_COST")).otherwise( when(df("CLM_LOC_CD") === "2", df("PHYSICIAN_AMT_STANDARD_COST"))))}),

    "D_DISCHARGE_STATUS_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(trim(df("DISCHARGE_STATUS_CODE")) isin ("20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "40", "41", "42"), "00")
            .when(length(trim(df("DISCHARGE_STATUS_CODE"))) === 0, null).otherwise(trim(df("DISCHARGE_STATUS_CODE"))))}),

    "D_PLACE_OF_SERVICE_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("AMA_CODE") isin ("05", "06", "07", "08", "09", "26"), "99").otherwise(trim(df("AMA_CODE"))))}),

    "IHCIS_TYPE_OF_SERVICE_KEY" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "1", df("FACILITY_IHCIS_TYPE_OF_SERVICE_KEY")).otherwise( when(df("CLM_LOC_CD") === "2", df("PHYSICIAN_IHCIS_TYPE_OF_SERVICE_KEY"))))}),
    "INPATIENT_CONF_ID" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "1", df("FACILITY_INPATIENT_CONF_ID")).otherwise(when(df("CLM_LOC_CD") === "2", df("PHYSICIAN_INPATIENT_CONF_ID"))))}),
    "NDC_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, substring(df("NDC_CODE"), 1, 11))}),
    "PROCEDURE_1_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "2", "0000000").otherwise(df("PROCEDURE_1_CODE")))}),
    "PROCEDURE_2_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "2", "0000000").otherwise(df("PROCEDURE_2_CODE")))}),
    "PROCEDURE_3_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "2", "0000000").otherwise(df("PROCEDURE_3_CODE")))}),
    "PROCEDURE_4_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "2", "0000000").otherwise(df("PROCEDURE_4_CODE")))}),
    "PROCEDURE_5_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "2", "0000000").otherwise(df("PROCEDURE_5_CODE")))}),
    "PROCEDURE_6_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "2", "0000000").otherwise(df("PROCEDURE_6_CODE")))}),
    "PROVIDER_CATEGORY_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("PROVIDER_CATEGORY_CODE") isin ("3946","1598"), "1099").otherwise(df("PROVIDER_CATEGORY_CODE")))}),
    "PROVIDER_NPI" -> mapFrom("NPI_NBR"),
    "ICD_VERSION_CODE" -> mapFrom("ICD_VER_CD"),
    "LOCATION_OF_SERVICE_CODE" -> mapFrom("CLM_LOC_CD"),
    "STANDARD_COST_YEAR" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("CLM_LOC_CD") === "1", df("FACILITY_STANDARD_COST_YEAR")).otherwise(when(df("CLM_LOC_CD") === "2", df("PHYSICIAN_STANDARD_COST_YEAR"))))}),
    "SERVICE_FROM_DATE" -> ((col: String, df: DataFrame) => {df.withColumn(col, parseCrossixDate("SERVICE_FROM_DATE", df))}),
    "SERVICE_POST_DATE" -> ((col: String, df: DataFrame) => {df.withColumn(col, parseCrossixDate("SERVICE_POST_DATE", df))}),
    "SERVICE_THRU_DATE" -> ((col: String, df: DataFrame) => {df.withColumn(col, parseCrossixDate("SERVICE_THRU_DATE", df))}))
}

/*

 build(new MedicalclaimMedicalclaim(cfg, 9), allColumns=true).distinct.write.parquet(cfg.get("MEDICALCLAIM_DATA_ROOT").get+"/MEDICAL_CLAIM_201802_P9")


 sqlContext.read.parquet(cfg.get("MEDICALCLAIM_DATA_ROOT").get+"/MEDICAL_CLAIM_201802_P9").write.mode("append").parquet(cfg.get("EMR_DATA_ROOT").get+"/MEDICAL_CLAIM_201802")




*/