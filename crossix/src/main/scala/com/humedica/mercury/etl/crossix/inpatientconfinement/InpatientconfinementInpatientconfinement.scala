package com.humedica.mercury.etl.crossix.inpatientconfinement

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._

class InpatientconfinementInpatientconfinement(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

    tables = List("inpatient_confinement", "place_of_service", "diagnosis", "drg", "procedure_code", "provider")

    columns = List("ADMISSION_DATE","AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE",
        "AMT_STANDARD_COST", "D_DISCHARGE_STATUS_CODE", "D_PLACE_OF_SERVICE_CODE", "DIAGNOSIS_1_CODE", "DIAGNOSIS_2_CODE",
        "DIAGNOSIS_3_CODE", "DIAGNOSIS_4_CODE", "DIAGNOSIS_5_CODE", "DISCHARGE_DATE", "DRG_CODE", "ICD_VERSION_CODE",
        "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID", "LENGTH_OF_STAY_DAYS", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
        "PROCEDURE_1_CODE", "PROCEDURE_2_CODE", "PROCEDURE_3_CODE", "PROCEDURE_4_CODE", "PROCEDURE_5_CODE", "PROVIDER_KEY",
        "PROVIDER_NPI", "RUN_OUT_PERIOD_IND", "STANDARD_COST_YEAR")

    columnSelect = Map(
        "inpatient_confinement" -> List("ADMISSION_DATE", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_STANDARD_COST",
            "D_DISCHARGE_STATUS_CODE", "DISCHARGE_DATE", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID", "LENGTH_OF_STAY_DAYS",
            "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID", "PROVIDER_KEY", "RUN_OUT_PERIOD_IND", "STANDARD_COST_YEAR",
            "DIAGNOSIS_OTH_1_KEY", "DIAGNOSIS_OTH_2_KEY", "DIAGNOSIS_OTH_3_KEY", "DIAGNOSIS_OTH_4_KEY", "DIAGNOSIS_OTH_5_KEY",
            "DRG_KEY", "PLACE_OF_SERVICE_KEY", "PROCEDURE_OTH_1_KEY", "PROCEDURE_OTH_2_KEY", "PROCEDURE_OTH_3_KEY", "PROCEDURE_OTH_4_KEY",
            "PROCEDURE_OTH_5_KEY"),
        "diagnosis" -> List("DIAGNOSIS_KEY", "CODE", "ICD_VER_CD"),
        "drg" -> List("DRG_KEY", "CODE"),
        "place_of_service" -> List("PLACE_OF_SERVICE_KEY", "AMA_CODE"),
        "procedure_code" -> List("PROCEDURE_KEY", "CODE"),
        "provider" -> List("NPI_NBR", "PROVIDER_KEY")
    )


    join = (dfs: Map[String, DataFrame]) => {
        var diagnosis = dfs("diagnosis").select("DIAGNOSIS_KEY", "CODE", "ICD_VER_CD").distinct()
        var drg = dfs("drg").select("DRG_KEY", "CODE").distinct()
        var procedure_code = dfs("procedure_code").select("PROCEDURE_KEY", "CODE").distinct()
        var provider = dfs("provider").select("NPI_NBR", "PROVIDER_KEY").distinct()
        var placeofservice = dfs("place_of_service").select("PLACE_OF_SERVICE_KEY", "AMA_CODE").distinct()
        var inpatientconfinement = dfs("inpatient_confinement").select("ADMISSION_DATE", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_STANDARD_COST",
            "D_DISCHARGE_STATUS_CODE", "DISCHARGE_DATE", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID", "LENGTH_OF_STAY_DAYS",
            "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID", "PROVIDER_KEY", "RUN_OUT_PERIOD_IND", "STANDARD_COST_YEAR",
            "DIAGNOSIS_OTH_1_KEY", "DIAGNOSIS_OTH_2_KEY", "DIAGNOSIS_OTH_3_KEY", "DIAGNOSIS_OTH_4_KEY", "DIAGNOSIS_OTH_5_KEY",
            "DRG_KEY", "PLACE_OF_SERVICE_KEY", "PROCEDURE_OTH_1_KEY", "PROCEDURE_OTH_2_KEY", "PROCEDURE_OTH_3_KEY", "PROCEDURE_OTH_4_KEY",
            "PROCEDURE_OTH_5_KEY").distinct()
        inpatientconfinement
                .join(placeofservice, Seq("PLACE_OF_SERVICE_KEY"), "left_outer")
                .join(diagnosis.as("diagnosis1").withColumnRenamed("CODE", "DIAGNOSIS_1_CODE").withColumnRenamed("DIAGNOSIS_KEY", "DIAGNOSIS_OTH_1_KEY"), Seq("DIAGNOSIS_OTH_1_KEY"), "left_outer")
                .join(diagnosis.as("diagnosis2").withColumnRenamed("CODE", "DIAGNOSIS_2_CODE").withColumnRenamed("DIAGNOSIS_KEY", "DIAGNOSIS_OTH_2_KEY").drop("ICD_VER_CD"), Seq("DIAGNOSIS_OTH_2_KEY"), "left_outer")
                .join(diagnosis.as("diagnosis3").withColumnRenamed("CODE", "DIAGNOSIS_3_CODE").withColumnRenamed("DIAGNOSIS_KEY", "DIAGNOSIS_OTH_3_KEY").drop("ICD_VER_CD"), Seq("DIAGNOSIS_OTH_3_KEY"), "left_outer")
                .join(diagnosis.as("diagnosis4").withColumnRenamed("CODE", "DIAGNOSIS_4_CODE").withColumnRenamed("DIAGNOSIS_KEY", "DIAGNOSIS_OTH_4_KEY").drop("ICD_VER_CD"), Seq("DIAGNOSIS_OTH_4_KEY"), "left_outer")
                .join(diagnosis.as("diagnosis5").withColumnRenamed("CODE", "DIAGNOSIS_5_CODE").withColumnRenamed("DIAGNOSIS_KEY", "DIAGNOSIS_OTH_5_KEY").drop("ICD_VER_CD"), Seq("DIAGNOSIS_OTH_5_KEY"), "left_outer")
                .join(drg, Seq("DRG_KEY"), "left_outer")
                .join(procedure_code.as("procedure_code1").withColumnRenamed("CODE", "PROCEDURE_1_CODE").withColumnRenamed("PROCEDURE_KEY", "PROCEDURE_OTH_1_KEY"), Seq("PROCEDURE_OTH_1_KEY"), "left_outer")
                .join(procedure_code.as("procedure_code2").withColumnRenamed("CODE", "PROCEDURE_2_CODE").withColumnRenamed("PROCEDURE_KEY", "PROCEDURE_OTH_2_KEY"), Seq("PROCEDURE_OTH_2_KEY"), "left_outer")
                .join(procedure_code.as("procedure_code3").withColumnRenamed("CODE", "PROCEDURE_3_CODE").withColumnRenamed("PROCEDURE_KEY", "PROCEDURE_OTH_3_KEY"), Seq("PROCEDURE_OTH_3_KEY"), "left_outer")
                .join(procedure_code.as("procedure_code4").withColumnRenamed("CODE", "PROCEDURE_4_CODE").withColumnRenamed("PROCEDURE_KEY", "PROCEDURE_OTH_4_KEY"), Seq("PROCEDURE_OTH_4_KEY"), "left_outer")
                .join(procedure_code.as("procedure_code5").withColumnRenamed("CODE", "PROCEDURE_5_CODE").withColumnRenamed("PROCEDURE_KEY", "PROCEDURE_OTH_5_KEY"), Seq("PROCEDURE_OTH_5_KEY"), "left_outer")
                .join(provider, Seq("PROVIDER_KEY"), "left_outer")
    }

    map = Map(
        "D_PLACE_OF_SERVICE_CODE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("AMA_CODE") isin("05", "06", "07", "08", "09", "26"), "99").otherwise(df("AMA_CODE")))
        }),
        "DIAGNOSIS_1_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("DIAGNOSIS_OTH_1_KEY") isin ("0"), "").otherwise(df("DIAGNOSIS_1_CODE")))}),
        "DIAGNOSIS_2_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("DIAGNOSIS_OTH_2_KEY") isin ("0"), "").otherwise(df("DIAGNOSIS_2_CODE")))}),
        "DIAGNOSIS_3_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("DIAGNOSIS_OTH_3_KEY") isin ("0"), "").otherwise(df("DIAGNOSIS_3_CODE")))}),
        "DIAGNOSIS_4_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("DIAGNOSIS_OTH_4_KEY") isin ("0"), "").otherwise(df("DIAGNOSIS_4_CODE")))}),
        "DIAGNOSIS_5_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("DIAGNOSIS_OTH_5_KEY") isin ("0"), "").otherwise(df("DIAGNOSIS_5_CODE")))}),
        "ICD_VERSION_CODE" -> mapFrom("ICD_VER_CD"),
        "DRG_CODE" -> mapFrom("CODE"),
        "PROCEDURE_1_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("PROCEDURE_OTH_1_KEY") isin ("0", "1"), "0000").otherwise(substring(df("PROCEDURE_1_CODE"), 1,7)))}),
        "PROCEDURE_2_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("PROCEDURE_OTH_2_KEY") isin ("0", "1"), "0000").otherwise(substring(df("PROCEDURE_2_CODE"), 1,7)))}),
        "PROCEDURE_3_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("PROCEDURE_OTH_3_KEY") isin ("0", "1"), "0000").otherwise(substring(df("PROCEDURE_3_CODE"), 1,7)))}),
        "PROCEDURE_4_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("PROCEDURE_OTH_4_KEY") isin ("0", "1"), "0000").otherwise(substring(df("PROCEDURE_4_CODE"), 1,7)))}),
        "PROCEDURE_5_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("PROCEDURE_OTH_5_KEY") isin ("0", "1"), "0000").otherwise(substring(df("PROCEDURE_5_CODE"), 1,7)))}),
        "PROVIDER_NPI" -> mapFrom("NPI_NBR"),
        "LSRD_MEMBER_SYSTEM_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("LSRD_MEMBER_SYSTEM_ID"), 256)))
    )

}

//build(new InpatientconfinementInpatientconfinement(cfg), allColumns=true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/INPATIENT_CONFINEMENT_201710")