package com.humedica.mercury.etl.crossix.rxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderCRClinicalEncounter(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List( "DATASRC", "PATIENTID", "ENCOUNTERID", "ARRIVALTIME", "ADMITTIME",
        "DISCHARGETIME", "VISITID", "APRDRG_CD", "LOCALDRG", "APRDRG_SOI", "APRDRG_ROM", "LOCALPATIENTTYPE", "LOCALDISCHARGEDISPOSITION", "ALT_ENCOUNTERID")

    tables = List("encounter", "hum_drg", "enc_alias", "temp_patient:CR2_TEMP_PATIENT", "zh_nomenclature", "zh_code_value", "cdr.map_predicate_values")

    columnSelect = Map(
        "encounter" -> List("FILE_ID", "ACTIVE_IND", "CONTRIBUTOR_SYSTEM_CD", "ENCNTR_ID", "DISCH_DT_TM", "INPATIENT_ADMIT_DT_TM"
            , "ARRIVE_DT_TM", "REG_DT_TM", "UPDT_DT_TM", "PERSON_ID", "ENCNTR_TYPE_CD", "LOC_FACILITY_CD", "LOC_BUILDING_CD", "MED_SERVICE_CD", "ADMIT_SRC_CD",
            "DISCH_DISPOSITION_CD", "REASON_FOR_VISIT"),
        "hum_drg" -> List("FILE_ID", "NOMENCLATURE_ID", "RISK_OF_MORTALITY_CD", "SEVERITY_OF_ILLNESS_CD", "DRG_PRIORITY", "UPDT_DT_TM", "ACTIVE_IND", "ENCNTR_ID", "PERSON_ID"),
        "zh_nomenclature" -> List("FILEID", "SOURCE_VOCABULARY_CD", "NOMENCLATURE_ID", "SOURCE_IDENTIFIER"),
        "zh_code_value" -> List("FILE_ID",  "CODE_VALUE", "CDF_MEANING"),
        "enc_alias" -> List("FILE_ID","ENCNTR_ALIAS_TYPE_CD", "ENCNTR_ID", "ALIAS", "UPDT_DT_TM")
    )

    def find_minimum(column1:Column, column2:Column, column3:Column) = {
        when(column1 < column2 && column1 < column3, column1)
                .when(column2 < column1 && column2 < column3, column2)
                .when(column3 < column1 && column3 < column2, column3)
    }


    def predicate_value_list(p_mpv: DataFrame, dataSrc: String, entity: String, table: String, column: String, colName: String): DataFrame = {
        var mpv1 = p_mpv.filter(p_mpv("DATA_SRC").equalTo(dataSrc).and(p_mpv("ENTITY").equalTo(entity)).and(p_mpv("TABLE_NAME").equalTo(table)).and(p_mpv("COLUMN_NAME").equalTo(column)))
        mpv1=mpv1.withColumn(colName, mpv1("COLUMN_VALUE"))
        mpv1.select(colName).orderBy(mpv1("DTS_VERSION").desc).distinct()
    }

    join = (dfs: Map[String, DataFrame])  => {

        var ENCOUNTER = dfs("encounter")
        ENCOUNTER = ENCOUNTER.withColumn("UPDT_DT_TM_enc", ENCOUNTER("UPDT_DT_TM"))

        var TEMP_INC = ENCOUNTER.withColumn("INP", when(date_format(from_unixtime(ENCOUNTER("INPATIENT_ADMIT_DT_TM").divide(1000)), "yyyy-MM-dd") === "2100-12-31", null)
                .when(date_format(from_unixtime(ENCOUNTER("INPATIENT_ADMIT_DT_TM").divide(1000)), "yyyy-MM-dd") === "1900-01-01", null).otherwise(ENCOUNTER("INPATIENT_ADMIT_DT_TM")))
                .withColumn("ARR", when(date_format(from_unixtime(ENCOUNTER("ARRIVE_DT_TM").divide(1000)), "yyyy-MM-dd") === "2100-12-31", null)
                        .when(date_format(from_unixtime(ENCOUNTER("ARRIVE_DT_TM").divide(1000)), "yyyy-MM-dd") === "1900-01-01", null).otherwise(ENCOUNTER("ARRIVE_DT_TM")))
                .withColumn("REG", when(date_format(from_unixtime(ENCOUNTER("REG_DT_TM").divide(1000)), "yyyy-MM-dd") === "2100-12-31", null)
                        .when(date_format(from_unixtime(ENCOUNTER("REG_DT_TM").divide(1000)), "yyyy-MM-dd") === "1900-01-01", null).otherwise(ENCOUNTER("REG_DT_TM")))

        var TEMP_ENC1 = TEMP_INC.select("REASON_FOR_VISIT", "ACTIVE_IND", "CONTRIBUTOR_SYSTEM_CD", "ENCNTR_ID", "DISCH_DT_TM", "INPATIENT_ADMIT_DT_TM", "ARRIVE_DT_TM",
            "REG_DT_TM", "UPDT_DT_TM", "PERSON_ID", "ENCNTR_TYPE_CD", "LOC_FACILITY_CD", "LOC_BUILDING_CD", "MED_SERVICE_CD",
            "ADMIT_SRC_CD", "DISCH_DISPOSITION_CD", "ARRIVE_DT_TM", "REG_DT_TM", "UPDT_DT_TM_enc")
                .withColumn("ADMITTIME", coalesce(TEMP_INC("INPATIENT_ADMIT_DT_TM"), TEMP_INC("ARRIVE_DT_TM"), TEMP_INC("REG_DT_TM")))

                .withColumn("ARRIVALTIME", find_minimum(coalesce(TEMP_INC("INPATIENT_ADMIT_DT_TM"), TEMP_INC("ARRIVE_DT_TM"), TEMP_INC("REG_DT_TM")),
                    coalesce(TEMP_INC("REG_DT_TM"), TEMP_INC("INPATIENT_ADMIT_DT_TM"), TEMP_INC("ARRIVE_DT_TM")),
                    coalesce(TEMP_INC("ARRIVE_DT_TM"), TEMP_INC("REG_DT_TM"), TEMP_INC("INPATIENT_ADMIT_DT_TM"))))
                .withColumn("INS_TIMESTAMP", when(TEMP_INC("ARRIVE_DT_TM") === TEMP_INC("REG_DT_TM"),null).otherwise(TEMP_INC("ARRIVE_DT_TM")))


        val group1 = Window.partitionBy(TEMP_ENC1("ENCNTR_ID")).orderBy(TEMP_ENC1("UPDT_DT_TM").desc)
        TEMP_ENC1 = TEMP_ENC1.withColumn("RESULT", row_number().over(group1))
        TEMP_ENC1 = TEMP_ENC1.filter(TEMP_ENC1("ENCNTR_ID").isNotNull.and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "472")
                .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "1276570")
                .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "1278011")
                .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "1278025")
                .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "19271182")
                .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "22257494")
                .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "32427669"))

        var TEMP_PATIENT = dfs("temp_table")

        var TEMP_ENC_PATIENT_JN = TEMP_PATIENT.join(TEMP_ENC1, TEMP_PATIENT("PATIENTID") === TEMP_ENC1("PERSON_ID"), "left_outer")

        val group0 = Window.partitionBy(TEMP_ENC_PATIENT_JN("ENCNTR_ID")).orderBy(TEMP_ENC_PATIENT_JN("UPDT_DT_TM").desc)
        TEMP_ENC_PATIENT_JN = TEMP_ENC_PATIENT_JN.withColumn("RN", row_number().over(group0)).filter("RN == 1").drop("RN")


        var HUM_DRG = dfs("hum_drg")
        var HUM_DRG_1 = HUM_DRG.filter(HUM_DRG("ACTIVE_IND") === "1").withColumnRenamed("NOMENCLATURE_ID", "HD_NOMENCLATURE_ID")

        var ZH_NOMENCLATURE = dfs("zh_nomenclature")
        var ZH_NOMENCLATURE_FILTER = ZH_NOMENCLATURE.filter(ZH_NOMENCLATURE("SOURCE_IDENTIFIER").isNotNull)


        var mpv = dfs("map_predicate_values")

        var LIST_ZAC_SOURCE_VACABULARY_CD = predicate_value_list(mpv, "ENCOUNTERS", "CLINICAL_ENCOUNTER", "ZH_APRDRG_CD", "SOURCE_VOCABULARY_CD", "ZACSOURCE_VOCABULARY_CD")

        var APRDRG = ZH_NOMENCLATURE_FILTER.join(LIST_ZAC_SOURCE_VACABULARY_CD, ZH_NOMENCLATURE_FILTER("SOURCE_VOCABULARY_CD") === LIST_ZAC_SOURCE_VACABULARY_CD("ZACSOURCE_VOCABULARY_CD"), "left_outer")

        var APRDRG1 = APRDRG.filter(APRDRG("ZACSOURCE_VOCABULARY_CD").isNull)

        var APR_HUM_DRG = HUM_DRG_1.join(APRDRG1, HUM_DRG_1("HD_NOMENCLATURE_ID") === APRDRG1("NOMENCLATURE_ID") , "left_outer")



        var APR_HUM_DRG1 = APR_HUM_DRG.select("ENCNTR_ID", "PERSON_ID", "NOMENCLATURE_ID", "RISK_OF_MORTALITY_CD", "SEVERITY_OF_ILLNESS_CD", "DRG_PRIORITY", "SOURCE_IDENTIFIER", "UPDT_DT_TM")
                .withColumn("ENCNTR_ID", APR_HUM_DRG("ENCNTR_ID").substr(1, 5)).withColumnRenamed("NOMENCLATURE_ID", "APR_NOMENCLATURE_ID")
                .withColumnRenamed("RISK_OF_MORTALITY_CD", "APR_RISK_OF_MORTALITY_CD").withColumnRenamed("SEVERITY_OF_ILLNESS_CD", "APR_SEVERITY_OF_ILLNESS_CD")
                .withColumnRenamed("SOURCE_IDENTIFIER", "APRDRG").withColumnRenamed("UPDT_DT_TM", "APR_UPDT_DT_TM")

        val group3 = Window.partitionBy(APR_HUM_DRG1("ENCNTR_ID")).orderBy(APR_HUM_DRG1("DRG_PRIORITY").desc, APR_HUM_DRG1("APR_UPDT_DT_TM").desc)
        APR_HUM_DRG1 = APR_HUM_DRG1.withColumn("RESULT1", row_number().over(group3)).filter("RESULT1 == 1").drop("RESULT1")

        var MS_HUM_DRG1 = APR_HUM_DRG1.withColumnRenamed("ENCNTR_ID", "ENCNTR_ID1").withColumnRenamed("APR_NOMENCLATURE_ID", "MS_NOMENCLATURE_ID").withColumnRenamed("APR_RISK_OF_MORTALITY_CD","MS_RISK_OF_MORTALITY_CD")
                .withColumnRenamed("APR_SEVERITY_OF_ILLNESS_CD", "MS_SEVERITY_OF_ILLNESS_CD").withColumnRenamed("APRDRG", "MSDRG").withColumnRenamed("APR_UPDT_DT_TM", "MS_UPDT_DT_TM").withColumnRenamed("PERSON_ID","PERSON_ID2")
        APR_HUM_DRG1 = APR_HUM_DRG1.withColumnRenamed("PERSON_ID","PERSON_ID1")

        APR_HUM_DRG1 = APR_HUM_DRG1.withColumnRenamed("ENCNTR_ID", "ENCNTR_ID3")
        var HUM_DRG2 = APR_HUM_DRG1.join(MS_HUM_DRG1, APR_HUM_DRG1("PERSON_ID1")===  MS_HUM_DRG1("PERSON_ID2") &&  APR_HUM_DRG1("ENCNTR_ID3")===MS_HUM_DRG1("ENCNTR_ID1") && APR_HUM_DRG1("ENCNTR_ID3")===MS_HUM_DRG1("ENCNTR_ID1"), "left_outer")
        var J4 = TEMP_ENC_PATIENT_JN.join(HUM_DRG2, TEMP_ENC_PATIENT_JN("PERSON_ID")===  HUM_DRG2("PERSON_ID1"), "left_outer")


        var RCV = dfs("zh_code_value")
        RCV=RCV.withColumn("FILE_ID", RCV("FILEID").cast("String")).select("FILE_ID",  "CODE_VALUE", "CDF_MEANING")

        var SCV = dfs("zh_code_value")
        SCV=SCV.withColumn("FILE_ID", SCV("FILEID").cast("String")).select("FILE_ID",  "CODE_VALUE", "CDF_MEANING")

        var J5 = J4.join(RCV, J4("APR_RISK_OF_MORTALITY_CD") === RCV("CODE_VALUE"), "left_outer")
        var J6 = J5.join(SCV, J5("APR_SEVERITY_OF_ILLNESS_CD") === SCV("CODE_VALUE"), "left_outer")
        var J7 = J6.join(ZH_NOMENCLATURE_FILTER, J6("MS_NOMENCLATURE_ID") === ZH_NOMENCLATURE_FILTER("NOMENCLATURE_ID"), "left_outer")

        var ENC_ALIAS = dfs("enc_alias")

        val group2 = Window.partitionBy(ENC_ALIAS("ENCNTR_ID")).orderBy(ENC_ALIAS("UPDT_DT_TM").desc)
        var ENC_ALIAS1 = ENC_ALIAS.withColumn("ROW_NUMBER", row_number().over(group2))
        ENC_ALIAS1 = ENC_ALIAS1.filter(ENC_ALIAS1("ROW_NUMBER") === 1).withColumnRenamed("ENCNTR_ID", "ENCNTR_ID1")

        var J8 = J7.join(ENC_ALIAS1, J7("ENCNTR_ID")===ENC_ALIAS1("ENCNTR_ID1"), "left_outer")

        var LIST_ZLDT_SOURCE_VOCABULARY_CD = predicate_value_list(mpv, "ENCOUNTERS", "CLINICAL_ENCOUNTER", "ZH_LCL_DRG_TYPE", "SOURCE_VOCABULARY_CD", "ZLDT_SOURCE_VOCABULARY")

        var RESULT_J1 = J8.join(LIST_ZLDT_SOURCE_VOCABULARY_CD, J8("SOURCE_VOCABULARY_CD") === LIST_ZLDT_SOURCE_VOCABULARY_CD("ZLDT_SOURCE_VOCABULARY"), "left_outer")
        var LIST_ENCNTR_ALIAS_TYPE_CD = predicate_value_list(mpv, "ALT_ENC_ID", "CLINICALENCOUNTER", "ENC_ALIAS", "ENCNTR_ALIAS_TYPE_CD", "LST_ENCNTR_ALIAS_TYPE_CD")
        var RESULT_J2 = RESULT_J1.join(LIST_ENCNTR_ALIAS_TYPE_CD, RESULT_J1("ENCNTR_ALIAS_TYPE_CD") === LIST_ENCNTR_ALIAS_TYPE_CD("LST_ENCNTR_ALIAS_TYPE_CD"), "left_outer")
        var LIST_MED_SVC_CD = predicate_value_list(mpv, "ENCOUNTER", "CLINICALENCOUNTER", "ENCOUNTER", "MED_SERVICE_CD", "LST_MED_SERVICE_CD")
        var RESULT_J3 = RESULT_J2.join(LIST_MED_SVC_CD, concat_ws("_", RESULT_J2("ENCNTR_TYPE_CD"), RESULT_J2("MED_SERVICE_CD")) === LIST_MED_SVC_CD("LST_MED_SERVICE_CD"), "left_outer")
        var RESULT_FE = RESULT_J3.filter((RESULT_J3("RESULT") === 1).and(RESULT_J3("ACTIVE_IND").notEqual("0")).and(RESULT_J3("REASON_FOR_VISIT").isNull.or(RESULT_J3("REASON_FOR_VISIT").notEqual("NO_SHOW") and RESULT_J3("REASON_FOR_VISIT").notEqual("CANCELLED"))).and((RESULT_J3("ENCNTR_ALIAS_TYPE_CD").isNull.or(RESULT_J3("LST_ENCNTR_ALIAS_TYPE_CD").isNull))))
        var RESULT = RESULT_FE.withColumn("DATASRC", lit("encounter"))
        RESULT_FE=RESULT.withColumnRenamed("PERSON_ID", "PATIENTID1").withColumnRenamed("ENCNTR_ID", "ENCOUNTERID").withColumnRenamed("DISCH_DT_TM", "DISCHARGETIME")
        RESULT_FE=RESULT_FE.withColumn("VISITID", RESULT_FE("ENCOUNTERID")).withColumnRenamed("APRDRG", "APRDRG_CD").withColumnRenamed("MSDRG", "LOCALDRG")
        RESULT_FE=RESULT_FE.withColumn("APRDRG_SOI", when(SCV("CDF_MEANING").isNull, "0").otherwise(SCV("CDF_MEANING")))
        RESULT_FE.withColumn("APRDRG_ROM", when(RCV("CDF_MEANING").isNull, "0").otherwise(RCV("CDF_MEANING")))
    }

    map = Map(
        "GROUPID" -> mapFrom("GROUPID"),
        "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
        "ALT_ENCOUNTERID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("ENCNTR_ALIAS_TYPE_CD").isNull, df("ALIAS")).otherwise(null))}),
        "LOCALDRGTYPE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("ZLDT_SOURCE_VOCABULARY").isNull, "MS-DRG").when(df("ZLDT_SOURCE_VOCABULARY") === "1221", "DRG").otherwise(null))}),
        "LOCALDISCHARGEDISPOSITION" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("DISCH_DISPOSITION_CD").isNull, null).otherwise(concat_ws(".", df("CLIENT_DS_ID_person"), df("DISCH_DISPOSITION_CD"))))}),
        "LOCALADMITSOURCE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("ADMIT_SRC_CD").isNull.or(df("ADMIT_SRC_CD") === "0"), null).otherwise(concat_ws(".", df("CLIENT_DS_ID_person"), df("ADMIT_SRC_CD"))))}),

        "LOCALADMITSOURCE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("ENCNTR_TYPE_CD").isNull.or(df("ENCNTR_TYPE_CD") === "0"), null)
                .when(df("LST_MED_SERVICE_CD").isNotNull, concat_ws("_", concat_ws(".", df("CLIENT_DS_ID_person"),  df("ENCNTR_TYPE_CD")), df("MED_SERVICE_CD")))
                .otherwise(concat_ws(".", df("CLIENT_DS_ID_person"),  df("ENCNTR_TYPE_CD"))))}),
        "FACILITYID" -> mapFrom("LOC_FACILITY_CD"),
        "DATASRC" -> literal("encounter")
    )
}

//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")