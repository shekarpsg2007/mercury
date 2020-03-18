package com.humedica.mercury.etl.crossix.util

import java.text.SimpleDateFormat
import com.humedica.mercury.etl.crossix.inpatientconfinement.InpatientconfinementInpatientconfinement
import com.humedica.mercury.etl.crossix.labresultshipaa.LabresulthipaaLabresulthipaa
import com.humedica.mercury.etl.crossix.medicalclaim.MedicalclaimMedicalclaim
import com.humedica.mercury.etl.crossix.membercoverage.MembercoverageMembercoverage
import com.humedica.mercury.etl.crossix.membercoveragepartd.MembercoveragepartdMembercoveragepartd
import com.humedica.mercury.etl.crossix.memberphi.MemberphiMemberphi
import com.humedica.mercury.etl.crossix.orxmembercoverage.OrxmembercoverageOrxmembercoverage
import com.humedica.mercury.etl.crossix.orxmembercoveragepartd.OrxmembercoveragepartdOrxmembercoveragepartd
import com.humedica.mercury.etl.crossix.orxmemberphi.OrxmemberphiOrxmemberphi
import com.humedica.mercury.etl.crossix.orxpharmacyclaim.OrxpharmacyclaimOrxpharmacyclaim
import com.humedica.mercury.etl.crossix.orxpharmacyclaimpartd.OrxpharmacyclaimpartdOrxpharmacyclaimpartd
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object CrossixNHIOpUtility {

    def getValue (key:String, monthStr:String, config: Map[String,String]) : String={
        var value = config.get(key).get
        if(!value.contains(monthStr)) {
            println("ERROR INVALID MonthStr.."+key+":"+monthStr+ " : "+value+". Please change etl.conf")
            throw new Exception("")
        }
        value
    }

    def prepareMonthlyNHIInputData(sqlContext: SQLContext, monthStr: String, config: Map[String,String]) = {

        var user_home = config.get("HOME_DIR").get

        var memberPhiPath = getValue("MEMBER_PHI_MONTHLY_MASTER", monthStr, config)
        var memberCoveragePartdPath =  getValue("MEMBER_COVERAGE_PARTD_MONTHLY_MASTER", monthStr, config)
        var memberCoveragePath = getValue("MEMBER_COVERAGE_MONTHLY_MASTER", monthStr, config)
        var labResultHipaaPath = getValue("LAB_RESULT_HIPAA_MONTHLY_MASTER", monthStr, config)
        var revenuePath = getValue("REVENUE_MONTHLY_MASTER", monthStr, config)
        var procedureModifierPath = getValue("PROCEDURE_MODIFIER_MONTHLY_MASTER", monthStr, config)
        var ndcPath = getValue("NDC_MONTHLY_MASTER", monthStr, config)
        var inpatientConfinementPath = getValue("INPATIENT_CONFINEMENT_MONTHLY_MASTER", monthStr, config)
        var procedureCodePath = getValue("PROCEDURE_CODE_MONTHLY_MASTER", monthStr, config)
        var drgPath = getValue("DRG_MONTHLY_MASTER", monthStr, config)
        var diagnosisPath = getValue("DIAGNOSIS_MONTHLY_MASTER", monthStr, config)
        var placeOfServicePath = getValue("PLACE_OF_SERVICE_MONTHLY_MASTER", monthStr, config)
        var providerPath = getValue("PROVIDER_MONTHLY_MASTER", monthStr, config)
        var medicalClaimPath = getValue("MEDICAL_CLAIM_MONTHLY_MASTER", monthStr, config)
        var physicianCSCPath = getValue("PHYSICIAN_CLAIM_STD_COST_MONTHLY_MASTER", monthStr, config)
        var facilityCSCPath = getValue("FACILITY_CLAIM_STD_COST_MONTHLY_MASTER", monthStr, config)
        var baseDir = user_home +"/data/nhi/monthly/" + monthStr

        println("Preparing INPATIENT_CONFINEMENT from "+inpatientConfinementPath+" into "+baseDir)
        sqlContext.read.parquet(inpatientConfinementPath)
                        .select("ADMISSION_DATE", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_STANDARD_COST","D_DISCHARGE_STATUS_CODE", "DISCHARGE_DATE",
                            "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID", "LENGTH_OF_STAY_DAYS","LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID", "PROVIDER_KEY",
                            "RUN_OUT_PERIOD_IND", "STANDARD_COST_YEAR","DIAGNOSIS_OTH_1_KEY", "DIAGNOSIS_OTH_2_KEY", "DIAGNOSIS_OTH_3_KEY", "DIAGNOSIS_OTH_4_KEY",
                            "DIAGNOSIS_OTH_5_KEY","DRG_KEY", "PLACE_OF_SERVICE_KEY", "PROCEDURE_OTH_1_KEY", "PROCEDURE_OTH_2_KEY", "PROCEDURE_OTH_3_KEY",
                            "PROCEDURE_OTH_4_KEY","PROCEDURE_OTH_5_KEY")
                .distinct().write.parquet(baseDir+"/INPATIENT_CONFINEMENT")
        println("Preparing PLACE_OF_SERVICE from "+placeOfServicePath+" into "+baseDir)
        sqlContext.read.parquet(placeOfServicePath).select("PLACE_OF_SERVICE_KEY", "AMA_CODE").distinct().write.parquet(baseDir+"/PLACE_OF_SERVICE")

        println("Preparing DIAGNOSIS from "+diagnosisPath+" into "+baseDir)
        sqlContext.read.parquet(diagnosisPath).select("DIAGNOSIS_KEY", "CODE", "ICD_VER_CD").distinct().write.parquet(baseDir+"/DIAGNOSIS")

        println("Preparing DRG from "+drgPath+" into "+baseDir)
        sqlContext.read.parquet(drgPath).select("DRG_KEY", "CODE").distinct().write.parquet(baseDir+"/DRG")

        println("Preparing PROCEDURE_CODE from "+procedureCodePath+" into "+baseDir)
        sqlContext.read.parquet(procedureCodePath).select("PROCEDURE_KEY", "CODE").distinct().write.parquet(baseDir+"/PROCEDURE_CODE")

        println("Preparing NDC from "+ndcPath+" into "+baseDir)
        sqlContext.read.parquet(ndcPath).select("NDC_KEY", "CODE").distinct().write.parquet(baseDir+"/NDC")

        println("Preparing PROCEDURE_MODIFIER from "+procedureModifierPath+" into "+baseDir)
        sqlContext.read.parquet(procedureModifierPath).select("PROCEDURE", "CODE").distinct().write.parquet(baseDir+"/PROCEDURE_MODIFIER")

        println("Preparing PROVIDER from "+providerPath+" into "+baseDir)
        sqlContext.read.parquet(providerPath).select("PROVIDER_KEY", "PROVIDER_CATEGORY_CODE", "NPI_NBR").distinct().write.parquet(baseDir+"/PROVIDER")

        println("Preparing REVENUE from "+revenuePath+" into "+baseDir)
        sqlContext.read.parquet(revenuePath).select("REVENUE_KEY", "CODE").distinct().write.parquet(baseDir+"/REVENUE")

        println("Preparing MEMBER_PHI from "+memberPhiPath+" into "+baseDir)
        sqlContext.read.parquet(memberPhiPath)
                .select("ENROLLEE_ID","INDIVIDUAL_ID","FAMILY_ID","SOURCE_MEMBER_ID","FIRST_NAME","MIDDLE_INITIAL",
                    "LAST_NAME","PHI_NAME","DATE_OF_BIRTH","GENDER","SSN","MEDICARE_ID","MEDICAID_RECIPIENT_NBR",
                    "MEDICAID_FAMILY_NBR","MARITAL_STATUS_CODE","ADDRESS_1","ADDRESS_2","CITY_NAME","STATE_CODE",
                    "ZIPCODE","PHONE","DATA_SOURCE","UPDATE_DATE","UPDATE_PROCESS_ID","MEDICAL_CLAIMS_PROCESSOR_CODE",
                    "LANGUAGE_CODE","PHI_EFF_DATE","PHI_END_DATE","ADDRESS_TYPE_CODE","MEMBER_PHI_KEY","INDIVIDUAL_ID_MATCH_CODE",
                    "ONCE_ASO_IND","LSRD_INDIVIDUAL_ID","LSRD_ENROLLEE_ID","LSRD_MEDICAID_FAMILY_NBR","LSRD_FAMILY_ID",
                    "CURR_ADDR_FLG","ALT_ID").distinct().write.parquet(baseDir+"/MEMBER_PHI")

        println("Preparing MEMBER_COVERAGE from "+memberCoveragePath+" into "+baseDir)
        sqlContext.read.parquet(memberCoveragePath).select("FUNDING_PHI_IND", "BUSINESS_LINE_CODE", "CUSTOMER_SEGMENT_NBR", "D_CUST_DRIVEN_HEALTH_PLAN_CODE",
            "D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "INDUSTRY_PRODUCT_CODE", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
            "STATE_CODE", "YEAR_OF_BIRTH").distinct().write.parquet(baseDir+"/MEMBER_COVERAGE")

        println("Preparing MEMBER_COVERAGE_PARTD from "+memberCoveragePartdPath+" into "+baseDir)
        sqlContext.read.parquet(memberCoveragePartdPath).select("D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
            "STATE_CODE", "YEAR_OF_BIRTH").distinct().write.parquet(baseDir+"/MEMBER_COVERAGE_PARTD")

        println("Preparing LAB_RESULT_HIPAA from "+labResultHipaaPath+" into "+baseDir)
        sqlContext.read.parquet(labResultHipaaPath).select("ANALYTE_SEQUENCE_NBR", "DATA_SOURCE_CODE","HI_NORMAL_VALUE_NBR","LOINC_CODE",
            "LOW_NORMAL_VALUE_NBR","LSRD_INDIVIDUAL_ID","LSRD_LAB_CLAIM_HEADER_SYS_ID","LSRD_MEMBER_SYSTEM_ID","PROC_CODE",
            "RESULT_ABNORMAL_CODE","RESULT_UNITS_NAME", "RESULT_VALUE_NBR","RESULT_VALUE_TEXT", "SERVICE_FROM_DATE",
            "VENDOR_TEST_DESC", "VENDOR_TEST_NBR", "PROVIDER_KEY").distinct().write.parquet(baseDir+"/LAB_RESULT_HIPAA")


        println("Preparing MEDICAL_CLAIM from "+medicalClaimPath+" into "+baseDir)
        sqlContext.read.parquet(medicalClaimPath).select("ADJUSTED_HCCC_CODE", "ADJUSTED_CLAIM_IND", "AMT_COB_SAVINGS", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE",
            "COB_CODE","DISCHARGE_STATUS_CODE", "ENCOUNTER_CODE", "CLM_LOC_CD", "LSRD_CLAIM_NBR", "LSRD_INDIVIDUAL_ID",
            "LSRD_MEMBER_SYSTEM_ID","PRESENT_ON_ADMISSION_1_CODE", "PRESENT_ON_ADMISSION_2_CODE", "PRESENT_ON_ADMISSION_3_CODE",
            "PRESENT_ON_ADMISSION_4_CODE", "PRESENT_ON_ADMISSION_5_CODE", "PRESENT_ON_ADMISSION_6_CODE","PRESENT_ON_ADMISSION_7_CODE",
            "PRESENT_ON_ADMISSION_8_CODE", "PRESENT_ON_ADMISSION_9_CODE", "PROVIDER_KEY", "QUANTITY_UNITS", "SERVICE_FROM_DATE",
            "SERVICE_LINE_NBR", "SERVICE_POST_DATE", "SERVICE_THRU_DATE","NDC_KEY", "PLACE_OF_SERVICE_KEY", "HEADER_PROCEDURE_1_KEY",
            "HEADER_PROCEDURE_2_KEY", "HEADER_PROCEDURE_3_KEY", "HEADER_PROCEDURE_4_KEY", "HEADER_PROCEDURE_5_KEY", "HEADER_PROCEDURE_6_KEY",
            "SERVICE_PROCEDURE_KEY", "PROCEDURE_MODIFIER_1_KEY", "REVENUE_KEY", "HEADER_DIAGNOSIS_1_KEY", "HEADER_DIAGNOSIS_2_KEY",
            "HEADER_DIAGNOSIS_3_KEY", "HEADER_DIAGNOSIS_4_KEY", "HEADER_DIAGNOSIS_5_KEY", "HEADER_DIAGNOSIS_6_KEY", "HEADER_DIAGNOSIS_7_KEY",
            "HEADER_DIAGNOSIS_8_KEY", "HEADER_DIAGNOSIS_9_KEY", "DRG_KEY", "COSMOS_SITE_CODE").distinct().write.parquet(baseDir+"/MEDICAL_CLAIM")

        println("Preparing PHYSICIAN_CLAIM_STD_COST from "+physicianCSCPath+" into "+baseDir)
        sqlContext.read.parquet(physicianCSCPath).select("AMT_STANDARD_COST", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID",
            "SERVICE_FROM_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "SERVICE_LINE_NBR", "LSRD_MEMBER_SYSTEM_ID", "STANDARD_COST_YEAR")
                .distinct().write.parquet(baseDir+"/PHYSICIAN_CLAIM_STD_COST")

        println("Preparing FACILITY_CLAIM_STD_COST from "+facilityCSCPath+" into "+baseDir)
        sqlContext.read.parquet(facilityCSCPath).select("AMT_STANDARD_COST", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID",
            "SERVICE_FROM_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "SERVICE_LINE_NBR", "LSRD_MEMBER_SYSTEM_ID", "STANDARD_COST_YEAR")
                .distinct().write.parquet(baseDir+"/FACILITY_CLAIM_STD_COST")

        /*
        println("Repartitioning LAB_RESULT_HIPAA from "+baseDir+"/LAB_RESULT_HIPAA into "+baseDir+"/LAB_RESULT_HIPAA_PART")
        sqlContext.read.parquet(baseDir+"/LAB_RESULT_HIPAA").repartition(10).write.parquet(baseDir+"/LAB_RESULT_HIPAA_PART")

        println("Repartitioning MEDICAL_CLAIM_PART from "+baseDir+"/MEDICAL_CLAIM into "+baseDir+"/MEDICAL_CLAIM_PART")
        sqlContext.read.parquet(baseDir+"/MEDICAL_CLAIM").repartition(10).write.parquet(baseDir+"/MEDICAL_CLAIM_PART")
        println("Creating LAB_RESULT_HIPAA<N>...")

        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA0").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA1").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA2").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA3").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA4").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA5").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA6").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA7").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA8").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA9").waitFor()

        println("Creating MEDICAL_CLAIM<N>...")

        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM0").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM1").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM2").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM3").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM4").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM5").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM6").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM7").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM8").waitFor()
        Runtime.getRuntime.exec("hadoop fs -mkdir "+baseDir+"/MEDICAL_CLAIM_PART/MEDICAL_CLAIM9").waitFor()
        */

    }
/*


hadoop fs -mv /user/rbabu/data/nhi/monthly/201801/LAB_RESULT_HIPAA_PART/part-00009-564c544e-eb26-406f-8615-041608c2a78d.snappy.parquet /user/rbabu/data/nhi/monthly/201801/LAB_RESULT_HIPAA_PART/LAB_RESULT_HIPAA9


*/

    def arrayEquals(src:Array[String], dest:Array[String]): Boolean = {
        var str1 = array2String(src)
        var str2 = array2String(dest)
        var flag = str1.equals(str2)
        if(flag == false)
            println("Comparing FAILED \nEXPETD :"+str1 +"\nACTUAL :"+str2)
        flag

    }


    def array2String(src:Array[String]): String = {
        var result = "";
        src.foreach(colName => {
            result = result + "," +colName
        })
        result
    }

/*

    def prepareMonthlyNHIInputData(sqlContext: SQLContext, monthStr: String, config: Map[String,String]) = {
        var memberPhiPath = config.get("MEMBER_PHI_MONTHLY_MASTER").get
        if(!memberPhiPath.contains(monthStr)) {
            throw new Exception(" INVALID YYYYMM... "+monthStr+":"+memberPhiPath+". Please change etl.conf")
        }
        var memberCoveragePartdPath = config.get("MEMBER_COVERAGE_PARTD_MONTHLY_MASTER").get
        var memberCoveragePath = config.get("MEMBER_COVERAGE_MONTHLY_MASTER").get
        var labResultHipaaPath = config.get("LAB_RESULT_HIPAA_MONTHLY_MASTER").get
        var revenuePath = config.get("REVENUE_MONTHLY_MASTER").get
        var procedureModifierPath = config.get("PROCEDURE_MODIFIER_MONTHLY_MASTER").get
        var ndcPath = config.get("NDC_MONTHLY_MASTER").get
        var inpatientConfinementPath = config.get("INPATIENT_CONFINEMENT_MONTHLY_MASTER").get
        var procedureCodePath = config.get("PROCEDURE_CODE_MONTHLY_MASTER").get
        var drgPath = config.get("DRG_MONTHLY_MASTER").get
        var diagnosisPath = config.get("DIAGNOSIS_MONTHLY_MASTER").get
        var placeOfServicePath = config.get("PLACE_OF_SERVICE_MONTHLY_MASTER").get
        var providerPath = config.get("PROVIDER_MONTHLY_MASTER").get
        var medicalClaimPath = config.get("MEDICAL_CLAIM_MONTHLY_MASTER").get
        var physicianCSCPath = config.get("PHYSICIAN_CLAIM_STD_COST_MONTHLY_MASTER").get
        var facilityCSCPath = config.get("FACILITY_CLAIM_STD_COST_MONTHLY_MASTER").get
        var baseDir = "/user/rbabu/data/nhi/monthly/" + monthStr
        println("Preparing INPATIENT_CONFINEMENT from "+inpatientConfinementPath+" into "+baseDir)
        sqlContext.read.parquet(inpatientConfinementPath)
                .select("ADMISSION_DATE", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_STANDARD_COST","D_DISCHARGE_STATUS_CODE", "DISCHARGE_DATE",
                    "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID", "LENGTH_OF_STAY_DAYS","LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID", "PROVIDER_KEY",
                    "RUN_OUT_PERIOD_IND", "STANDARD_COST_YEAR","DIAGNOSIS_OTH_1_KEY", "DIAGNOSIS_OTH_2_KEY", "DIAGNOSIS_OTH_3_KEY", "DIAGNOSIS_OTH_4_KEY",
                    "DIAGNOSIS_OTH_5_KEY","DRG_KEY", "PLACE_OF_SERVICE_KEY", "PROCEDURE_OTH_1_KEY", "PROCEDURE_OTH_2_KEY", "PROCEDURE_OTH_3_KEY",
                    "PROCEDURE_OTH_4_KEY","PROCEDURE_OTH_5_KEY")
                .distinct().write.parquet(baseDir+"/INPATIENT_CONFINEMENT")
        println("Preparing PLACE_OF_SERVICE from "+placeOfServicePath+" into "+baseDir)
        sqlContext.read.parquet(placeOfServicePath).select("PLACE_OF_SERVICE_KEY", "AMA_CODE").distinct().write.parquet(baseDir+"/PLACE_OF_SERVICE")

        println("Preparing DIAGNOSIS from "+diagnosisPath+" into "+baseDir)
        sqlContext.read.parquet(diagnosisPath).select("DIAGNOSIS_KEY", "CODE", "ICD_VER_CD").distinct().write.parquet(baseDir+"/DIAGNOSIS")

        println("Preparing DRG from "+drgPath+" into "+baseDir)
        sqlContext.read.parquet(drgPath).select("DRG_KEY", "CODE").distinct().write.parquet(baseDir+"/DRG")

        println("Preparing PROCEDURE_CODE from "+procedureCodePath+" into "+baseDir)
        sqlContext.read.parquet(procedureCodePath).select("PROCEDURE_KEY", "CODE").distinct().write.parquet(baseDir+"/PROCEDURE_CODE")

        println("Preparing NDC from "+ndcPath+" into "+baseDir)
        sqlContext.read.parquet(ndcPath).select("NDC_KEY", "CODE").distinct().write.parquet(baseDir+"/NDC")

        println("Preparing PROCEDURE_MODIFIER from "+procedureModifierPath+" into "+baseDir)
        sqlContext.read.parquet(procedureModifierPath).select("PROCEDURE", "CODE").distinct().write.parquet(baseDir+"/PROCEDURE_MODIFIER")

        println("Preparing PROVIDER from "+providerPath+" into "+baseDir)
        sqlContext.read.parquet(providerPath).select("PROVIDER_KEY", "PROVIDER_CATEGORY_CODE", "NPI_NBR").distinct().write.parquet(baseDir+"/PROVIDER")

        println("Preparing REVENUE from "+revenuePath+" into "+baseDir)
        sqlContext.read.parquet(revenuePath).select("REVENUE_KEY", "CODE").distinct().write.parquet(baseDir+"/REVENUE")

        println("Preparing MEMBER_PHI from "+memberPhiPath+" into "+baseDir)
        sqlContext.read.parquet(memberPhiPath)
                .select("ENROLLEE_ID","INDIVIDUAL_ID","FAMILY_ID","SOURCE_MEMBER_ID","FIRST_NAME","MIDDLE_INITIAL",
                    "LAST_NAME","PHI_NAME","DATE_OF_BIRTH","GENDER","SSN","MEDICARE_ID","MEDICAID_RECIPIENT_NBR",
                    "MEDICAID_FAMILY_NBR","MARITAL_STATUS_CODE","ADDRESS_1","ADDRESS_2","CITY_NAME","STATE_CODE",
                    "ZIPCODE","PHONE","DATA_SOURCE","UPDATE_DATE","UPDATE_PROCESS_ID","MEDICAL_CLAIMS_PROCESSOR_CODE",
                    "LANGUAGE_CODE","PHI_EFF_DATE","PHI_END_DATE","ADDRESS_TYPE_CODE","MEMBER_PHI_KEY","INDIVIDUAL_ID_MATCH_CODE",
                    "ONCE_ASO_IND","LSRD_INDIVIDUAL_ID","LSRD_ENROLLEE_ID","LSRD_MEDICAID_FAMILY_NBR","LSRD_FAMILY_ID",
                    "CURR_ADDR_FLG","ALT_ID").distinct().write.parquet(baseDir+"/MEMBER_PHI")

        println("Preparing MEMBER_COVERAGE from "+memberCoveragePath+" into "+baseDir)
        sqlContext.read.parquet(memberCoveragePath).select("FUNDING_PHI_IND", "BUSINESS_LINE_CODE", "CUSTOMER_SEGMENT_NBR", "D_CUST_DRIVEN_HEALTH_PLAN_CODE",
            "D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "INDUSTRY_PRODUCT_CODE", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
            "STATE_CODE", "YEAR_OF_BIRTH").distinct().write.parquet(baseDir+"/MEMBER_COVERAGE")

        println("Preparing MEMBER_COVERAGE_PARTD from "+memberCoveragePartdPath+" into "+baseDir)
        sqlContext.read.parquet(memberCoveragePartdPath).select("D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
            "STATE_CODE", "YEAR_OF_BIRTH").distinct().write.parquet(baseDir+"/MEMBER_COVERAGE_PARTD")

        println("Preparing LAB_RESULT_HIPAA from "+labResultHipaaPath+" into "+baseDir)
        sqlContext.read.parquet(labResultHipaaPath).select("ANALYTE_SEQUENCE_NBR", "DATA_SOURCE_CODE","HI_NORMAL_VALUE_NBR","LOINC_CODE",
            "LOW_NORMAL_VALUE_NBR","LSRD_INDIVIDUAL_ID","LSRD_LAB_CLAIM_HEADER_SYS_ID","LSRD_MEMBER_SYSTEM_ID","PROC_CODE",
            "RESULT_ABNORMAL_CODE","RESULT_UNITS_NAME", "RESULT_VALUE_NBR","RESULT_VALUE_TEXT", "SERVICE_FROM_DATE",
            "VENDOR_TEST_DESC", "VENDOR_TEST_NBR", "PROVIDER_KEY").distinct().write.parquet(baseDir+"/LAB_RESULT_HIPAA")


        println("Preparing MEDICAL_CLAIM from "+medicalClaimPath+" into "+baseDir)
        sqlContext.read.parquet(medicalClaimPath).select("ADJUSTED_HCCC_CODE", "ADJUSTED_CLAIM_IND", "AMT_COB_SAVINGS", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE",
            "COB_CODE","DISCHARGE_STATUS_CODE", "ENCOUNTER_CODE", "CLM_LOC_CD", "LSRD_CLAIM_NBR", "LSRD_INDIVIDUAL_ID",
            "LSRD_MEMBER_SYSTEM_ID","PRESENT_ON_ADMISSION_1_CODE", "PRESENT_ON_ADMISSION_2_CODE", "PRESENT_ON_ADMISSION_3_CODE",
            "PRESENT_ON_ADMISSION_4_CODE", "PRESENT_ON_ADMISSION_5_CODE", "PRESENT_ON_ADMISSION_6_CODE","PRESENT_ON_ADMISSION_7_CODE",
            "PRESENT_ON_ADMISSION_8_CODE", "PRESENT_ON_ADMISSION_9_CODE", "PROVIDER_KEY", "QUANTITY_UNITS", "SERVICE_FROM_DATE",
            "SERVICE_LINE_NBR", "SERVICE_POST_DATE", "SERVICE_THRU_DATE","NDC_KEY", "PLACE_OF_SERVICE_KEY", "HEADER_PROCEDURE_1_KEY",
            "HEADER_PROCEDURE_2_KEY", "HEADER_PROCEDURE_3_KEY", "HEADER_PROCEDURE_4_KEY", "HEADER_PROCEDURE_5_KEY", "HEADER_PROCEDURE_6_KEY",
            "SERVICE_PROCEDURE_KEY", "PROCEDURE_MODIFIER_1_KEY", "REVENUE_KEY", "HEADER_DIAGNOSIS_1_KEY", "HEADER_DIAGNOSIS_2_KEY",
            "HEADER_DIAGNOSIS_3_KEY", "HEADER_DIAGNOSIS_4_KEY", "HEADER_DIAGNOSIS_5_KEY", "HEADER_DIAGNOSIS_6_KEY", "HEADER_DIAGNOSIS_7_KEY",
            "HEADER_DIAGNOSIS_8_KEY", "HEADER_DIAGNOSIS_9_KEY", "DRG_KEY", "COSMOS_SITE_CODE").distinct().write.parquet(baseDir+"/MEDICAL_CLAIM")

        println("Preparing PHYSICIAN_CLAIM_STD_COST from "+physicianCSCPath+" into "+baseDir)
        sqlContext.read.parquet(physicianCSCPath).select("AMT_STANDARD_COST", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID",
            "SERVICE_FROM_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "SERVICE_LINE_NBR", "LSRD_MEMBER_SYSTEM_ID", "STANDARD_COST_YEAR")
                .distinct().write.parquet(baseDir+"/PHYSICIAN_CLAIM_STD_COST")

        println("Preparing FACILITY_CLAIM_STD_COST from "+facilityCSCPath+" into "+baseDir)
        sqlContext.read.parquet(facilityCSCPath).select("AMT_STANDARD_COST", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID",
            "SERVICE_FROM_DATE", "COSMOS_SITE_CODE", "LSRD_CLAIM_NBR", "SERVICE_LINE_NBR", "LSRD_MEMBER_SYSTEM_ID", "STANDARD_COST_YEAR")
                .distinct().write.parquet(baseDir+"/FACILITY_CLAIM_STD_COST")

        println("Repartitioning LAB_RESULT_HIPAA from "+baseDir+"/LAB_RESULT_HIPAA into "+baseDir+"/LAB_RESULT_HIPAA_PART")
        sqlContext.read.parquet(baseDir+"/LAB_RESULT_HIPAA").repartition(10).write.parquet(baseDir+"/LAB_RESULT_HIPAA_PART")

        println("Repartitioning LAB_RESULT_HIPAA from "+baseDir+"/MEDICAL_CLAIM into "+baseDir+"/MEDICAL_CLAIM_PART")
        sqlContext.read.parquet(baseDir+"/MEDICAL_CLAIM").repartition(10).write.parquet(baseDir+"/MEDICAL_CLAIM_PART")
    }

*/

    def validateNHIPHI(sqlContext: SQLContext, dateStr:String, config: Map[String,String]): Unit = {
        var user_home = config.get("HOME_DIR").get
        println("Validating ... "+user_home+"/data/nhi/monthly/"+dateStr+"/MEMBER_PHI_"+dateStr)
        var MEMBER_PHI = sqlContext.read.parquet(user_home+"/data/nhi/monthly/"+dateStr+"/MEMBER_PHI_"+dateStr)
        var result = arrayEquals(Array("ENROLLEE_ID","INDIVIDUAL_ID","FAMILY_ID","SOURCE_MEMBER_ID","FIRST_NAME","MIDDLE_INITIAL","LAST_NAME","PHI_NAME","DATE_OF_BIRTH","GENDER","SSN","MEDICARE_ID",
            "MEDICAID_RECIPIENT_NBR","MEDICAID_FAMILY_NBR","MARITAL_STATUS_CODE","ADDRESS_1","ADDRESS_2","CITY_NAME","STATE_CODE","ZIPCODE","PHONE","DATA_SOURCE","UPDATE_DATE",
            "UPDATE_PROCESS_ID","MEDICAL_CLAIMS_PROCESSOR_CODE","LANGUAGE_CODE","PHI_EFF_DATE","PHI_END_DATE","ADDRESS_TYPE_CODE","MEMBER_PHI_KEY",
            "INDIVIDUAL_ID_MATCH_CODE","ONCE_ASO_IND","LSRD_INDIVIDUAL_ID","LSRD_ENROLLEE_ID","LSRD_MEDICAID_FAMILY_NBR","LSRD_FAMILY_ID","CURR_ADDR_FLG","ALT_ID"), MEMBER_PHI.columns)

        if(result == false) {
            throw new Exception("MEMBER_PHI ==> FAILED COLUMN COMPARISON")
        }

        println( "MEMBER_PHI ROWCOUNT :"+MEMBER_PHI.count())
        //CrossixGenericUtil.summary(MEMBER_PHI)

        println("Validating ... "+user_home+"/data/nhi/monthly/"+dateStr+"/MEMBER_COVERAGE_"+dateStr)
        var MEMBER_COVERAGE = sqlContext.read.parquet(user_home+"/data/nhi/monthly/"+dateStr+"/MEMBER_COVERAGE_"+dateStr)
        result = arrayEquals(Array("ASO", "BUSINESS_LINE_CODE","CUSTOMER_SEGMENT_NBR","D_CUST_DRIVEN_HEALTH_PLAN_CODE",
            "D_ZIPCODE_3","EFF_DATE","END_DATE","GENDER","INDUSTRY_PRODUCT_CODE","LSRD_INDIVIDUAL_ID","LSRD_MEMBER_SYSTEM_ID",
            "STATE_CODE","YEAR_OF_BIRTH"), MEMBER_COVERAGE.columns)

        if(result == false) {
            throw new Exception("MEMBER_COVERAGE ==> FAILED COLUMN COMPARISON")
        }

        println( "MEMBER_COVERAGE ROWCOUNT :"+MEMBER_COVERAGE.count())
        //CrossixGenericUtil.summary(MEMBER_COVERAGE)


        println("Validating ... "+user_home+"/data/nhi/monthly/"+dateStr+"/MEMBER_COVERAGE_PARTD_"+dateStr)
        var MEMBER_COVERAGE_PARTD = sqlContext.read.parquet(user_home+"/data/nhi/monthly/"+dateStr+"/MEMBER_COVERAGE_PARTD_"+dateStr)
        result = arrayEquals(Array("D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
            "STATE_CODE", "YEAR_OF_BIRTH"), MEMBER_COVERAGE_PARTD.columns)

        if(result == false) {
            throw new Exception("MEMBER_COVERAGE_PARTD ==> FAILED COLUMN COMPARISON")
        }

        println( "MEMBER_COVERAGE_PARTD ROWCOUNT :"+MEMBER_COVERAGE_PARTD.count())
        //Metrics.summary(MEMBER_COVERAGE_PARTD).show(100, false)
    }



    def runNhiBuild(cfg: Map[String, String], dateStr:String) = {
        println("Creating... InpatientconfinementInpatientconfinement..")
        CrossixGenericUtil.build(new InpatientconfinementInpatientconfinement(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/INPATIENT_CONFINEMENT_"+dateStr)
        println("Creating... LabresulthipaaLabresulthipaa..")
        CrossixGenericUtil.build(new LabresulthipaaLabresulthipaa(cfg)).distinct.write.parquet(cfg.get("LABRESULTHIPAA_DATA_ROOT").get+"/LAB_RESULT_HIPAA_"+dateStr)
        println("Creating... MedicalclaimMedicalclaim..")
        CrossixGenericUtil.build(new MedicalclaimMedicalclaim(cfg)).distinct.write.parquet(cfg.get("MEDICALCLAIM_DATA_ROOT").get+"/MEDICAL_CLAIM_"+dateStr)
        println("Creating... MembercoverageMembercoverage..")
        CrossixGenericUtil.build(new MembercoverageMembercoverage(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/MEMBER_COVERAGE_"+dateStr)
        println("Creating... MembercoveragepartdMembercoveragepartd..")
        CrossixGenericUtil.build(new MembercoveragepartdMembercoveragepartd(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/MEMBER_COVERAGE_PARTD_"+dateStr)
        println("Creating... MemberphiMemberphi..")
        CrossixGenericUtil.build(new MemberphiMemberphi(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/MEMBER_PHI_"+dateStr)
    }



    def validateNHIInpatientConfinement(sqlContext: SQLContext, dateStr:String, config: Map[String,String]): Unit = {
        var user_home = config.get("HOME_DIR").get

        println("Validating ... "+user_home+"/data/nhi/monthly/"+dateStr+"/INPATIENT_CONFINEMENT_"+dateStr)
        var INPATIENT_CONFINEMENT = sqlContext.read.parquet(user_home+"/data/nhi/monthly/"+dateStr+"/INPATIENT_CONFINEMENT_"+dateStr)
        var result = arrayEquals(Array("ADMISSION_DATE","AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE",
            "AMT_STANDARD_COST", "D_DISCHARGE_STATUS_CODE", "D_PLACE_OF_SERVICE_CODE", "DIAGNOSIS_1_CODE", "DIAGNOSIS_2_CODE",
            "DIAGNOSIS_3_CODE", "DIAGNOSIS_4_CODE", "DIAGNOSIS_5_CODE", "DISCHARGE_DATE", "DRG_CODE", "ICD_VERSION_CODE",
            "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID", "LENGTH_OF_STAY_DAYS", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
            "PROCEDURE_1_CODE", "PROCEDURE_2_CODE", "PROCEDURE_3_CODE", "PROCEDURE_4_CODE", "PROCEDURE_5_CODE", "PROVIDER_KEY",
            "PROVIDER_NPI", "RUN_OUT_PERIOD_IND", "STANDARD_COST_YEAR"), INPATIENT_CONFINEMENT.columns)

        if(result == false) {
            throw new Exception("INPATIENT_CONFINEMENT ==> FAILED COLUMN COMPARISON")
        }

        println( "INPATIENT_CONFINEMENT ROWCOUNT :"+INPATIENT_CONFINEMENT.count())
        // Metrics.summary(INPATIENT_CONFINEMENT).show(100, false)

    }


    def parquet2csv(sqlContext: SQLContext, src: String): Unit = {
        var dest = src + "_OUTPUT"
        sqlContext.read.parquet(src).write.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\001").option("codec", "gzip").save(dest)
    }

    def validateNHILabResults(sqlContext: SQLContext, dateStr:String, config: Map[String,String]): Unit = {
        var user_home = config.get("HOME_DIR").get

        println("Validating ... "+user_home+"/data/nhi/monthly/"+dateStr+"/LAB_RESULT_HIPAA_"+dateStr)
        var LAB_RESULT_HIPAA = sqlContext.read.parquet(user_home+"/data/nhi/monthly/"+dateStr+"/LAB_RESULT_HIPAA_"+dateStr)
        var result = arrayEquals(Array("ANALYTE_SEQUENCE_NBR", "DATA_SOURCE_CODE","HI_NORMAL_VALUE_NBR","LOINC_CODE",
            "LOW_NORMAL_VALUE_NBR","LSRD_INDIVIDUAL_ID","LSRD_LAB_CLAIM_HEADER_SYSTEM_ID","LSRD_MEMBER_SYSTEM_ID","PROC_CODE",
            "PROVIDER_NPI","RESULT_ABNORMAL_CODE", "RESULT_UNITS_NAME","RESULT_VALUE_NBR", "RESULT_VALUE_TEXT",
            "SERVICE_FROM_DATE", "VENDOR_TEST_DESC", "VENDOR_TEST_NBR"), LAB_RESULT_HIPAA.columns)

        if(result == false) {
            throw new Exception("LAB_RESULT_HIPAA ==> FAILED COLUMN COMPARISON")
        }

        println( "LAB_RESULT_HIPAA ROWCOUNT :"+LAB_RESULT_HIPAA.count())
        // Metrics.summary(LAB_RESULT_HIPAA).show(100, false)
    }


    def validateNHIMedicalClaim(sqlContext: SQLContext, dateStr:String, config: Map[String,String]): Unit = {
        var user_home = config.get("HOME_DIR").get

        println("Validating ... "+user_home+"/data/nhi/monthly/"+dateStr+"/MEDICAL_CLAIM_"+dateStr)
        var MEDICAL_CLAIM = sqlContext.read.parquet(user_home+"/data/nhi/monthly/"+dateStr+"/MEDICAL_CLAIM_"+dateStr)
        var result = arrayEquals(Array("ADJUSTED_HCCC_CODE", "ADJUSTED_CLAIM_IND", "AMT_COB_SAVINGS", "AMT_COINSURANCE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_STANDARD_COST",
            "COB_CODE", "D_DISCHARGE_STATUS_CODE", "D_PLACE_OF_SERVICE_CODE", "DIAGNOSIS_1_CODE", "DIAGNOSIS_2_CODE", "DIAGNOSIS_3_CODE",
            "DIAGNOSIS_4_CODE", "DIAGNOSIS_5_CODE", "DIAGNOSIS_6_CODE", "DIAGNOSIS_7_CODE", "DIAGNOSIS_8_CODE", "DIAGNOSIS_9_CODE",
            "DRG_CODE", "ENCOUNTER_CODE", "ICD_VERSION_CODE", "IHCIS_TYPE_OF_SERVICE_KEY", "INPATIENT_CONF_ID", "LOCATION_OF_SERVICE_CODE",
            "LSRD_CLAIM_NBR", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID", "NDC_CODE", "PRESENT_ON_ADMISSION_1_CODE", "PRESENT_ON_ADMISSION_2_CODE",
            "PRESENT_ON_ADMISSION_3_CODE", "PRESENT_ON_ADMISSION_4_CODE", "PRESENT_ON_ADMISSION_5_CODE", "PRESENT_ON_ADMISSION_6_CODE",
            "PRESENT_ON_ADMISSION_7_CODE", "PRESENT_ON_ADMISSION_8_CODE", "PRESENT_ON_ADMISSION_9_CODE", "PROCEDURE_1_CODE",
            "PROCEDURE_2_CODE", "PROCEDURE_3_CODE", "PROCEDURE_4_CODE", "PROCEDURE_5_CODE", "PROCEDURE_6_CODE", "PROCEDURE_CODE",
            "PROCEDURE_MODIFIER_CODE", "PROVIDER_CATEGORY_CODE", "PROVIDER_KEY", "PROVIDER_NPI", "QUANTITY_UNITS", "REVENUE_CODE",
            "SERVICE_FROM_DATE", "SERVICE_LINE_NBR", "SERVICE_POST_DATE", "SERVICE_THRU_DATE", "STANDARD_COST_YEAR"), MEDICAL_CLAIM.columns)

        if(result == false) {
            throw new Exception("MEDICAL_CLAIM ==> FAILED COLUMN COMPARISON")
        }

        println( "MEDICAL_CLAIM ROWCOUNT :"+MEDICAL_CLAIM.count())
        // Metrics.summary(MEDICAL_CLAIM).show(100, false)

    }

    def convertParquet2csv(sqlContext: SQLContext, dateStr: String, config: Map[String,String]): Unit = {
        parquet2csv(sqlContext, dateStr, "MEDICAL_CLAIM",config)
        parquet2csv(sqlContext, dateStr, "LAB_RESULT_HIPAA",config)
        parquet2csv(sqlContext, dateStr, "INPATIENT_CONFINEMENT",config)
        parquet2csv(sqlContext, dateStr, "MEMBER_PHI",config)
        parquet2csv(sqlContext, dateStr, "MEMBER_COVERAGE",config)
        parquet2csv(sqlContext, dateStr, "MEMBER_COVERAGE_PARTD",config)
    }

    def parquet2csv(sqlContext: SQLContext, dateStr: String, table: String, config: Map[String,String]): String = {
        var user_home = config.get("HOME_DIR").get

        var src = user_home+"/data/nhi/monthly/" + dateStr + "/" + table+"_"+dateStr
        var upload = user_home+"/data/nhi/monthly/" + dateStr + "/UPLOAD/" + table + "_" + dateStr + ".gz"
        var dest = user_home+"/data/nhi/monthly/" + dateStr + "/UPLOAD/" + table + "_OUTPUT"
        sqlContext.read.parquet(src).write.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\001").option("codec", "gzip").save(dest)
        var hdp = "hadoop fs -text " + dest + "/*.gz  | gzip | hadoop fs -put - " + upload
        println(hdp)
        hdp
    }

    def convertCSVs2GZ(sqlContext: SQLContext, dateStr: String, sc: SparkContext, config: Map[String,String]): Unit = {
        csv2gz(sqlContext, dateStr, "MEDICAL_CLAIM", sc, config)
        csv2gz(sqlContext, dateStr, "LAB_RESULT_HIPAA", sc, config)
        csv2gz(sqlContext, dateStr, "INPATIENT_CONFINEMENT", sc, config)
        csv2gz(sqlContext, dateStr, "MEMBER_PHI", sc, config)
        csv2gz(sqlContext, dateStr, "MEMBER_COVERAGE", sc, config)
        csv2gz(sqlContext, dateStr, "MEMBER_COVERAGE_PARTD", sc, config)
    }

    def csv2gz(sqlContext: SQLContext, dateStr: String, table: String, sc: SparkContext, config: Map[String,String]) = {
        var user_home = config.get("HOME_DIR").get

        var src = user_home+"/data/nhi/monthly/" + dateStr + "/" + table+"_"+dateStr
        var upload = user_home+"/data/nhi/monthly/" + dateStr + "/UPLOAD/" + table + "_" + dateStr + ".gz"
        var c1 = sqlContext.read.parquet(src).count
        var c2 = sc.textFile(upload).count
        if (c1 == c2) {
            println("hadoop fs -copyToLocal " + upload)
            println(table + "_" + dateStr + ".gz : "+c1+" Rows")

        }
        else {
            println("ERROR " + dateStr + " : " + table)
        }
    }

}


 //hadoop fs -text  /user/rbabu/data/orx/weekly/20170928_20171004/ORX_MEMBER_COVERAGE_OUTPUT/*.gz | gzip | hadoop fs -put - /user/rbabu/data/orx/weekly/20170928_20171004/ORX_MEMBER_COVERAGE_20170928_20171004.gz
