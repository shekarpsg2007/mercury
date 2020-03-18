package com.humedica.mercury.etl.cerner_v2.clinicalencounter

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by mschlomka on 8/13/2018.
  */

class ClinicalencounterTemptable(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  tables = List("encounter", "patient:cerner_v2.patient.PatientPatient", "cdr.map_predicate_values")

  columns = List("ACCOMMODATION_CD","ACCOMMODATION_REASON_CD", "ACCOMMODATION_REQUEST_CD","ACCOMP_BY_CD", "ACTIVE_IND",
    "ACTIVE_STATUS_CD", "ACTIVE_STATUS_DT_TM", "ACTIVE_STATUS_PRSNL_ID", "ADMIT_MODE_CD", "ADMIT_SRC_CD", "ADMIT_TYPE_CD",
    "ADMIT_WITH_MEDICATION_CD", "ALC_DECOMP_DT_TM", "ALC_REASON_CD", "ALT_LVL_CARE_CD", "ALT_LVL_CARE_DT_TM", "ALT_RESULT_DEST_CD",
    "AMBULATORY_COND_CD", "ARCHIVE_DT_TM_ACT", "ARCHIVE_DT_TM_EST", "ARRIVE_DT_TM", "ASSIGN_TO_LOC_DT_TM", "BBD_PROCEDURE_CD",
    "BEG_EFFECTIVE_DT_TM", "BIRTH_DT_CD", "BIRTH_DT_TM", "CHART_COMPLETE_DT_TM", "CONFID_LEVEL_CD", "CONTRACT_STATUS_CD",
    "CONTRIBUTOR_SYSTEM_CD", "COURTESY_CD", "CREATE_DT_TM", "CREATE_PRSNL_ID", "DATA_STATUS_CD", "DATA_STATUS_DT_TM",
    "DATA_STATUS_PRSNL_ID", "DEPART_DT_TM", "DIET_TYPE_CD", "DISCH_DISPOSITION_CD", "DISCH_DT_TM", "DISCH_TO_LOCTN_CD",
    "DOC_RCVD_DT_TM", "ENCNTR_CLASS_CD", "ENCNTR_COMPLETE_DT_TM", "ENCNTR_FINANCIAL_ID", "ENCNTR_ID", "ENCNTR_STATUS_CD",
    "ENCNTR_TYPE_CD", "ENCNTR_TYPE_CLASS_CD", "END_EFFECTIVE_DT_TM", "EST_ARRIVE_DT_TM", "EST_DEPART_DT_TM", "EST_LENGTH_OF_STAY",
    "EXPECTED_DELIVERY_DT_TM", "FILEID", "FINANCIAL_CLASS_CD", "GUARANTOR_TYPE_CD", "INFO_GIVEN_BY", "INITIAL_CONTACT_DT_TM",
    "INPATIENT_ADMIT_DT_TM", "ISOLATION_CD", "LAST_MENSTRUAL_PERIOD_DT_TM", "LOC_BED_CD", "LOC_BUILDING_CD", "LOC_FACILITY_CD",
    "LOC_NURSE_UNIT_CD", "LOC_ROOM_CD", "LOC_TEMP_CD", "LOCATION_CD", "MED_SERVICE_CD", "MENTAL_CATEGORY_CD", "MENTAL_HEALTH_CD",
    "MENTAL_HEALTH_DT_TM", "NAME_FIRST", "NAME_FIRST_KEY", "NAME_FIRST_SYNONYM_ID", "NAME_FULL_FORMATTED", "NAME_LAST",
    "NAME_LAST_KEY", "NAME_PHONETIC", "ONSET_DT_TM", "ORGANIZATION_ID", "PA_CURRENT_STATUS_CD", "PA_CURRENT_STATUS_DT_TM",
    "PARENT_RET_CRITERIA_ID", "PATIENT_CLASSIFICATION_CD", "PERSON_ID", "PLACEMENT_AUTH_PRSNL_ID", "PRE_REG_DT_TM",
    "PRE_REG_PRSNL_ID", "PREADMIT_NBR", "PREADMIT_TESTING_CD", "PREGNANCY_STATUS_CD", "PROGRAM_SERVICE_CD", "PSYCHIATRIC_STATUS_CD",
    "PURGE_DT_TM_ACT", "PURGE_DT_TM_EST", "READMIT_CD", "REASON_FOR_VISIT", "REFER_FACILITY_CD", "REFERRAL_RCVD_DT_TM",
    "REFERRING_COMMENT", "REG_DT_TM", "REG_PRSNL_ID", "REGION_CD", "RESULT_ACCUMULATION_DT_TM", "RESULT_DEST_CD", "SAFEKEEPING_CD",
    "SECURITY_ACCESS_CD", "SERVICE_CATEGORY_CD", "SEX_CD", "SITTER_REQUIRED_CD", "SPECIALTY_UNIT_CD", "SPECIES_CD", "TRAUMA_CD",
    "TRAUMA_DT_TM", "TRIAGE_CD", "TRIAGE_DT_TM", "UPDT_APPLCTX", "UPDT_CNT", "UPDT_DT_TM", "UPDT_ID", "UPDT_TASK", "VALUABLES_CD",
    "VIP_CD", "VISITOR_STATUS_CD", "ZERO_BALANCE_DT_TM", "DISCHARGETIME", "ADMITTIME", "INS_TIMESTAMP", "ARRIVALTIME", "ROWNUMBER")

  columnSelect = Map(
    "encounter" -> List("ACCOMMODATION_CD","ACCOMMODATION_REASON_CD", "ACCOMMODATION_REQUEST_CD","ACCOMP_BY_CD", "ACTIVE_IND",
      "ACTIVE_STATUS_CD", "ACTIVE_STATUS_DT_TM", "ACTIVE_STATUS_PRSNL_ID", "ADMIT_MODE_CD", "ADMIT_SRC_CD", "ADMIT_TYPE_CD",
      "ADMIT_WITH_MEDICATION_CD", "ALC_DECOMP_DT_TM", "ALC_REASON_CD", "ALT_LVL_CARE_CD", "ALT_LVL_CARE_DT_TM", "ALT_RESULT_DEST_CD",
      "AMBULATORY_COND_CD", "ARCHIVE_DT_TM_ACT", "ARCHIVE_DT_TM_EST", "ARRIVE_DT_TM", "ASSIGN_TO_LOC_DT_TM", "BBD_PROCEDURE_CD",
      "BEG_EFFECTIVE_DT_TM", "BIRTH_DT_CD", "BIRTH_DT_TM", "CHART_COMPLETE_DT_TM", "CONFID_LEVEL_CD", "CONTRACT_STATUS_CD",
      "CONTRIBUTOR_SYSTEM_CD", "COURTESY_CD", "CREATE_DT_TM", "CREATE_PRSNL_ID", "DATA_STATUS_CD", "DATA_STATUS_DT_TM",
      "DATA_STATUS_PRSNL_ID", "DEPART_DT_TM", "DIET_TYPE_CD", "DISCH_DISPOSITION_CD", "DISCH_DT_TM", "DISCH_TO_LOCTN_CD",
      "DOC_RCVD_DT_TM", "ENCNTR_CLASS_CD", "ENCNTR_COMPLETE_DT_TM", "ENCNTR_FINANCIAL_ID", "ENCNTR_ID", "ENCNTR_STATUS_CD",
      "ENCNTR_TYPE_CD", "ENCNTR_TYPE_CLASS_CD", "END_EFFECTIVE_DT_TM", "EST_ARRIVE_DT_TM", "EST_DEPART_DT_TM", "EST_LENGTH_OF_STAY",
      "EXPECTED_DELIVERY_DT_TM", "FILEID", "FINANCIAL_CLASS_CD", "GUARANTOR_TYPE_CD", "INFO_GIVEN_BY", "INITIAL_CONTACT_DT_TM",
      "INPATIENT_ADMIT_DT_TM", "ISOLATION_CD", "LAST_MENSTRUAL_PERIOD_DT_TM", "LOC_BED_CD", "LOC_BUILDING_CD", "LOC_FACILITY_CD",
      "LOC_NURSE_UNIT_CD", "LOC_ROOM_CD", "LOC_TEMP_CD", "LOCATION_CD", "MED_SERVICE_CD", "MENTAL_CATEGORY_CD", "MENTAL_HEALTH_CD",
      "MENTAL_HEALTH_DT_TM", "NAME_FIRST", "NAME_FIRST_KEY", "NAME_FIRST_SYNONYM_ID", "NAME_FULL_FORMATTED", "NAME_LAST",
      "NAME_LAST_KEY", "NAME_PHONETIC", "ONSET_DT_TM", "ORGANIZATION_ID", "PA_CURRENT_STATUS_CD", "PA_CURRENT_STATUS_DT_TM",
      "PARENT_RET_CRITERIA_ID", "PATIENT_CLASSIFICATION_CD", "PERSON_ID", "PLACEMENT_AUTH_PRSNL_ID", "PRE_REG_DT_TM",
      "PRE_REG_PRSNL_ID", "PREADMIT_NBR", "PREADMIT_TESTING_CD", "PREGNANCY_STATUS_CD", "PROGRAM_SERVICE_CD", "PSYCHIATRIC_STATUS_CD",
      "PURGE_DT_TM_ACT", "PURGE_DT_TM_EST", "READMIT_CD", "REASON_FOR_VISIT", "REFER_FACILITY_CD", "REFERRAL_RCVD_DT_TM",
      "REFERRING_COMMENT", "REG_DT_TM", "REG_PRSNL_ID", "REGION_CD", "RESULT_ACCUMULATION_DT_TM", "RESULT_DEST_CD", "SAFEKEEPING_CD",
      "SECURITY_ACCESS_CD", "SERVICE_CATEGORY_CD", "SEX_CD", "SITTER_REQUIRED_CD", "SPECIALTY_UNIT_CD", "SPECIES_CD", "TRAUMA_CD",
      "TRAUMA_DT_TM", "TRIAGE_CD", "TRIAGE_DT_TM", "UPDT_APPLCTX", "UPDT_CNT", "UPDT_DT_TM", "UPDT_ID", "UPDT_TASK", "VALUABLES_CD",
      "VIP_CD", "VISITOR_STATUS_CD", "ZERO_BALANCE_DT_TM"),
    "patient" -> List("PATIENTID")
  )

  beforeJoin = Map(
    "encounter" -> ((df: DataFrame) => {
      val date_list = List("2100-12-31", "1900-01-01")
      df.withColumn("inp", when(substring(df("INPATIENT_ADMIT_DT_TM"), 1, 10).isin(date_list: _*), null)
          .otherwise(df("INPATIENT_ADMIT_DT_TM")))
        .withColumn("arr", when(substring(df("ARRIVE_DT_TM"), 1, 10).isin(date_list: _*), null)
          .otherwise(df("ARRIVE_DT_TM")))
        .withColumn("reg", when(substring(df("REG_DT_TM"), 1, 10).isin(date_list: _*), null)
          .otherwise(df("REG_DT_TM")))
        .withColumn("ecdt", when(substring(df("ENCNTR_COMPLETE_DT_TM"), 1, 10).isin(date_list: _*), null)
          .otherwise(df("ENCNTR_COMPLETE_DT_TM")))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("encounter")
      .join(dfs("patient"), dfs("encounter")("PERSON_ID") === dfs("patient")("PATIENTID"), "inner")
  }

  afterMap = (df: DataFrame) => {
    val list_encntr_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ARRIVALTIME", "CLINICALENCOUNTER", "ENCOUNTER", "ENCNTR_TYPE_CD")
    val coalesce1 = coalesce(df("inp"), df("arr"), df("reg"))
    val coalesce2 = coalesce(df("arr"), df("reg"), df("inp"))
    val coalesce3 = coalesce(df("reg"), df("inp"), df("arr"))
    val groups = Window.partitionBy(df("ENCNTR_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.filter("encntr_id is not null")
      .withColumn("DISCHARGETIME", df("DISCH_DT_TM"))
      .withColumn("ADMITTIME",
        when((from_unixtime(unix_timestamp(df("DISCH_DT_TM"))) - from_unixtime(unix_timestamp(coalesce1)) < 0) &&
          df("ENCNTR_TYPE_CD").isin(list_encntr_type_cd: _*), df("ecdt"))
        .otherwise(coalesce1))
      .withColumn("INS_TIMESTAMP", coalesce(df("arr"), df("reg")))
      .withColumn("ARRIVALTIME",
        when((from_unixtime(unix_timestamp(df("DISCH_DT_TM"))) - from_unixtime(unix_timestamp(least(coalesce1, coalesce2, coalesce3))) < 0) &&
          df("ENCNTR_TYPE_CD").isin(list_encntr_type_cd: _*), df("ecdt"))
        .otherwise(least(coalesce1, coalesce2, coalesce3)))
      .withColumn("ROWNUMBER", row_number.over(groups))
  }

}