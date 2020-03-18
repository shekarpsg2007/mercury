package com.humedica.mercury.etl.cerner_v2.clinicalencounter

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Engine
import scala.collection.JavaConverters._


/**
 * Auto-generated on 08/09/2018
 */


class ClinicalencounterEncounter(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  tables = List("enc_alias", "zh_nomenclature", "zh_code_value", "cdr.map_predicate_values", "hum_drg",
    "temptable:cerner_v2.clinicalencounter.ClinicalencounterTemptable")

  columnSelect = Map(
    "enc_alias" -> List("ENCNTR_ID", "UPDT_DT_TM", "ENCNTR_ALIAS_TYPE_CD", "ALIAS"),
    "zh_nomenclature" -> List("NOMENCLATURE_ID", "SOURCE_IDENTIFIER", "SOURCE_VOCABULARY_CD"),
    "zh_code_value" -> List("CDF_MEANING", "CODE_VALUE"),
    "hum_drg" -> List("ENCNTR_ID", "PERSON_ID", "NOMENCLATURE_ID", "RISK_OF_MORTALITY_CD", "SEVERITY_OF_ILLNESS_CD",
      "DRG_PRIORITY", "UPDT_DT_TM", "ACTIVE_IND"),
    "temptable" -> List("PERSON_ID", "ENCNTR_ID", "ARRIVALTIME", "ADMITTIME", "DISCHARGETIME", "ENCNTR_TYPE_CD", "MED_SERVICE_CD",
      "ADMIT_SRC_CD", "DISCH_DISPOSITION_CD", "LOC_FACILITY_CD", "LOC_BUILDING_CD", "ACTIVE_IND", "REASON_FOR_VISIT")
  )

  beforeJoin = Map(
    "enc_alias" -> ((df: DataFrame) => {
      val list_encntr_alias_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ALT_ENC_ID", "CLINICALENCOUNTER", "ENC_ALIAS", "ENCNTR_ALIAS_TYPE_CD")
      val df2 = df.filter("encntr_alias_type_cd in (" + list_encntr_alias_type_cd + ")")
      val groups = Window.partitionBy(df("ENCNTR_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last)
      df2.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn")
    }),
    "temptable" -> ((df: DataFrame) => {
      val list_encntr_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTER", "CLINICALENCOUNTER", "ENCOUNTER", "ENCNTR_TYPE_CD")
      df.filter("rownumber = 1 and arrivaltime is not null and active_ind <> '0' and (reason_for_visit is null or reason_for_visit not in ('CANCELLED', 'NO SHOW')) " +
        "and (encntr_type_cd is null or encntr_type_cd not in (" + list_encntr_type_cd + "))")
    }),
    "hum_drg" -> ((df: DataFrame) => {
      val list_zac_source_vocabulary_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERS", "CLINICAL_ENCOUNTER", "ZH_APRDRG_CD", "SOURCE_VOCABULARY_CD")
      val zh_apr = table("zh_nomenclature")
        .filter("source_vocabulary_cd in (" + list_zac_source_vocabulary_cd + ")")
      val hum_drg_1 = df.join(zh_apr, Seq("NOMENCLATURE_ID"), "inner")
        .filter("active_ind = '1' and source_identifier is not null")
        .withColumn("ENCNTR_ID", expr("substring(ENCNTR_ID, 1, instr(ENCNTR_ID, '.') - 1)"))
      val groups = Window.partitionBy(hum_drg_1("ENCNTR_ID"))
        .orderBy(hum_drg_1("DRG_PRIORITY"), hum_drg_1("UPDT_DT_TM").desc_nulls_last)
      val apr_hum_drg = hum_drg_1.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .withColumnRenamed("SOURCE_IDENTIFIER", "APRDRG")
        .withColumnRenamed("NOMENCLATURE_ID", "APR_NOMENCLATURE_ID")
        .select("ENCNTR_ID", "PERSON_ID", "APR_NOMENCLATURE_ID", "RISK_OF_MORTALITY_CD", "SEVERITY_OF_ILLNESS_CD", "APRDRG")

      val list_zld_source_vocabulary_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERS", "CLINICAL_ENCOUNTER", "ZH_LCL_DRG", "SOURCE_VOCABULARY_CD")
      val zh_ms = table("zh_nomenclature")
        .filter("source_vocabulary_cd in (" + list_zld_source_vocabulary_cd + ")")
      val hum_drg_2 = df.join(zh_ms, Seq("NOMENCLATURE_ID"), "inner")
        .filter("active_ind = '1' and source_identifier is not null")
        .withColumn("ENCNTR_ID", expr("substring(ENCNTR_ID, 1, instr(ENCNTR_ID, '.') - 1)"))
        .withColumn("MSDRG", trim(zh_ms("SOURCE_IDENTIFIER")))
      val groups2 = Window.partitionBy(hum_drg_2("ENCNTR_ID"))
        .orderBy(hum_drg_2("DRG_PRIORITY"), hum_drg_2("UPDT_DT_TM").desc_nulls_last)
      val ms_hum_drg = hum_drg_2.withColumn("rn", row_number.over(groups2))
        .filter("rn = 1")
        .withColumn("MSDRG", when(length(hum_drg_2("MSDRG")) > 3, expr("substring(msdrg, length(msdrg) - 2)"))
          .when(length(hum_drg_2("MSDRG")) < 3, lpad(hum_drg_2("MSDRG"), 3, "0"))
          .otherwise(hum_drg_2("MSDRG")))
        .withColumnRenamed("NOMENCLATURE_ID", "MS_NOMENCLATURE_ID")
        .select("ENCNTR_ID", "PERSON_ID", "MS_NOMENCLATURE_ID", "MSDRG")

      apr_hum_drg.join(ms_hum_drg, Seq("ENCNTR_ID", "PERSON_ID"), "full_outer")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    val rcv = table("zh_code_value")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_rcv")
      .withColumnRenamed("CDF_MEANING", "CDF_MEANING_rcv")
    val scv = table("zh_code_value")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_scv")
      .withColumnRenamed("CDF_MEANING", "CDF_MEANING_scv")
    dfs("temptable")
      .join(dfs("hum_drg"), Seq("ENCNTR_ID", "PERSON_ID"), "left_outer")
      .join(dfs("zh_nomenclature"), dfs("hum_drg")("MS_NOMENCLATURE_ID") === dfs("zh_nomenclature")("NOMENCLATURE_ID"), "left_outer")
      .join(rcv, dfs("hum_drg")("RISK_OF_MORTALITY_CD") === rcv("CODE_VALUE_rcv"), "left_outer")
      .join(scv, dfs("hum_drg")("RISK_OF_MORTALITY_CD") === scv("CODE_VALUE_scv"), "left_outer")
      .join(dfs("enc_alias"), Seq("ENCNTR_ID"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("encounter"),
    "ARRIVALTIME" -> mapFrom("ARRIVALTIME"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "APRDRG_SOI" -> mapFrom("CDF_MEANING_scv", nullIf = Seq("0")),
    "ADMITTIME" -> mapFrom("ADMITTIME"),
    "ALT_ENCOUNTERID" -> mapFrom("ALIAS"),
    "APRDRG_CD" -> mapFrom("APRDRG"),
    "APRDRG_ROM" -> mapFrom("CDF_MEANING_rcv", nullIf = Seq("0")),
    "DISCHARGETIME" -> mapFrom("DISCHARGETIME"),
    "FACILITYID" -> mapFrom("LOC_FACILITY_CD", nullIf = Seq("0")),
    "LOCALADMITSOURCE" -> mapFrom("ADMIT_SRC_CD", prefix = config(CLIENT_DS_ID) + ".", nullIf = Seq("0")),
    "LOCALDISCHARGEDISPOSITION" -> mapFrom("DISCH_DISPOSITION_CD", prefix = config(CLIENT_DS_ID) + ".", nullIf = Seq("0")),
    "LOCALDRG" -> mapFrom("MSDRG"),
    "LOCALDRGTYPE" -> ((col: String, df: DataFrame) => {
      val list_zldt_source_vocabulary_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERS", "CLINICAL_ENCOUNTER", "ZH_LCL_DRG_TYPE", "SOURCE_VOCABULARY_CD")
      df.withColumn(col, when(df("SOURCE_VOCABULARY_CD").isin(list_zldt_source_vocabulary_cd: _*), lit("MS-DRG"))
        .when(df("SOURCE_VOCABULARY_CD") === lit("1221"), lit("DRG"))
        .otherwise(null))
    }),
    "LOCALPATIENTTYPE" -> ((col: String, df: DataFrame) => {
      val list_med_svc_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTER", "CLINICALENCOUNTER", "ENCOUNTER", "MED_SERVICE_CD")
      df.withColumn(col, when(df("ENCNTR_TYPE_CD").isNull || df("ENCNTR_TYPE_CD") === lit("0"), null)
        .when(concat_ws("_", df("ENCNTR_TYPE_CD"), df("MED_SERVICE_CD")).isin(list_med_svc_cd: _*),
          concat(lit(config(CLIENT_DS_ID) + "."), df("ENCNTR_TYPE_CD"), lit("_"), df("MED_SERVICE_CD")))
        .otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("ENCNTR_TYPE_CD"))))
    })
  )

  afterMap = (df: DataFrame) => {
    val cols = Engine.schema.getStringList("Clinicalencounter").asScala.map(_.split("-")(0).toUpperCase())
    df.select(cols.map(col): _*).distinct
  }

  mapExceptions = Map(
    ("H416989_CR2", "FACILITYID") -> mapFrom("LOC_BUILDING_CD", nullIf = Seq("0"))
  )

}