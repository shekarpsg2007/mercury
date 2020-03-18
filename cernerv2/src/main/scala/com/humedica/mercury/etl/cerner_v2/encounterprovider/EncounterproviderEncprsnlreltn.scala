package com.humedica.mercury.etl.cerner_v2.encounterprovider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 08/09/2018
 */


class EncounterproviderEncprsnlreltn(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinicalencounter:cerner_v2.clinicalencounter.ClinicalencounterEncounter", "enc_prsnl_reltn",
    "cdr.map_provider_role", "cdr.map_predicate_values", "zh_prsnl")

  columnSelect = Map(
    "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID", "DISCHARGETIME"),
    "enc_prsnl_reltn" -> List("BEG_EFFECTIVE_DT_TM", "PRSNL_PERSON_ID", "ENCNTR_ID", "ENCNTR_PRSNL_R_CD", "UPDT_DT_TM"),
    "zh_prsnl" -> List("PERSON_ID", "NAME_FULL_FORMATTED")
  )

  beforeJoin = Map(
    "enc_prsnl_reltn" -> ((df: DataFrame) => {
      df.filter("beg_effective_dt_tm is not null and prsnl_person_id <> '0'")
    }),
    "cdr.map_provider_role" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and cui <> 'CH001419'")
        .drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("enc_prsnl_reltn")
      .join(dfs("clinicalencounter"), dfs("enc_prsnl_reltn")("ENCNTR_ID") === dfs("clinicalencounter")("ENCOUNTERID"), "inner")
      .join(dfs("cdr.map_provider_role"), dfs("enc_prsnl_reltn")("ENCNTR_PRSNL_R_CD") === dfs("cdr.map_provider_role")("LOCALCODE"), "inner")
      .join(dfs("zh_prsnl"), dfs("enc_prsnl_reltn")("PRSNL_PERSON_ID") === dfs("zh_prsnl")("PERSON_ID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val full_name_excl_pattern = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROV_EXCLUDE", "ENCOUNTERPROVIDER", "ENCOUNTER_PRSNL_RELTN", "EXCLUSION_STRING")
    df.filter("coalesce(name_full_formatted, 'NONAME') not like " + full_name_excl_pattern)
  }

  afterJoinExceptions = Map(
    "H984787_CR2" -> ((df: DataFrame) => {
      val full_name_excl_pattern = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROV_EXCLUDE", "ENCOUNTERPROVIDER", "ENCOUNTER_PRSNL_RELTN", "EXCLUSION_STRING")
      df.filter("coalesce(name_full_formatted, 'NONAME') not like " + full_name_excl_pattern +
        " and not (cui = 'CH001408' and updt_dt_tm > dischargetime)")
    })
  )

  map = Map(
    "DATASRC" -> literal("enc_prsnl_reltn"),
    "ENCOUNTERTIME" -> mapFrom("BEG_EFFECTIVE_DT_TM"),
    "PROVIDERID" -> mapFrom("PRSNL_PERSON_ID"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "PROVIDERROLE" -> mapFrom("ENCNTR_PRSNL_R_CD", prefix=config(CLIENT_DS_ID) + ".")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("PROVIDERID"), df("ENCOUNTERTIME"), df("PROVIDERROLE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}