package com.humedica.mercury.etl.cerner_v2.providerpatientrelation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 08/09/2018
  */


class ProviderpatientrelationProvpatrel(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("person_prsnl_reltn", "enc_prsnl_reltn", "clinicalencounter:cerner_v2.clinicalencounter.ClinicalencounterEncounter",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "person_prsnl_reltn" -> List("PERSON_ID", "PRSNL_PERSON_ID", "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM",
      "UPDT_DT_TM", "ACTIVE_IND", "ACTIVE_STATUS_DT_TM", "PERSON_PRSNL_R_CD"),
    "enc_prsnl_reltn" -> List("PRSNL_PERSON_ID", "BEG_EFFECTIVE_DT_TM", "END_EFFECTIVE_DT_TM", "ENCNTR_PRSNL_R_CD",
      "UPDT_DT_TM", "ACTIVE_IND", "ACTIVE_STATUS_DT_TM", "ENCNTR_ID"),
    "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID")
  )

  beforeJoin = Map(
    "person_prsnl_reltn" -> ((df: DataFrame) => {
      val addColumn = df.withColumn("DATASRC", lit("person_prsnl_reltn"))
        .withColumnRenamed("PRSNL_PERSON_ID", "PROVIDERID")
        .withColumnRenamed("PERSON_ID", "PATIENTID")
        .withColumnRenamed("BEG_EFFECTIVE_DT_TM", "STARTDATE")
        .withColumnRenamed("END_EFFECTIVE_DT_TM", "ENDDATE")
        .withColumnRenamed("PERSON_PRSNL_R_CD", "PRSNL_R_CD")
        .withColumnRenamed("UPDT_DT_TM", "UPDATE_DATE")
        .withColumnRenamed("ACTIVE_STATUS_DT_TM", "ACTIVE_DATE")
        .select("PROVIDERID", "DATASRC", "PATIENTID", "STARTDATE", "ENDDATE", "PRSNL_R_CD", "UPDATE_DATE", "ACTIVE_IND",
          "ACTIVE_DATE")

      val list_ep_exclude = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENC_PRSNL_RELTN", "PROV_PAT_REL", "ENC_PRSNL_RELTN", "EXCLUDE_TABLE_DATA")
      val ep = table("enc_prsnl_reltn")
        .filter("'Y' not in (" + list_ep_exclude + ")")
      val ce = table("clinicalencounter")
      val joined = ep.join(ce, ep("ENCNTR_ID") === ce("ENCOUNTERID"), "inner")
      val addColumn2 = joined.withColumn("DATASRC", lit("enc_prsnl_reltn"))
        .withColumnRenamed("PRSNL_PERSON_ID", "PROVIDERID")
        .withColumnRenamed("BEG_EFFECTIVE_DT_TM", "STARTDATE")
        .withColumnRenamed("END_EFFECTIVE_DT_TM", "ENDDATE")
        .withColumnRenamed("ENCNTR_PRSNL_R_CD", "PRSNL_R_CD")
        .withColumnRenamed("UPDT_DT_TM", "UPDATE_DATE")
        .withColumnRenamed("ACTIVE_STATUS_DT_TM", "ACTIVE_DATE")
        .select("PROVIDERID", "DATASRC", "PATIENTID", "STARTDATE", "ENDDATE", "PRSNL_R_CD", "UPDATE_DATE", "ACTIVE_IND",
          "ACTIVE_DATE")

      addColumn.union(addColumn2)
    })
  )

  afterJoin = (df: DataFrame) => {
    val list_prsnl_r_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PERSON_PRSNL_RELTN", "PROV_PAT_REL", "PERSON_PRSNL_RELTN", "PERSON_PRSNL_R_CD")
    val fil = df.filter("coalesce(providerid, '0') <> '0' and patientid is not null and startdate is not null " +
      "and prsnl_r_cd in (" + list_prsnl_r_cd + ")")
    val groups = Window.partitionBy(fil("providerid"), fil("patientid"), fil("startdate"))
      .orderBy(fil("update_date").desc_nulls_last, fil("active_ind").desc, fil("active_date").desc_nulls_last, fil("enddate"))
    fil.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

  map = Map(
    "LOCALRELSHIPCODE" -> literal("PCP"),
    "ENDDATE" -> mapFrom("ENDDATE", nullIf = Seq("2100-12-31 00:00:00.0"))
  )

  beforeJoinExceptions = Map(
    "H416989_CR2" -> Map(
      "person_prsnl_reltn" -> ((df: DataFrame) => {
        val addColumn = df.withColumn("DATASRC", lit("person_prsnl_reltn"))
          .withColumnRenamed("PRSNL_PERSON_ID", "PROVIDERID")
          .withColumnRenamed("PERSON_ID", "PATIENTID")
          .withColumnRenamed("BEG_EFFECTIVE_DT_TM", "STARTDATE")
          .withColumnRenamed("END_EFFECTIVE_DT_TM", "ENDDATE")
          .withColumnRenamed("PERSON_PRSNL_R_CD", "PRSNL_R_CD")
          .withColumnRenamed("UPDT_DT_TM", "UPDATE_DATE")
          .withColumnRenamed("ACTIVE_STATUS_DT_TM", "ACTIVE_DATE")
          .filter("active_ind = '1' and substring(end_effective_dt_tm, 1, 10) = '2100-12-31'")
          .select("PROVIDERID", "DATASRC", "PATIENTID", "STARTDATE", "ENDDATE", "PRSNL_R_CD", "UPDATE_DATE", "ACTIVE_IND",
            "ACTIVE_DATE")

        val list_ep_exclude = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENC_PRSNL_RELTN", "PROV_PAT_REL", "ENC_PRSNL_RELTN", "EXCLUDE_TABLE_DATA")
        val ep = table("enc_prsnl_reltn")
          .filter("'Y' not in (" + list_ep_exclude + ")")
        val ce = table("clinicalencounter")
        val joined = ep.join(ce, ep("ENCNTR_ID") === ce("ENCOUNTERID"), "inner")
        val addColumn2 = joined.withColumn("DATASRC", lit("enc_prsnl_reltn"))
          .withColumnRenamed("PRSNL_PERSON_ID", "PROVIDERID")
          .withColumnRenamed("BEG_EFFECTIVE_DT_TM", "STARTDATE")
          .withColumnRenamed("END_EFFECTIVE_DT_TM", "ENDDATE")
          .withColumnRenamed("ENCNTR_PRSNL_R_CD", "PRSNL_R_CD")
          .withColumnRenamed("UPDT_DT_TM", "UPDATE_DATE")
          .withColumnRenamed("ACTIVE_STATUS_DT_TM", "ACTIVE_DATE")
          .select("PROVIDERID", "DATASRC", "PATIENTID", "STARTDATE", "ENDDATE", "PRSNL_R_CD", "UPDATE_DATE", "ACTIVE_IND",
            "ACTIVE_DATE")

        addColumn.union(addColumn2)
      })
    )
  )

}