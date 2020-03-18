package com.humedica.mercury.etl.cerner_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class ProcedureProcedure(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_nomenclature", "procedure", "clinicalencounter:cerner_v2.clinicalencounter.ClinicalencounterEncounter",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "zh_nomenclature" -> List("NOMENCLATURE_ID", "SOURCE_VOCABULARY_CD", "SOURCE_IDENTIFIER", "SOURCE_STRING"),
    "procedure" -> List("RANKING_CD", "NOMENCLATURE_ID", "PROC_DT_TM", "ENCNTR_ID", "UPDT_DT_TM"),
    "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID")
  )

  beforeJoin = Map(
    "procedure" -> ((df: DataFrame) => {
      df.filter("proc_dt_tm is not null")
    }),
    "clinicalencounter" -> ((df: DataFrame) => {
      df.filter("patientid is not null")
    }),
    "zh_nomenclature" -> ((df: DataFrame) => {
      val list_i9 = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_ICD9", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_i10 = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_ICD10", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_cpt = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CPT", "CLAIM", "ZH_NOMENCLATURE", "SOURCE_VOCABULARY_CD")
      val list_hcpcs = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "HCPCS", "CLAIM", "ZH_NOMENCLATURE", "SOURCE_VOCABULARY_CD")

      val fil = df.filter("source_vocabulary_cd in (" + list_i9 + "," + list_i10 + "," + list_cpt + "," + list_hcpcs + ")")
      fil.withColumn("CODETYPE",
          when(expr("source_vocabulary_cd in (" + list_i9 + ")"), lit("ICD9"))
          .when(expr("source_vocabulary_cd in (" + list_i10 + ")"), lit("ICD10"))
          .when(expr("source_vocabulary_cd in (" + list_cpt + ")"), lit("CPT4"))
          .when(expr("source_vocabulary_cd in (" + list_hcpcs + ")"), lit("HCPCS"))
          .otherwise(null))
        .withColumn("SOURCE_ID", fil("SOURCE_IDENTIFIER"))
        .withColumn("SOURCE_IDENTIFIER", when(fil("SOURCE_VOCABULARY_CD") =!= lit("673967"), fil("SOURCE_IDENTIFIER"))
          .otherwise(null))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("procedure")
      .join(dfs("clinicalencounter"), dfs("procedure")("ENCNTR_ID") === dfs("clinicalencounter")("ENCOUNTERID"), "inner")
      .join(dfs("zh_nomenclature"), Seq("NOMENCLATURE_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("procedure"),
    "LOCALCODE" -> mapFrom("NOMENCLATURE_ID"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PROCEDUREDATE" -> mapFrom("PROC_DT_TM"),
    "LOCALNAME" -> mapFrom("SOURCE_STRING"),
    "ACTUALPROCDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(from_unixtime(unix_timestamp(df("PROC_DT_TM"))) > current_timestamp() ||
        from_unixtime(unix_timestamp(df("PROCEDUREDATE"))) < to_date(lit("1901-01-01")), null)
          .otherwise(df("PROC_DT_TM")))
    }),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALPRINCIPLEINDICATOR" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RANKING_CD") === lit("3310"), lit("Y")).otherwise("N"))
    }),
    "MAPPEDCODE" -> mapFrom("SOURCE_IDENTIFIER"),
    "CODETYPE" -> mapFrom("CODETYPE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("PROCEDUREDATE"), df("LOCALCODE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and mappedcode not like '%, %'")
      .drop("rn")
  }

  beforeJoinExceptions = Map(
    "H667594_CR2" -> Map(
      "procedure" -> ((df: DataFrame) => {
        df.filter("proc_dt_tm is not null")
      }),
      "clinicalencounter" -> ((df: DataFrame) => {
        df.filter("patientid is not null")
      }),
      "zh_nomenclature" -> ((df: DataFrame) => {
        val list_i9 = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_ICD9", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
        val list_i10 = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DIAGNOSIS_ICD10", "DIAGNOSIS", "NOMENCLATURE", "SOURCE_VOCABULARY_CD")
        val list_cpt = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CPT", "CLAIM", "ZH_NOMENCLATURE", "SOURCE_VOCABULARY_CD")
        val list_hcpcs = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "HCPCS", "CLAIM", "ZH_NOMENCLATURE", "SOURCE_VOCABULARY_CD")

        val fil = df.filter("source_vocabulary_cd in (" + list_i9 + "," + list_i10 + "," + list_cpt + "," + list_hcpcs + ")")
        fil.withColumn("CODETYPE",
          when(expr("source_vocabulary_cd in (" + list_i9 + ")"), lit("ICD9"))
            .when(expr("source_vocabulary_cd in (" + list_i10 + ")"), lit("ICD10"))
            .when(expr("source_vocabulary_cd in (" + list_cpt + ")"), lit("CPT4"))
            .when(expr("source_vocabulary_cd in (" + list_hcpcs + ")"), lit("HCPCS"))
            .otherwise(null))
          .withColumn("SOURCE_ID", fil("SOURCE_IDENTIFIER"))
          .withColumn("SOURCE_IDENTIFIER",
            when(fil("SOURCE_VOCABULARY_CD") === lit("823322"), null)
            .when(fil("SOURCE_VOCABULARY_CD") === lit("1231"),
              when(instr(fil("SOURCE_IDENTIFIER"), ".") === lit("4") || expr("source_identifier not like '%.%'"), null)
              .otherwise(fil("SOURCE_IDENTIFIER")))
            .otherwise(fil("SOURCE_IDENTIFIER")))
      })
    )
  )

  mapExceptions = Map(
    ("H667594_CR2", "LOCALPRINCIPLEINDICATOR") -> mapFrom("RANKING_CD"),
    ("H667594_CR2", "CODETYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("SOURCE_VOCABULARY_CD") === lit("1222"), lit("HCPCS"))
        .when(df("SOURCE_VOCABULARY_CD") === lit("1217"), lit("CPT4"))
        .when(df("SOURCE_VOCABULARY_CD") === lit("823322"), null)
        .when(df("SOURCE_VOCABULARY_CD") === lit("1231"),
          when(instr(df("SOURCE_ID"), ".") === lit("4") || expr("source_id not like '%.%'"), null)
          .otherwise(lit("ICD9")))
        .otherwise(null))
    })
  )

}