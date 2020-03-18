package com.humedica.mercury.etl.epic_v2.insurance

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

class InsuranceEncountervisit(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("encountervisit",
    "zh_payor",
    "zh_clarity_epp",
    "coverage",
    "coverage_2",
    "zh_claritydept",
    "cdr.map_predicate_values")


  columnSelect = Map(
    "encountervisit" -> List("CHECKIN_TIME", "PAT_ID", "PAT_ENC_CSN_ID", "VISIT_FC", "VISIT_EPM_ID", "COVERAGE_ID", "ED_ARRIVAL_TIME",
      "VISIT_EPP_ID", "enc_type_c", "appt_status_c", "disch_disp_c", "visit_epm_id", "visit_epp_id", "DEPARTMENT_ID", "EFFECTIVE_DEPT_ID",
      "HOSP_ADMSN_TIME", "CONTACT_DATE", "UPDATE_DATE", "FILEID"),
    "zh_payor" -> List("PAYOR_NAME", "PAYOR_ID"),
    "zh_clarity_epp" -> List("BENEFIT_PLAN_NAME", "BENEFIT_PLAN_ID"),
    "coverage" -> List("GROUP_NUM", "SUBSCR_NUM", "COVERAGE_ID", "UPDATE_DATE", "PAYOR_ID", "PLAN_ID"),
    "coverage_2" -> List("CVG_ID", "FINANCIAL_CLASS_C", "UPDATE_DATE"),
    "zh_claritydept" -> List("REV_LOC_ID", "DEPARTMENT_ID")
  )

  beforeJoin = Map(
    "encountervisit" -> ((df: DataFrame) => {
      val list_disch_disp_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "DISCH_DISP_C")
      val list_exclude_encvisit = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "INSURANCE", "ENCOUNTERVISIT", "EXCLUDE")
      val fil = df.filter("(PAT_ID is not null and PAT_ID <> '-1') and " +
        "(enc_type_c is null or enc_type_c not in ('5')) and " +
        "(appt_status_c is null or appt_status_c not in ('3','4','5')) " +
        "and (pat_enc_csn_id is not null and pat_enc_csn_id <> '-1') " +
        "and (disch_disp_c is null or disch_disp_c not in (" + list_disch_disp_c + ")) " +
        "and 'Y' <> (" + list_exclude_encvisit + ")")
      fil.withColumn("EPM_ID", when(fil("VISIT_EPM_ID") === lit("-1"), lit(null)).otherwise(fil("VISIT_EPM_ID")))
        .withColumn("EPP_ID", when(fil("VISIT_EPP_ID") === lit("-1"), lit(null)).otherwise(fil("VISIT_EPP_ID")))
    }),
    "zh_claritydept" -> renameColumn("DEPARTMENT_ID", "DEPARTMENT_ID_zh"),
    "coverage" -> ((df: DataFrame) => {
      val newdf = df.withColumnRenamed("UPDATE_DATE", "UPDATE_DATE_cov").withColumnRenamed("PAYOR_ID", "PAYOR_ID_cov").repartition(1000)
      val groups = Window.partitionBy(newdf("COVERAGE_ID")).orderBy(newdf("UPDATE_DATE_cov").desc)
      val addColumn = newdf.withColumn("rank_coverage", row_number.over(groups))
      addColumn
    }),
    "coverage_2" -> ((df: DataFrame) => {
      val newdf = df.withColumnRenamed("UPDATE_DATE", "UPDATE_DATE_2")
      val groups = Window.partitionBy(newdf("CVG_ID")).orderBy(newdf("UPDATE_DATE_2").desc_nulls_last)
      newdf.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("encountervisit")
      .join(dfs("coverage"), dfs("encountervisit")("COVERAGE_ID") === dfs("coverage")("COVERAGE_ID") && dfs("coverage")("RANK_COVERAGE") === 1, "left_outer")
      .join(dfs("coverage_2"), dfs("encountervisit")("COVERAGE_ID") === dfs("coverage_2")("CVG_ID"), "left_outer")
      .join(dfs("zh_claritydept"), coalesce(dfs("encountervisit")("DEPARTMENT_ID"), dfs("encountervisit")("EFFECTIVE_DEPT_ID")) === dfs("zh_claritydept")("DEPARTMENT_ID_zh"), "left_outer")
      .join(dfs("zh_payor"), coalesce(dfs("encountervisit")("EPM_ID"), dfs("coverage")("PAYOR_ID_cov")) === dfs("zh_payor")("PAYOR_ID"), "left_outer")
      .join(dfs("zh_clarity_epp"), coalesce(dfs("encountervisit")("EPP_ID"), dfs("coverage")("PLAN_ID")) === dfs("zh_clarity_epp")("BENEFIT_PLAN_ID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    df.withColumn("FINCLASS", coalesce(df("VISIT_FC"), df("FINANCIAL_CLASS_C")))
  }

  map = Map(
    "DATASRC" -> literal("encountervisit"),
    "INS_TIMESTAMP" -> cascadeFrom(Seq("CHECKIN_TIME", "ED_ARRIVAL_TIME", "HOSP_ADMSN_TIME", "CONTACT_DATE")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    //"FACILITYID" -> mapFrom("REV_LOC_ID"),
    "GROUPNBR" -> mapFrom("GROUP_NUM"),
    "INSURANCEORDER" -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(df("VISIT_EPM_ID") =!= "-1", "1").otherwise(null))),
    "POLICYNUMBER" -> mapFrom("SUBSCR_NUM"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PLANTYPE" -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(df("FINCLASS").isNotNull, concat(lit(config(CLIENT_DS_ID) + ".epic."), df("FINCLASS"))).otherwise(null))),
    "PAYORCODE" -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(df("VISIT_EPM_ID").isNotNull && df("VISIT_EPM_ID") =!= lit("-1"), df("VISIT_EPM_ID"))
        .when(df("PAYOR_ID_cov").isNotNull && df("PAYOR_ID_cov") =!= lit("-1"), df("PAYOR_ID_cov"))
        .otherwise(lit(null)))),
    "PAYORNAME" -> mapFrom("PAYOR_NAME"),
    "PLANCODE" -> ((col: String, df: DataFrame) =>
      df.withColumn(col, when(df("VISIT_EPP_ID").isNotNull && df("VISIT_EPP_ID") =!= lit("-1"), df("VISIT_EPP_ID"))
        .when(df("PLAN_ID").isNotNull && df("PLAN_ID") =!= lit("-1"), df("PLAN_ID"))
        .otherwise(lit(null)))),
    "PLANNAME" -> mapFrom("BENEFIT_PLAN_NAME")
  )


  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("ENCOUNTERID")).orderBy(df1("UPDATE_DATE").desc, df1("FILEID").desc)
    val addColumn = df1.withColumn("rownumber", row_number.over(groups))
    addColumn.filter("INS_TIMESTAMP is not null and PLANCODE is not null and PAYORCODE is not null and rownumber = 1")
  }
}