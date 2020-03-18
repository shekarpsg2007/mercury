package com.humedica.mercury.etl.epic_v2.insurance

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 02/02/2017
  */


class InsuranceHspacctcvglist(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List("hsp_acct_cvg_list",
    "zh_payor",
    "zh_beneplan",
    "coverage")


  columnSelect = Map(
    "hsp_acct_cvg_list" -> List("DISCH_DATE_TIME", "PAT_ID", "LINE", "PRIM_ENC_CSN_ID", "ACCT_FIN_CLASS_C", "ADM_DATE_TIME", "INST_OF_UPDATE", "COVERAGE_ID", "FILEID"),
    "zh_payor" -> List("PAYOR_NAME", "PAYOR_ID"),
    "zh_beneplan" -> List("BENEFIT_PLAN_NAME", "BENEFIT_PLAN_ID"),
    "coverage" -> List("GROUP_NUM", "SUBSCR_NUM", "CVG_TERM_DT", "CVG_EFF_DT", "PAYOR_ID", "PLAN_ID", "COVERAGE_ID", "UPDATE_DATE")
  )

  beforeJoin = Map(
    "coverage" -> ((df: DataFrame) => {
      val dedup = bestRowPerGroup(List("COVERAGE_ID"), "UPDATE_DATE")(df)
      dedup.filter("COVERAGE_ID is not null")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("hsp_acct_cvg_list")
      .join(dfs("coverage"), Seq("COVERAGE_ID"), "left_outer")
      .join(dfs("zh_payor"), Seq("PAYOR_ID"), "left_outer")
      .join(dfs("zh_beneplan"), dfs("coverage")("PLAN_ID") === dfs("zh_beneplan")("BENEFIT_PLAN_ID"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("hsp_acct_cvg_list"),
    "INS_TIMESTAMP" -> cascadeFrom(Seq("DISCH_DATE_TIME", "ADM_DATE_TIME", "INST_OF_UPDATE")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "GROUPNBR" -> mapFrom("GROUP_NUM"),
    "INSURANCEORDER" -> mapFrom("LINE"),
    "POLICYNUMBER" -> mapFrom("SUBSCR_NUM"),
    "ENCOUNTERID" -> mapFrom("PRIM_ENC_CSN_ID"),
    "ENROLLENDDT" -> mapFrom("CVG_TERM_DT"),
    "ENROLLSTARTDT" -> mapFrom("CVG_EFF_DT"),
    "PLANTYPE" -> mapFrom("ACCT_FIN_CLASS_C", nullIf = Seq("-1")),
    "PAYORCODE" -> mapFrom("PAYOR_ID"),
    "PAYORNAME" -> mapFrom("PAYOR_NAME"),
    "PLANCODE" -> mapFrom("PLAN_ID"),
    "PLANNAME" -> mapFrom("BENEFIT_PLAN_NAME")
  )


  afterMap = (df: DataFrame) => {
    bestRowPerGroup(List("PATIENTID", "INS_TIMESTAMP"), "FILEID")(df)
  }

}