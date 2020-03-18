package com.humedica.mercury.etl.cerner_v2.insurance

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class InsuranceEncplanreltn(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:cerner_v2.clinicalencounter.ClinicalencounterTemptable", "enc_plan_reltn",
    "zh_organization", "zh_health_plan")

  columnSelect = Map(
    "enc_plan_reltn" -> List("ORGANIZATION_ID", "GROUP_NBR", "PRIORITY_SEQ", "SUBS_MEMBER_NBR", "ENCNTR_ID", "UPDT_DT_TM",
      "HEALTH_PLAN_ID", "ACTIVE_STATUS_DT_TM"),
    "zh_organization" -> List("ORGANIZATION_ID", "ORG_NAME"),
    "temptable" -> List("ENCNTR_ID", "PERSON_ID", "INS_TIMESTAMP", "PERSON_ID", "FINANCIAL_CLASS_CD", "ROWNUMBER", "ACTIVE_IND"),
    "zh_health_plan" -> List("HEALTH_PLAN_ID", "PLAN_NAME", "PLAN_TYPE_CD")
  )

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      df.filter("rownumber = 1 and active_ind <> '0'")
        .withColumn("FINANCIAL_CLASS_CD", when(df("FINANCIAL_CLASS_CD") === lit("0"), null).otherwise(df("FINANCIAL_CLASS_CD")))
    }),
    "zh_health_plan" -> ((df: DataFrame) => {
      df.withColumn("PLAN_TYPE_CD", when(df("PLAN_TYPE_CD") === lit("0"), null).otherwise(df("PLAN_TYPE_CD")))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("enc_plan_reltn"), Seq("ENCNTR_ID"), "left_outer")
      .join(dfs("zh_health_plan"), Seq("HEALTH_PLAN_ID"), "left_outer")
      .join(dfs("zh_organization"), Seq("ORGANIZATION_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("enc_plan_reltn"),
    "INS_TIMESTAMP" -> mapFrom("INS_TIMESTAMP"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "GROUPNBR" -> mapFrom("GROUP_NBR"),
    "INSURANCEORDER" -> mapFrom("PRIORITY_SEQ"),
    "POLICYNUMBER" -> mapFrom("SUBS_MEMBER_NBR"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "PLANTYPE" -> cascadeFrom(Seq("FINANCIAL_CLASS_CD", "PLAN_TYPE_CD"), prefix = config(CLIENT_DS_ID) + "."),
    "PAYORCODE" -> mapFrom("HEALTH_PLAN_ID", nullIf = Seq("0")),
    "PAYORNAME" -> mapFrom("PLAN_NAME"),
    "PLANCODE" -> mapFrom("HEALTH_PLAN_ID"),
    "PLANNAME" -> mapFrom("PLAN_NAME")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("ins_timestamp is not null")
    val groups = Window.partitionBy(fil("PATIENTID"), fil("INS_TIMESTAMP"), fil("ENCOUNTERID"), fil("PAYORCODE"))
      .orderBy(fil("UPDT_DT_TM").desc_nulls_last, fil("ACTIVE_STATUS_DT_TM").desc_nulls_last)
    fil.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and patientid is not null and payorcode is not null and plantype is not null")
  }

  mapExceptions = Map(
    ("H416989_CR2", "PAYORCODE") -> mapFrom("ORGANIZATION_ID", nullIf = Seq("0")),
    ("H416989_CR2", "PAYORNAME") -> mapFrom("ORG_NAME"),
    ("H416989_CR2", "PLANCODE") -> mapFrom("ORGANIZATION_ID"),
    ("H416989_CR2", "PLANNAME") -> mapFrom("ORG_NAME")
  )

}