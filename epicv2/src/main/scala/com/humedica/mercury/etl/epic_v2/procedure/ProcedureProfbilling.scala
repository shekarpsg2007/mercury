package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Created by bhenriksen on 2/16/17.
 */


class ProcedureProfbilling(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.claim.ClaimProfbillingtemptable", "encountervisit:epic_v2.clinicalencounter.ClinicalencounterEncountervisit",
    "zh_clarity_edg", "cdr.map_predicate_values")

  columnSelect=Map(
    "encountervisit" -> List("ENCOUNTERID", "PATIENTID"))


  beforeJoin = Map(
  //"cdr.map_predicate_values" -> ((df: DataFrame) => {
  //  val fil = df.filter("GROUPID = '" + config(GROUP) + "' AND DATA_SRC = 'PAT_ENC_DX' AND ENTITY = 'DIAGNOSIS' AND TABLE_NAME = 'PAT_ENC_DX' AND COLUMN_NAME = 'DX_ID'" +
  //    " AND CLIENT_DS_ID = '" + config(CLIENT_DS_ID) + "'")
  //  fil.select("COLUMN_VALUE")
  //}),
    "temptable" -> ((df: DataFrame) => {
      df.repartition(200)
    }),
    "encountervisit" -> ((df: DataFrame) => {
      df.repartition(200)
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("encountervisit"), dfs("temptable")("PAT_ENC_CSN_ID") === dfs("encountervisit")("ENCOUNTERID"), "left_outer")
      .join(dfs("zh_clarity_edg"), dfs("temptable")("LOCALCODE") === dfs("zh_clarity_edg")("DX_ID"), "left_outer")
   //   .join(dfs("cdr.map_predicate_values"), dfs("temptable")("LOCALCODE") === dfs("cdr.map_predicate_values")("COLUMN_VALUE"), "left_outer")
      .withColumn("INCL_VAL", when(coalesce(dfs("temptable")("PAT_ENC_CSN_ID"), lit("-1")) =!= "-1" && coalesce(dfs("encountervisit")("ENCOUNTERID"),lit("-1")) === "-1", "1").otherwise(null))
  }

  map = Map(
    "DATASRC" -> literal("profbilling"),
    //"ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      val include_encid = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PROFBILLING", "CLINICALENCOUNTER", "PROFBILLING_TXN", "INCLUDE")
      df.withColumn(col, when(df("PAT_ENC_CSN_ID").isNotNull && df("PAT_ENC_CSN_ID") =!= lit("-1"), df("PAT_ENC_CSN_ID"))
        .when(coalesce(df("PAT_ENC_CSN_ID"),lit("-1")) === lit("-1") && lit("'Y'") === include_encid,
          concat(lit("profbilling."), coalesce(df("PAT_ID"), df("PATIENTID")), lit("."), df("TXN_TX_ID")))
        .otherwise(null))
    }),
    "PATIENTID" -> cascadeFrom(Seq("PAT_ID", "PATIENTID")),
    "SOURCEID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat(lit("profbilling."), coalesce(df("PAT_ID"), df("PATIENTID")), lit("."), df("TXN_TX_ID")))
    }),
    "PERFORMINGPROVIDERID" -> mapFrom("PERFORMING_PROV_ID", nullIf=Seq("-1")),
    "PROCEDUREDATE" -> mapFrom("ORIG_SERVICE_DATE"),
    "ACTUALPROCDATE" -> mapFrom("ORIG_SERVICE_DATE"),
    "LOCALCODE" -> mapFrom("CPT_CODE"),
    "CODETYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("CPT_CODE").rlike("^[0-9]{4}[0-9A-Z]$"), "CPT4")
        .when(df("CPT_CODE").rlike("^[A-Z]{1,1}[0-9]{4}$"), "HCPCS")
        .when(df("CPT_CODE").rlike("^[0-9]{2,2}\\.[0-9]{1,2}$"), "ICD9")
        .otherwise(null))
    }),
    "MAPPEDCODE" -> mapFromRequiredLength("CPT_CODE", 5),
    "LOCALBILLINGPROVIDERID" -> mapFrom("BILLING_PROVIDER_ID", nullIf=Seq("-1")),
    "INCL_VAL" -> mapFrom("INCL_VAL")
  )

  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("ENCOUNTERID"),df1("PATIENTID"),df1("SOURCEID"),df1("PERFORMINGPROVIDERID"),df1("PROCEDUREDATE"),
      df1("LOCALCODE"), df1("CODETYPE"), df1("MAPPEDCODE"), df1("LOCALBILLINGPROVIDERID")).orderBy(df1("POST_DATE").desc)
    val addColumn = df1.withColumn("procedure_row", row_number.over(groups))
                     // .withColumn("incl_val", when(coalesce(df1("PAT_ENC_CSN_ID"), lit("-1")) =!= "-1" && coalesce(df1("ENCOUNTERID"),lit("-1")) === "-1", "1").otherwise(null))
    addColumn.filter("procedure_row = 1 and LOCALCODE is not null" +
      " and PATIENTID is not null AND PROCEDUREDATE is not null and INCL_VAL is null and CODETYPE in ('HCPCS', 'CPT4', 'REV')")
  }
}


// TEST
// val p = new ProcedureProfbilling(cfg) ; val pro = build(p) ; pro.show ; pro.count; pro.select("ENCOUNTERID").distinct.count
