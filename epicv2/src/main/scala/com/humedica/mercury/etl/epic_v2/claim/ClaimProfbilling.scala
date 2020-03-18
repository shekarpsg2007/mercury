package com.humedica.mercury.etl.epic_v2.claim

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
 
/**
 * Auto-generated on 01/27/2017
 */
 
 
class ClaimProfbilling(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {
 
  tables = List("temptable:epic_v2.claim.ClaimProfbillingtemptable", "encountervisit:epic_v2.clinicalencounter.ClinicalencounterEncountervisit", "zh_clarity_edg"
    ,"cdr.map_predicate_values")
 
 
 
  columnSelect=Map(
    "encountervisit" -> List("ENCOUNTERID", "PATIENTID")
  )
  
  beforeJoin = Map(
    "encountervisit" -> ((df: DataFrame) => {
      df.withColumnRenamed("ENCOUNTERID", "ENCOUNTERID_ev")
        .withColumnRenamed("PATIENTID", "PATIENTID_ev")
    })
  )
 
  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("encountervisit"), dfs("temptable")("PAT_ENC_CSN_ID") === dfs("encountervisit")("ENCOUNTERID_ev"), "left_outer")
      .join(dfs("zh_clarity_edg"), dfs("temptable")("LOCALCODE") === dfs("zh_clarity_edg")("DX_ID"), "left_outer")
  }
 
  map = Map(
    "DATASRC" -> literal("profbilling"),
    "CLAIMID" -> mapFrom("TXN_TX_ID"),
    "PATIENTID" -> cascadeFrom(Seq("PAT_ID", "PATIENTID_ev")),
    "SOURCEID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat(lit("profbilling."), coalesce(df("PAT_ID"), df("PATIENTID_ev")), lit("."), df("TXN_TX_ID")))
    }),
    "CHARGE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DETAIL_TYPE") === "1" && (df("AMOUNT") leq 99999999.99), df("AMOUNT")).otherwise(null).cast("String"))
    }),
    "QUANTITY" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, df("PROCEDURE_QUANTITY").cast("String"))
    }),
    "LOCALCPT" -> mapFrom("CPT_CODE"),
    "LOCALCPTMOD1" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MODIFIER_ONE")) <= "2", df("MODIFIER_ONE")).otherwise(null))
    }),
    "LOCALCPTMOD2" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MODIFIER_TWO")) <= "2", df("MODIFIER_TWO")).otherwise(null))
    }),
    "LOCALCPTMOD3" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MODIFIER_THREE")) <= "2", df("MODIFIER_THREE")).otherwise(null))
    }),
    "LOCALCPTMOD4" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MODIFIER_FOUR")) <= "2", df("MODIFIER_FOUR")).otherwise(null))
    }),
    "SERVICEDATE" -> mapFrom("ORIG_SERVICE_DATE"),
    "CLAIMPROVIDERID" -> mapFrom("PERFORMING_PROV_ID", nullIf=Seq("-1")),
    "MAPPEDCPT" -> mapFromRequiredLength("CPT_CODE", 5),
    "MAPPEDCPTMOD1" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MODIFIER_ONE")) <= "2", df("MODIFIER_ONE")).otherwise(null))
    }),
    "MAPPEDCPTMOD2" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MODIFIER_TWO")) <= "2", df("MODIFIER_TWO")).otherwise(null))
    }),
    "MAPPEDCPTMOD3" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MODIFIER_THREE")) <= "2", df("MODIFIER_THREE")).otherwise(null))
    }),
    "MAPPEDCPTMOD4" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MODIFIER_FOUR")) <= "2", df("MODIFIER_FOUR")).otherwise(null))
    }),
    "LOCALBILLINGPROVIDERID" -> mapFrom("BILLING_PROVIDER_ID", nullIf=Seq("-1")),
    "POS" -> ((col: String, df: DataFrame) => {
      val include_pos = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "PROFBILLING", "CLAIM", "ZH_ORGRES", "INCLUDE")
      df.withColumn(col, when(lit("'Y'") === include_pos && df("POS_TYPE_C") =!= lit("-1"), df("POS_TYPE_C")).otherwise(null))
    }),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      val include_encid = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "PROFBILLING", "CLINICALENCOUNTER", "PROFBILLING_TXN", "INCLUDE")
      df.withColumn(col, when(df("PAT_ENC_CSN_ID").isNotNull && df("PAT_ENC_CSN_ID") =!= lit("-1"), df("PAT_ENC_CSN_ID"))
                        .when((df("PAT_ENC_CSN_ID").isNull || df("PAT_ENC_CSN_ID") === lit("-1")) && lit("'Y'") === include_encid,
                            concat_ws("", lit("profbilling."),coalesce(df("PAT_ID"),df("PATIENTID_ev")),lit("."),df("TXN_TX_ID")))
                        .otherwise(null))
    })
  )
 
  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("CLAIMID"), df1("ENCOUNTERID"), df1("PATIENTID"), df1("CHARGE"), df1("QUANTITY"), df1("LOCALCPT"), df1("LOCALCPTMOD1"),
      df1("LOCALCPTMOD2"), df1("LOCALCPTMOD3"), df1("LOCALCPTMOD4"), df1("SERVICEDATE"), df1("CLAIMPROVIDERID"), df1("MAPPEDCPT"), df1("MAPPEDCPTMOD1"), df1("MAPPEDCPTMOD2"),
      df1("MAPPEDCPTMOD3"), df1("MAPPEDCPTMOD4"), df1("LOCALBILLINGPROVIDERID")).orderBy(df1("POST_DATE").desc)
    val addColumn = df1.withColumn("claim_row", row_number.over(groups))
      .withColumn("incl_val", when(coalesce(df1("PAT_ENC_CSN_ID"), lit("-1")) =!= "-1" && coalesce(df1("ENCOUNTERID_ev"),lit("-1")) === "-1", "1").otherwise(null))
    addColumn.filter("claim_row = 1 and CLAIMID is not null and PATIENTID is not null AND SERVICEDATE is not null and incl_val is null")
  }
}

// val c = new ClaimProfbilling(cfg) ; val cl = build(c) ; cl.show ; cl.count
