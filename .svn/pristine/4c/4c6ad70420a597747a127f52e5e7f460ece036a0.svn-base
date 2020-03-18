package com.humedica.mercury.etl.epic_v2.claim


import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 01/27/2017
 */


class ClaimInptbillingacct(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_orgres",
    "inptbilling_acct",
    "clinicalencounter:epic_v2.clinicalencounter.ClinicalencounterEncountervisit",
    "excltxn:epic_v2.claim.ClaimExcltxn",
    "inptbilling_txn",
    "zh_procdictionary",
    "zh_beneplan",
    "zh_payor",
    "zh_cl_ub_rev_code",
    "hsp_acct_pat_csn",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "inptbilling_txn" -> List("ORIG_REV_TX_ID", "TX_TYPE_HA_C", "TX_ID", "HCPCS_CODE", "CPT_CODE", "UB_REV_CODE_ID", "TX_POST_DATE", "PLACE_OF_SVC_ID", "HSP_ACCOUNT_ID", "SERVICE_DATE", "MODIFIERS",
      "TX_AMOUNT", "COST", "QUANTITY", "PERFORMING_PROV_ID", "PAT_ENC_CSN_ID", "BILLING_PROV_ID", "PROC_ID"),
    "inptbilling_acct" -> List("CODING_STATUS_C", "HSP_ACCOUNT_ID", "INST_OF_UPDATE", "PRIMARY_PLAN_ID", "PRIMARY_PAYOR_ID", "PAT_ID","PRIM_ENC_CSN_ID"),
    "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID"),
    "zh_procdictionary" -> List("PROC_ID"),
    "zh_beneplan" -> List("BENEFIT_PLAN_ID"),
    "zh_payor" -> List("PAYOR_ID"),
    "zh_cl_ub_rev_code" -> List("ub_rev_code_id", "revenue_code"),
    "zh_orgres" -> List("POS_ID", "POS_TYPE_C"),
    "hsp_acct_pat_csn" -> List("HSP_ACCOUNT_ID","PAT_ID","PAT_ENC_CSN_ID","LINE","FILEID")
  )

  beforeJoin = Map(
    "inptbilling_txn" -> ((df: DataFrame) => {
      val localcptmod = df.withColumn("LOCALCPTMOD1", when(df("HCPCS_CODE").rlike("^[A-Z]{1,1}[0-9]{4}[A-Z]{2}$"), substring(df("HCPCS_CODE"), 6, 2)))
      val mappedcptmod = localcptmod.withColumn("MAPPEDCPTMOD_1", regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 1))
      val mapcptmod2 = mappedcptmod.withColumn("MAPPEDCPTMOD_2", when(isnull(regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 2)) || (regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 2) === ""), null)
        .otherwise(regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 2)))
      val mapcptmod3 = mapcptmod2.withColumn("MAPPEDCPTMOD_3", when(isnull(regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 3)) || (regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 3) === ""), null)
        .otherwise(regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 3)))
      val mapcptmod4 = mapcptmod3.withColumn("MAPPEDCPTMOD_4", when(isnull(regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 4)) || (regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 4) === ""), null)
        .otherwise(regexp_extract(df("MODIFIERS"), "(^[^,]*),?([^,]*),?([^,]*),?([^,]*)", 4)))
      val fil = mapcptmod4.filter("(ORIG_REV_TX_ID is null or ORIG_REV_TX_ID = '-1') and TX_TYPE_HA_C = '1'").drop("ORIG_REV_TX_ID")
      val gtt = table("excltxn")
      val zh_rev_code = table("zh_cl_ub_rev_code").withColumnRenamed("UB_REV_CODE_ID", "UB_REV_CODE_ID_zh")
      val joined = fil.join(gtt, fil("TX_ID") === gtt("ORIG_REV_TX_ID"), "left_outer")
        .join(zh_rev_code, fil("UB_REV_CODE_ID") === zh_rev_code("UB_REV_CODE_ID_zh"), "left_outer")
      val joined1 = joined.filter("ORIG_REV_TX_ID is null").withColumn("MAPPEDCODE"
        ,when(df("HCPCS_CODE").startsWith("CPT") && (length(df("HCPCS_CODE")) === 8 ), substring(df("HCPCS_CODE"),4,9999))
          .when(length(df("HCPCS_CODE")) === 5, df("HCPCS_CODE"))
          .when(length(df("CPT_CODE")) === 5, df("CPT_CODE"))
          .when(length(df("UB_REV_CODE_ID")) === 3, df("UB_REV_CODE_ID"))
          .otherwise(null))
      val groups = Window.partitionBy(joined1("TX_ID")).orderBy(joined1("TX_POST_DATE").desc)
      joined1.withColumn("txn_rownumber", row_number.over(groups))
        .withColumn("UB_REV_CODE_ID_C", coalesce(joined1("REVENUE_CODE"), joined1("UB_REV_CODE_ID")))

    }),
    "inptbilling_acct" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID")).orderBy(df("INST_OF_UPDATE").desc)
      df.withColumn("acct_rownumber", row_number.over(groups)).withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_acct")
    }),
    "clinicalencounter" -> ((df: DataFrame) => {
      df.withColumnRenamed("PATIENTID", "PATIENTID_ce")
        .withColumnRenamed("ENCOUNTERID", "ENCOUNTERID_ce")
    }),
    "hsp_acct_pat_csn" -> ((df: DataFrame) =>{
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID"),df("PAT_ID")).orderBy(df("LINE").asc,df("FILEID").desc)
      df.withColumn("hsp_rownumber", row_number.over(groups)).withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_hsp")
          .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_hsp")
          .withColumnRenamed("PAT_ID","PAT_ID_hsp")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("inptbilling_txn")
      .join(dfs("inptbilling_acct")
        .join(dfs("zh_beneplan"), dfs("zh_beneplan")("BENEFIT_PLAN_ID") === dfs("inptbilling_acct")("PRIMARY_PLAN_ID"), "left_outer")
        .join(dfs("zh_payor"), dfs("zh_payor")("PAYOR_ID") === dfs("inptbilling_acct")("PRIMARY_PAYOR_ID"), "left_outer")
        ,dfs("inptbilling_txn")("HSP_ACCOUNT_ID") === dfs("inptbilling_acct")("HSP_ACCOUNT_ID_acct") && (dfs("inptbilling_acct")("acct_rownumber") === 1), "left_outer")
      .join(dfs("hsp_acct_pat_csn"), dfs("inptbilling_txn")("HSP_ACCOUNT_ID") === dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_hsp") && (dfs("hsp_acct_pat_csn")("hsp_rownumber") === 1), "left_outer")
      .join(dfs("clinicalencounter"), dfs("clinicalencounter")("ENCOUNTERID_ce") === coalesce(
          when(dfs("inptbilling_txn")("PAT_ENC_CSN_ID") === "-1", null).otherwise(dfs("inptbilling_txn")("PAT_ENC_CSN_ID")),
          when(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID") === "-1", null).otherwise(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID")),
          when(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp") === "-1", null).otherwise(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp"))
      ), "left_outer")
      .join(dfs("zh_procdictionary"), Seq("PROC_ID"), "left_outer")
      .join(dfs("zh_orgres"), dfs("zh_orgres")("POS_ID") === dfs("inptbilling_txn")("PLACE_OF_SVC_ID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
    df.filter("txn_rownumber = 1 AND (coalesce(CODING_STATUS_C, 'X') in (" + list_coding_status_c + ") or ('NO_MPV_MATCHES') in (" + list_coding_status_c + "))")
  }

  map = Map(
    "DATASRC" -> literal("inptbilling_acct"),
    "CLAIMID" -> mapFrom("TX_ID", prefix = "inptbilling."),
    "PATIENTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ID_hsp") === "-1", null).otherwise(df("PAT_ID_hsp")),
        when(df("PATIENTID_ce") === "-1", null).otherwise(df("PATIENTID_ce")),
        when(df("PAT_ID") === "-1", null).otherwise(df("PAT_ID"))
      ))
    }),
    "SERVICEDATE" -> mapFrom("SERVICE_DATE"),
    "MAPPEDCPTMOD1" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MAPPEDCPTMOD_1")) <= "2", df("MAPPEDCPTMOD_1")).otherwise(df("LOCALCPTMOD1")))
    }),
    "MAPPEDCPTMOD2" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MAPPEDCPTMOD_2")) <= "2", df("MAPPEDCPTMOD_2")).otherwise(null))
    }),
    "MAPPEDCPTMOD3" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MAPPEDCPTMOD_3")) <= "2", df("MAPPEDCPTMOD_3")).otherwise(null))
    }),
    "MAPPEDCPTMOD4" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("MAPPEDCPTMOD_4")) <= "2", df("MAPPEDCPTMOD_4")).otherwise(null))
    }),
    "LOCALREV" ->  ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("REVENUE_CODE"), df("UB_REV_CODE_ID")))
      }),
    "LOCALBILLINGPROVIDERID" -> mapFrom("BILLING_PROV_ID", nullIf = Seq("-1")),
    "LOCALCPT" -> cascadeFrom(Seq("HCPCS_CODE", "CPT_CODE")),
    "CLAIMPROVIDERID" -> mapFrom("PERFORMING_PROV_ID", nullIf = Seq("-1")),
    "MAPPEDCPT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when((df("HCPCS_CODE") startsWith  "CPT") && (length(df("HCPCS_CODE")) === 8), substring(df("HCPCS_CODE"), 4, 9999))
        .when(length(df("HCPCS_CODE")) === 5, df("HCPCS_CODE"))
        .when(df("HCPCS_CODE").rlike("^[A-Z]{1,1}[0-9]{4}[A-Z]{2}$"), substring(df("HCPCS_CODE"), 1, 5))
        .when(length(df("CPT_CODE")) === 5, df("CPT_CODE")).otherwise(null))
    }),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ENC_CSN_ID") === "-1", null).otherwise(df("PAT_ENC_CSN_ID")),
        when(df("PRIM_ENC_CSN_ID") === "-1", null).otherwise(df("PRIM_ENC_CSN_ID")),
        when(df("PAT_ENC_CSN_ID_hsp") === "-1", null).otherwise(df("PAT_ENC_CSN_ID_hsp"))
      ))
    }),
    "SOURCEID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(
        when(df("PAT_ENC_CSN_ID") === "-1", null).otherwise(df("PAT_ENC_CSN_ID")),
        when(df("PRIM_ENC_CSN_ID") === "-1", null).otherwise(df("PRIM_ENC_CSN_ID")),
        when(df("PAT_ENC_CSN_ID_hsp") === "-1", null).otherwise(df("PAT_ENC_CSN_ID_hsp"))
      ))
    }),
    "POS" -> ((col: String, df: DataFrame) => {
      val include_pos = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "INPTBILLING_ACCT", "CLAIM", "ZH_ORGRES", "INCLUDE")
      df.withColumn(col, when(lit("'Y'") === include_pos && df("POS_TYPE_C") =!= lit("-1"), df("POS_TYPE_C")).otherwise(null))
    })
  )

  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val addColumn = df1.withColumn("MAPPEDREV", when(length(df1("LOCALREV")) isin (1,2,3,4),lpad(df1("LOCALREV"),4,"0")).otherwise(null))
   // val groups = Window.partitionBy(addColumn("HSP_ACCOUNT_ID"), addColumn("TX_ID"), addColumn("MAPPEDCPT"), addColumn("PROC_ID"), addColumn("SERVICEDATE"), addColumn("LOCALBILLINGPROVIDERID"), addColumn("MODIFIERS"))
   //   .orderBy(addColumn("TX_POST_DATE").desc)
    val groups = Window.partitionBy(addColumn("ENCOUNTERID"),addColumn("MAPPEDCPT"),addColumn("LOCALREV"),addColumn("PROC_ID"),addColumn("SERVICE_DATE")
      ,addColumn("LOCALBILLINGPROVIDERID"),addColumn("MODIFIERS")).orderBy(addColumn("TX_POST_DATE").desc)
    val addColumn1 = addColumn.withColumn("rn", row_number.over(groups))
    //val groups2 = Window.partitionBy(addColumn1("HSP_ACCOUNT_ID"), addColumn("TX_ID"), addColumn1("MAPPEDCPT"), addColumn1("PROC_ID"), addColumn1("SERVICEDATE"), addColumn1("LOCALBILLINGPROVIDERID"), addColumn1("MODIFIERS"))
    val groups2 = Window.partitionBy(addColumn1("ENCOUNTERID"),addColumn1("MAPPEDCPT"),addColumn1("LOCALREV"),addColumn1("PROC_ID"),addColumn1("SERVICE_DATE")
      ,addColumn("LOCALBILLINGPROVIDERID"),addColumn("MODIFIERS"))
    val out = addColumn1.withColumn("CHARGE",round(sum(addColumn1("TX_AMOUNT")).over(groups2),2).cast("String"))
      .withColumn("COSTAMOUNT", sum(addColumn1("COST")).over(groups2))
      .withColumn("QUANTITY",sum(addColumn1("QUANTITY")).over(groups2).cast("String"))
      .withColumn("incl_val", when(df("ENCOUNTERID").isNotNull && coalesce(df("ENCOUNTERID_ce"),lit("-1")) === "-1", "1").otherwise(null))
    out.filter("rn = 1 and CLAIMID is not null and PATIENTID is not null and SERVICEDATE is not null and incl_val is null").drop("rn")
  }

  mapExceptions = Map(
    ("H406239_EP2", "POS") -> mapFrom("POS_TYPE_C", nullIf=Seq("-1"))
  )

}

// val c = new ClaimInptbillingacct(cfg) ; val cl = build(c); cl.show; cl.count
