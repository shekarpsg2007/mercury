package com.humedica.mercury.etl.epic_v2.claim

import com.humedica.mercury.etl.core.engine.Constants.{CLIENT_DS_ID, GROUP}
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Functions

/**
  * Created by cdivakaran on 5/25/17.
  */
class ClaimProfbillingtemptable (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  cacheMe = true

  tables = List("profbilling_txn", "ZH_V_EDG_HX_ICD10", "zh_edg_curr_icd10", "ZH_V_EDG_HX_ICD9", "zh_edg_curr_icd9", "profbilling_inv", "zh_orgres"
                ,"cdr.map_predicate_values" //"encountervisit:epic_v2.clinicalencounter.ClinicalencounterEncountervisit"
  )

  columns = List("TXN_TX_ID", "INV_TX_ID", "PAT_ID", "CPT_CODE", "ORIG_SERVICE_DATE", "MODIFIER_ONE", "MODIFIER_TWO", "MODIFIER_THREE", "MODIFIER_FOUR",
    "DETAIL_TYPE", "AMOUNT", "PERFORMING_PROV_ID", "PAT_ENC_CSN_ID", "BILLING_PROVIDER_ID", "PROCEDURE_QUANTITY", "POST_DATE", "LOCALCODE", "LOCALNAME",
    "STANDARDCODE", "STANDARDNAME", "POS", "INCL_VAL", "DROPS_ICD9", "DROPS_ICD10","POS_TYPE_C","DEPTID","LOC_ID","PROV_ID","REFERRING_PROV_ID"
    ,"CODE_9a","CODE_9b","CODE_10a","CODE_10b")

  columnSelect = Map(
    "profbilling_txn" -> List("DETAIL_TYPE", "TX_ID", "DX_ONE_ID", "DX_TWO_ID", "DX_THREE_ID", "DX_FOUR_ID", "DX_FIVE_ID", "DX_SIX_ID", "ORIG_SERVICE_DATE", "CPT_CODE", "MODIFIER_ONE"
      , "MODIFIER_TWO", "MODIFIER_THREE", "MODIFIER_FOUR", "AMOUNT", "PERFORMING_PROV_ID", "PAT_ENC_CSN_ID", "BILLING_PROVIDER_ID", "PROCEDURE_QUANTITY", "POST_DATE","TDL_EXTRACT_DATE"
      , "DEPT_ID"),
    "profbilling_inv" -> List("TX_ID", "PAT_ID", "INV_STATUS_C", "HX_DATETIME", "POS_ID", "DEPARTMENT_ID","LOC_ID","PROV_ID","REFERRING_PROV_ID"),
    "ZH_V_EDG_HX_ICD10" -> List("DX_ID", "EFF_START_DATE", "EFF_END_DATE", "CODE"),
    "zh_edg_curr_icd10" -> List("DX_ID", "CODE"),
    "ZH_V_EDG_HX_ICD9" -> List("DX_ID", "EFF_START_DATE", "EFF_END_DATE", "CODE"),
    "zh_edg_curr_icd9" -> List("DX_ID", "CODE")
  )

  beforeJoin = Map(
    "profbilling_txn" -> ((df: DataFrame) => {
      val fil = df.filter("(DETAIL_TYPE is null or DETAIL_TYPE <> '10') and TX_ID is not null and TX_ID <> '-1'")
      val groups = Window.partitionBy(fil("TX_ID"))
        .orderBy(when(fil("DETAIL_TYPE") === "1", 1).otherwise(0).desc, fil("TDL_EXTRACT_DATE").desc)
      val addColumn = fil.withColumn("rownumber_txn", row_number.over(groups))
      val fil2 = addColumn.filter("rownumber_txn = 1").drop("rownumber_txn")
      val fpiv = unpivot(
        Seq("DX_ONE_ID", "DX_TWO_ID", "DX_THREE_ID", "DX_FOUR_ID", "DX_FIVE_ID", "DX_SIX_ID"),
        Seq("DX_ONE_ID", "DX_TWO_ID", "DX_THREE_ID", "DX_FOUR_ID", "DX_FIVE_ID", "DX_SIX_ID"), typeColumnName = "LOCALNAME")
      fpiv("LOCALCODE", fil2)
    }),
    "ZH_V_EDG_HX_ICD10" -> ((df: DataFrame) => {
      df.withColumnRenamed("DX_ID", "DX_ID_ICD10a").withColumnRenamed("CODE", "CODE_10a")
        .withColumnRenamed("EFF_START_DATE", "EFF_START_DATE_10").withColumnRenamed("EFF_END_DATE", "EFF_END_DATE_10")
    }),
    "zh_edg_curr_icd10" -> ((df: DataFrame) => {
      df.withColumnRenamed("DX_ID", "DX_ID_ICD10b").withColumnRenamed("CODE", "CODE_10b")
    }),
    "ZH_V_EDG_HX_ICD9" -> ((df: DataFrame) => {
      df.withColumnRenamed("DX_ID", "DX_ID_ICD9a").withColumnRenamed("CODE", "CODE_9a")
        .withColumnRenamed("EFF_START_DATE", "EFF_START_DATE_9").withColumnRenamed("EFF_END_DATE", "EFF_END_DATE_9")
    }),
    "zh_edg_curr_icd9" -> ((df: DataFrame) => {
      df.withColumnRenamed("DX_ID", "DX_ID_ICD9b").withColumnRenamed("CODE", "CODE_9b")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("profbilling_txn")
      .join(dfs("ZH_V_EDG_HX_ICD10"), dfs("profbilling_txn")("LOCALCODE") === dfs("ZH_V_EDG_HX_ICD10")("DX_ID_ICD10a") && (dfs("profbilling_txn")("ORIG_SERVICE_DATE")
        between(dfs("ZH_V_EDG_HX_ICD10")("EFF_START_DATE_10"), dfs("ZH_V_EDG_HX_ICD10")("EFF_END_DATE_10"))), "left_outer")
      .join(dfs("zh_edg_curr_icd10"), dfs("profbilling_txn")("LOCALCODE") === dfs("zh_edg_curr_icd10")("DX_ID_ICD10b"), "left_outer")
      .join(dfs("ZH_V_EDG_HX_ICD9"), dfs("profbilling_txn")("LOCALCODE") === dfs("ZH_V_EDG_HX_ICD9")("DX_ID_ICD9a") && (dfs("profbilling_txn")("ORIG_SERVICE_DATE")
        between(dfs("ZH_V_EDG_HX_ICD9")("EFF_START_DATE_9"), dfs("ZH_V_EDG_HX_ICD9")("EFF_END_DATE_9"))), "left_outer")
      .join(dfs("zh_edg_curr_icd9"), dfs("profbilling_txn")("LOCALCODE") === dfs("zh_edg_curr_icd9")("DX_ID_ICD9b"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val inv = table("profbilling_inv")
    val drop_diag = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPIC", "DIAGNOSIS", "DIAGNOSIS", "LOCALDIAGNOSIS")
    val inv1 = inv.filter("TX_ID is not null and TX_ID <> '-1' AND PAT_ID is not null and PAT_ID <> '-1' and INV_STATUS_C not in ('7','4')")
    val groups1 = Window.partitionBy(inv1("TX_ID")).orderBy(inv1("HX_DATETIME").desc)
    val inv2 = inv1.withColumn("rownumber_inv", row_number.over(groups1)).withColumnRenamed("TX_ID", "TX_ID_inv")
    val orgres = table("zh_orgres")
    val ct_window = Window.partitionBy(df("TX_ID"), df("ORIG_SERVICE_DATE"), df("PAT_ENC_CSN_ID"), df("LOCALCODE"))
    val addColumn = df.withColumn("MAPPED_ICD9", coalesce(df("CODE_9a"), df("CODE_9b")))
      .withColumn("MAPPED_ICD10", coalesce(df("CODE_10a"), df("CODE_10b")))
    val addColumn1 = addColumn.withColumn("DROPS_ICD9",
      when((lit("'Y'") === lit(drop_diag)) && expr("ORIG_SERVICE_DATE >= from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
        when(count(addColumn("MAPPED_ICD9")).over(ct_window).gt(lit(0))
          && count(addColumn("MAPPED_ICD10")).over(ct_window).gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .withColumn("DROPS_ICD10",
        when(lit("'Y'") === lit(drop_diag) && expr("ORIG_SERVICE_DATE < from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
          when(count(addColumn("MAPPED_ICD9")).over(ct_window).gt(lit(0))
            && count(addColumn("MAPPED_ICD10")).over(ct_window).gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
    val fpiv = Functions.unpivot(
      Seq("MAPPED_ICD9", "MAPPED_ICD10"),
      Seq("ICD9", "ICD10"), typeColumnName = "STANDARDNAME", includeNulls=true)
    //val fpiv1 = unpivot()
    val pivot = fpiv("STANDARDCODE", addColumn1)
    pivot.join(inv2
      .join(orgres, Seq("POS_ID"), "left_outer"),
      pivot("TX_ID") === inv2("TX_ID_inv") && (inv2("rownumber_inv") === 1), "left_outer")
   // inv2.join(inv2
   //    .join(orgres, Seq("POS_ID"), "left_outer"),
   //     df("TX_ID") === inv2("TX_ID_inv") && (inv2("rownumber_inv") === 1), "left_outer")
  }

  map = Map(
    "TXN_TX_ID" -> mapFrom("TX_ID"),
    "INV_TX_ID" -> mapFrom("TX_ID_inv"),
    "PAT_ENC_CSN_ID" -> mapFrom("PAT_ENC_CSN_ID", nullIf = Seq("-1")),
    "LOCALCODE" -> mapFrom("LOCALCODE", nullIf = Seq("-1")),
    "POS_TYPE_C" -> mapFrom("POS_TYPE_C"),
    "INCL_VAL" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(coalesce(df("PAT_ENC_CSN_ID"), lit("-1")) =!= "-1"
        ,"1").otherwise(null))
    }),
    "DEPTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("DEPT_ID"), df("DEPARTMENT_ID")))
    }),
    "POS" -> ((col: String, df: DataFrame) => {
      val include_pos = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "PROFBILLING", "CLAIM", "ZH_ORGRES", "INCLUDE")
      df.withColumn(col, when(lit("'Y'") === include_pos && df("POS_TYPE_C") =!= lit("-1"), df("POS_TYPE_C")).otherwise(null))
    })
  )


  mapExceptions = Map(
    ("H704847_EP2", "POS_TYPE_C") -> mapFrom("POS_TYPE_C", nullIf=Seq("-1"))
  )

}

// val c = new ClaimProfbillingtemptable(cfg) ; val cl = build(c) ; cl.show ; cl.count





