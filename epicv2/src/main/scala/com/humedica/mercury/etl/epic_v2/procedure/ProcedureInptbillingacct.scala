package com.humedica.mercury.etl.epic_v2.procedure

  import com.humedica.mercury.etl.core.engine.Constants._
  import com.humedica.mercury.etl.core.engine.EntitySource
  import com.humedica.mercury.etl.core.engine.Functions._
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._
  import com.humedica.mercury.etl.core.engine.Engine


  /**
   * Auto-generated on 02/01/2017
   */

  class ProcedureInptbillingacct(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

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
      "inptbilling_acct" -> List("CODING_STATUS_C", "HSP_ACCOUNT_ID", "INST_OF_UPDATE", "PRIMARY_PLAN_ID", "PRIMARY_PAYOR_ID", "PAT_ID","FILEID","PRIM_ENC_CSN_ID"),
      "clinicalencounter" -> List("ENCOUNTERID", "PATIENTID"),
      "zh_procdictionary" -> List("PROC_ID", "PROC_NAME"),
      "hsp_acct_pat_csn" -> List("HSP_ACCOUNT_ID","PAT_ID","PAT_ENC_CSN_ID","LINE","FILEID"),
      "zh_beneplan" -> List("BENEFIT_PLAN_ID"),
      "zh_payor" -> List("PAYOR_ID"),
      "zh_cl_ub_rev_code" -> List("ub_rev_code_id", "revenue_code"),
      "zh_orgres" -> List("POS_ID", "POS_TYPE_C")
    )

    beforeJoin = Map(
      "inptbilling_txn" -> ((df: DataFrame) => {
        val df1 = df.repartition(600)
        val fil = df1.filter("(ORIG_REV_TX_ID is null or ORIG_REV_TX_ID = '-1') and TX_TYPE_HA_C = '1'").drop("ORIG_REV_TX_ID")
        val gtt = table("excltxn").repartition(600)
        val zh_rev_code = table("zh_cl_ub_rev_code").withColumnRenamed("UB_REV_CODE_ID", "UB_REV_CODE_ID_zh")
        val joined = fil.join(gtt, fil("TX_ID") === gtt("ORIG_REV_TX_ID"), "left_outer")
                        .join(zh_rev_code, fil("UB_REV_CODE_ID") === zh_rev_code("UB_REV_CODE_ID_zh"), "left_outer")
        val joined1 = joined.filter("ORIG_REV_TX_ID is null")
        val groups = Window.partitionBy(joined1("TX_ID")).orderBy(joined1("TX_POST_DATE").desc)
        val out = joined1.withColumn("txn_rownumber", row_number.over(groups))
                           .withColumn("UB_REV_CODE_ID_C", coalesce(joined1("REVENUE_CODE"), joined1("UB_REV_CODE_ID")))
        val fpiv = unpivot(Seq("HCPCS_CODE", "CPT_CODE", "UB_REV_CODE_ID_C"),
          Seq("HCPCS_CODE", "CPT_CODE", "REV_CODE"), typeColumnName = "CODE")
        val fpiv1 = fpiv("MAPPEDCODE", out)
        fpiv1.filter("txn_rownumber = '1'")
      }),
      "inptbilling_acct" -> ((df: DataFrame) => {
        val df1=df.repartition(600)
        val groups = Window.partitionBy(df1("HSP_ACCOUNT_ID")).orderBy(df1("INST_OF_UPDATE").desc)
        df1.withColumn("acct_rownumber", row_number.over(groups)).withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_acct")
             .filter("acct_rownumber = 1")
      }),
        "clinicalencounter" -> ((df: DataFrame) => {
          df.withColumnRenamed("PATIENTID", "PATIENTID_ce")
            .withColumnRenamed("ENCOUNTERID", "ENCOUNTERID_ce")
      }),
      "hsp_acct_pat_csn" -> ((df: DataFrame) => {
        val groups = Window.partitionBy(df("HSP_ACCOUNT_ID"),df("PAT_ID")).orderBy(df("LINE").asc,df("FILEID").desc)
        df.withColumn("hsp_rw", row_number.over(groups))
          .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_hsp")
          .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_hsp")
          .withColumnRenamed("PAT_ID","PAT_ID_hsp")
          .filter("hsp_rw=1")
      })
    )

    join = (dfs: Map[String, DataFrame]) => {
      dfs("inptbilling_txn")
        .join(dfs("inptbilling_acct"),dfs("inptbilling_txn")("HSP_ACCOUNT_ID") === dfs("inptbilling_acct")("HSP_ACCOUNT_ID_acct"), "left_outer")
          .join(dfs("hsp_acct_pat_csn"), dfs("inptbilling_txn")("HSP_ACCOUNT_ID") === dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_hsp"), "left_outer")
          .join(dfs("zh_beneplan"), dfs("zh_beneplan")("BENEFIT_PLAN_ID") === dfs("inptbilling_acct")("PRIMARY_PLAN_ID"), "left_outer")
          .join(dfs("zh_payor"), dfs("zh_payor")("PAYOR_ID") === dfs("inptbilling_acct")("PRIMARY_PAYOR_ID"), "left_outer")
        //.join(dfs("clinicalencounter"), dfs("inptbilling_txn")("PAT_ENC_CSN_ID") === dfs("clinicalencounter")("ENCOUNTERID_ce"), "left_outer")
        .join(dfs("clinicalencounter"), dfs("clinicalencounter")("ENCOUNTERID_ce") === coalesce(
        when(dfs("inptbilling_txn")("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(dfs("inptbilling_txn")("PAT_ENC_CSN_ID")),
        when(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID") === lit("-1"), null).otherwise(dfs("inptbilling_acct")("PRIM_ENC_CSN_ID")),
        when(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hsp"))
      ), "left_outer")
        .join(dfs("zh_procdictionary"), Seq("PROC_ID"), "left_outer")
        .join(dfs("zh_orgres"), dfs("zh_orgres")("POS_ID") === dfs("inptbilling_txn")("PLACE_OF_SVC_ID"), "left_outer")
    }

    afterJoin = (df: DataFrame) => {
      val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
      df.filter("coalesce(CODING_STATUS_C, 'X') in (" + list_coding_status_c + ") or 'NO_MPV_MATCHES' in (" + list_coding_status_c + ")")
    }

    map = Map(
      "DATASRC" -> literal("inptbilling_acct"),
      //"ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID", nullIf = Seq("-1")),
      "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
        df.withColumn(col, coalesce(
          when(df("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID")),
          when(df("PRIM_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID")),
          when(df("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_hsp"))
        ))
      }),
    //  "PATIENTID" -> ((col: String, df: DataFrame) => {
    //    df.withColumn(col, when(coalesce(df("PAT_ID"), lit("-1")) =!= "-1", df("PAT_ID")).otherwise(df("PATIENTID_ce"))
    //  }),
      "PATIENTID" -> ((col: String, df: DataFrame) => {
        df.withColumn(col, coalesce(
          when(df("PAT_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ID_hsp")),
          when(df("PATIENTID_ce") === lit("-1"), null).otherwise(df("PATIENTID_ce")),
          when(df("PAT_ID") === lit("-1"), null).otherwise(df("PAT_ID"))
        ))
      }),
     // "SOURCEID" -> nullValue(),
      "SOURCEID" -> ((col: String, df: DataFrame) => {
        df.withColumn(col, coalesce(
          when(df("PAT_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID")),
          when(df("PRIM_ENC_CSN_ID") === lit("-1"), null).otherwise(df("PRIM_ENC_CSN_ID")),
          when(df("PAT_ENC_CSN_ID_hsp") === lit("-1"), null).otherwise(df("PAT_ENC_CSN_ID_hsp"))
        ))
      }),
      "PERFORMINGPROVIDERID" -> mapFrom("PERFORMING_PROV_ID", nullIf=Seq("-1")),
      "PROCEDUREDATE" -> mapFrom("SERVICE_DATE"),
      "ACTUALPROCDATE" -> mapFrom("SERVICE_DATE"),
      "LOCALCODE" -> mapFrom("PROC_ID"),
      "HOSP_PX_FLAG" -> literal("Y"),
      "CODETYPE" -> ((col: String, df: DataFrame) => {
        df.withColumn(col, when(df("MAPPEDCODE").rlike("^[0-9]{4}[0-9A-Z]$"), "CPT4")
          .when(df("MAPPEDCODE").rlike("^[A-Z]{1,1}[0-9]{4}$"), "HCPCS")
          .when(df("MAPPEDCODE").rlike("^[0-9]{2,2}\\.[0-9]{1,2}$"), "ICD9")
     //     .when(length(df("MAPPEDCODE")) === 4, "REV")
          .when(df("CODE") === "REV_CODE", "REV")
          .otherwise(null))
      }),
      "MAPPEDCODE" -> ((col: String, df: DataFrame) => {
        df.withColumn(col, when(df("CODE") === "REV_CODE", lpad(df("MAPPEDCODE"),4,"0"))
                          .when(df("CODE") === "HCPCS_CODE" && df("MAPPEDCODE").rlike("^[A-Z]{1,1}[0-9]{4}[A-Z]{2}$"), substring(df("MAPPEDCODE"), 1, 5))
                          .otherwise(df("MAPPEDCODE")))
      }),
      "LOCALBILLINGPROVIDERID" -> mapFrom("BILLING_PROV_ID", nullIf = Seq("-1")),
      "LOCALNAME" -> mapFrom("PROC_NAME")
    )

    afterMap = (df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("ENCOUNTERID"), df1("MAPPEDCODE"), df1("PROCEDUREDATE"), df1("PERFORMINGPROVIDERID")).orderBy(df1("TX_POST_DATE").desc)
      val addColumn = df1.withColumn("proc_row", row_number().over(groups))
      //  .withColumn("incl_val", when(coalesce(df1("PAT_ENC_CSN_ID"), lit("-1")) =!= "-1" && coalesce(df1("ENCOUNTERID"),lit("-1")) === "-1", "1").otherwise(null))
        .withColumn("incl_val", when(coalesce(df1("ENCOUNTERID"), lit("-1")) =!= lit("-1") && coalesce(df1("ENCOUNTERID_ce"),lit("-1")) === lit("-1"), "1").otherwise(null))
      addColumn.filter("proc_row = 1 and PATIENTID is not null and PROCEDUREDATE is not null and incl_val is null and CODETYPE in ('HCPCS', 'CPT4', 'REV')")
    }
  }
  // TEST
  // val p = new ProcedureInptbillingacct(cfg) ; val pr = build(p) ; pr.show ; pr.count ; pr.select("ENCOUNTERID").distinct.count