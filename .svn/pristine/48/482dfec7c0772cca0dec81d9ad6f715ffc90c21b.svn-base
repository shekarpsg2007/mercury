package com.humedica.mercury.etl.epic_v2.diagnosis

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/27/2017
 */


class DiagnosisProfbilling(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.claim.ClaimProfbillingtemptable", "encountervisit:epic_v2.clinicalencounter.ClinicalencounterEncountervisit",
    "zh_clarity_edg", "cdr.map_predicate_values")

  columnSelect=Map(
    "encountervisit" -> List("ENCOUNTERID", "PATIENTID")
  )

 // beforeJoin = Map(
 // "cdr.map_predicate_values" -> ((df: DataFrame) => {
 //   val fil = df.filter("GROUPID = '" + config(GROUP) + "' AND DATA_SRC = 'PAT_ENC_DX' AND ENTITY = 'DIAGNOSIS' AND TABLE_NAME = 'PAT_ENC_DX' AND COLUMN_NAME = 'DX_ID'" +
 //     " AND CLIENT_DS_ID = '" + config(CLIENT_DS_ID) + "'")
 //   fil.select("COLUMN_VALUE")
 // }))

  beforeJoin = Map(
    "encountervisit" -> renameColumn("ENCOUNTERID", "ENCOUNTERID_ce")
  )

  join = (dfs: Map[String, DataFrame]) => {

     // val fil = dfs("cdr.map_predicate_values").filter("GROUPID = '"+config(GROUP)+"' AND CLIENT_DS_ID = '"+config(CLIENT_DS_ID)+"' AND DATA_SRC = 'PAT_ENC_DX' " +
     // "AND entity = 'DIAGNOSIS' AND TABLE_NAME = 'PAT_ENC_DX' AND COLUMN_NAME = 'DX_ID'")
      dfs("temptable")
      .join(dfs("encountervisit"), dfs("temptable")("PAT_ENC_CSN_ID") === dfs("encountervisit")("ENCOUNTERID_ce"), "left_outer")
      .join(dfs("zh_clarity_edg"), dfs("temptable")("LOCALCODE") === dfs("zh_clarity_edg")("DX_ID"), "left_outer")
      //.join(dfs("cdr.map_predicate_values"), dfs("temptable")("LOCALCODE") === fil("COLUMN_VALUE"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("profbilling"),
    "PATIENTID" -> cascadeFrom(Seq("PAT_ID", "PATIENTID")),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      val include_encid = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "PROFBILLING", "CLINICALENCOUNTER", "PROFBILLING_TXN", "INCLUDE")
      df.withColumn(col, when(df("PAT_ENC_CSN_ID").isNotNull && df("PAT_ENC_CSN_ID") =!= lit("-1"), df("PAT_ENC_CSN_ID"))
        .when((df("PAT_ENC_CSN_ID").isNull || df("PAT_ENC_CSN_ID") === lit("-1")) && lit("'Y'") === include_encid,
          concat_ws("", lit("profbilling."),coalesce(df("PAT_ID"),df("PATIENTID")),lit("."),df("TXN_TX_ID")))
        .otherwise(null))
    }),
    "SOURCEID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat(lit("profbilling."), coalesce(df("PAT_ID"), df("PATIENTID")), lit("."), df("TXN_TX_ID")))
    }),
    "DX_TIMESTAMP" -> mapFrom("ORIG_SERVICE_DATE"),
    "LOCALDIAGNOSIS" -> mapFrom("LOCALCODE"),
   /* "MAPPEDDIAGNOSIS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RECORD_TYPE_C").isin("1", "-1") || isnull(df("RECORD_TYPE_C")), df("STANDARDCODE"))
        .when(df("REF_BILL_CODE").isin("IMO0001", "000.0"), null).otherwise(df("REF_BILL_CODE")))
    }),
    "CODETYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RECORD_TYPE_C").isin("1", "-1") || isnull(df("RECORD_TYPE_C")), df("STANDARDNAME"))
        .when(df("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
        .when(df("REF_BILL_CODE_SET_C") === "1", "ICD9")
        .when(df("REF_BILL_CODE_SET_C") === "2", "ICD10"))
    }),*/
    //"MAPPEDDIAGNOSIS" -> mapFrom("MAPPEDDIAGNOSIS"),
    //"CODETYPE" -> mapFrom("CODETYPE"),
    "PRIMARYDIAGNOSIS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("LOCALNAME") === "DX_ONE_ID", 1).otherwise(0))
    }),
    "HOS_DX_FLAG" -> literal("N")
  )

/*
  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"),df("SOURCEID"),df("DX_TIMESTAMP"),df("ENCOUNTERID"),df("LOCALDIAGNOSIS"),
      df("MAPPEDDIAGNOSIS"), df("CODETYPE")).orderBy(df("POST_DATE").desc)
    val addColumn = df.withColumn("diag_row", row_number.over(groups))
                      .withColumn("incl_val", when(coalesce(df("PAT_ENC_CSN_ID"), lit("-1")) =!= "-1" && coalesce(df("ENCOUNTERID"),lit("-1")) === "-1", "1").otherwise(null))
    addColumn.filter("diag_row = 1 and LOCALDIAGNOSIS is not null" +
      " and PATIENTID is not null AND DX_TIMESTAMP is not null and incl_val is null " +
      "and ( (CODETYPE = 'ICD9' and DROPS_ICD9 = '0') or (CODETYPE = 'ICD10' and DROPS_ICD10 = '0') )")
  }*/

  afterMap = (df: DataFrame) => {

    val df1 = df.repartition(1000)
    val drop_diag = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPIC", "DIAGNOSIS", "DIAGNOSIS", "LOCALDIAGNOSIS")
    val fil2 = df1.withColumn("MAPPED_ICD9", coalesce(df1("CODE_9a"), df1("CODE_9b")))
      .withColumn("MAPPED_ICD10", coalesce(df1("CODE_10a"), df1("CODE_10b")))

    val fpiv = unpivot(
      Seq("MAPPED_ICD9", "MAPPED_ICD10"),
      Seq("ICD9", "ICD10"), typeColumnName = "STANDARDCODETYPE"
    )
    val fpiv2 = fpiv("STANDARDCODE", fil2)

    val mappeddiag = fpiv2.withColumn("MAPPEDDIAGNOSIS",
      when(fpiv2("RECORD_TYPE_C").isin("1", "-1") || isnull(fpiv2("RECORD_TYPE_C")), fpiv2("STANDARDCODE"))
        .when(fpiv2("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
        .otherwise(fpiv2("REF_BILL_CODE")))

    val cdtype = mappeddiag.withColumn("CODETYPE",  when(mappeddiag("RECORD_TYPE_C").isin("1", "-1") || isnull(mappeddiag("RECORD_TYPE_C")), mappeddiag("STANDARDCODETYPE"))
      .when(mappeddiag("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "1", "ICD9")
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "2", "ICD10"))

    val ct_window = Window.partitionBy(cdtype("PATIENTID"), cdtype("DX_TIMESTAMP"), cdtype("ENCOUNTERID"), cdtype("LOCALDIAGNOSIS"))

    val fil3 = cdtype.withColumn("ICD9_CT", sum(when(cdtype("CODETYPE") === lit("ICD9"), 1).otherwise(0)).over(ct_window))
      .withColumn("ICD10_CT", sum(when(cdtype("CODETYPE") === lit("ICD10"), 1).otherwise(0)).over(ct_window))

    val icdfil = fil3.withColumn("drop_icd9s",
      when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP >= from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
        when(fil3("ICD9_CT").gt(lit(0)) && fil3("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .withColumn("drop_icd10s",
        when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP < from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
          when(fil3("ICD9_CT").gt(lit(0)) && fil3("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))

    val df2 = icdfil.withColumn("incl_val", when(coalesce(icdfil("PAT_ENC_CSN_ID"), lit("-1")) =!= lit("-1") && coalesce(icdfil("ENCOUNTERID_ce"),lit("-1")) === lit("-1"), "1").otherwise(null))
      .filter("dx_timestamp is not null and patientid is not null and localdiagnosis is not null and incl_val is null ")

    val groups = Window.partitionBy(df2("PATIENTID"),df2("SOURCEID"),df2("DX_TIMESTAMP"),df2("ENCOUNTERID"),df2("LOCALDIAGNOSIS"), df2("MAPPEDDIAGNOSIS"),df2("CODETYPE")).orderBy(df2("POST_DATE").desc)
    val dedupe_addcol = df2.withColumn("rn", row_number.over(groups))
    dedupe_addcol.filter("rn = 1 and ((codetype = 'ICD9' and drop_icd9s = '0') or (codetype = 'ICD10' and drop_icd10s = '0'))" )
  }


}

// test
// val d = new DiagnosisProfbilling(cfg) ; val diag = build(d) ; diag.show
