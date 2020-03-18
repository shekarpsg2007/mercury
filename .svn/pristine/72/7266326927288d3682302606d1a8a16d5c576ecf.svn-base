package com.humedica.mercury.etl.epic_v2.diagnosis

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.{Engine, EntitySource}
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 01/27/2017
  */


class DiagnosisProblemlist(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List("problemlist", "zh_clarity_edg", "zh_edg_curr_icd9", "zh_edg_curr_icd10", "zh_v_edg_hx_icd9", "zh_v_edg_hx_icd10"
    ,"cdr.map_predicate_values")

  columnSelect = Map(
    "problemlist" -> List("DX_ID", "PAT_ID", "HOSPITAL_PL_YN", "PRINCIPAL_PL_YN",
      "UPDATE_DATE", "PROBLEM_STATUS_C", "NOTED_DATE", "DATE_OF_ENTRY", "RESOLVED_DATE"),
    "zh_clarity_edg" -> List("DX_ID", "REF_BILL_CODE", "REF_BILL_CODE_SET_C", "RECORD_TYPE_C"),
    "zh_edg_curr_icd9" -> List("DX_ID", "CODE"),
    "zh_edg_curr_icd10" -> List("DX_ID", "CODE"),
    "zh_v_edg_hx_icd9" -> List("DX_ID", "CODE", "EFF_START_DATE", "EFF_END_DATE"),
    "zh_v_edg_hx_icd10" -> List("DX_ID", "CODE", "EFF_START_DATE", "EFF_END_DATE"))

  beforeJoin = Map(
    "problemlist" -> ((df: DataFrame) => {
      val context = Engine.getSession()
      import context.implicits._
      val list_code_order = List(1,2)
      val df1 = list_code_order.map((_,1)).toDF("CODE_ORDER", "FOO").drop("FOO")
      val groups = Window.partitionBy(df("PAT_ID"), df("DX_ID"), coalesce(df("NOTED_DATE"), df("DATE_OF_ENTRY")))
        .orderBy(df("UPDATE_DATE").desc)
      val dedup = df.withColumn("rownumber", row_number.over(groups))
      val df2 = dedup.filter("rownumber = 1")
      val fil = df2.filter("PROBLEM_STATUS_C != '3'")
        .withColumn("NOTED_DATE", when(isnull(df2("RESOLVED_DATE")) || (df2("NOTED_DATE") lt df2("RESOLVED_DATE")), df2("NOTED_DATE")))
        .withColumn("DATE_OF_ENTRY", when(isnull(df2("RESOLVED_DATE")) || (df2("DATE_OF_ENTRY") lt df2("RESOLVED_DATE")), df2("DATE_OF_ENTRY")))
      val zh = table("zh_clarity_edg")
      val join1 = fil.join(zh, Seq("DX_ID"), "inner")
        .join(df1)
      join1.withColumn("DX_TIMESTAMP",
        when(join1("CODE_ORDER") === 1,
          when(join1("NOTED_DATE").isNotNull, join1("NOTED_DATE"))
            .when(isnull(join1("NOTED_DATE")) && isnull(join1("DATE_OF_ENTRY")), join1("RESOLVED_DATE")))
          .when(join1("CODE_ORDER") === 2, join1("DATE_OF_ENTRY")))
    }),
    "zh_edg_curr_icd9" -> renameColumn("CODE", "ECODE9"),
    "zh_edg_curr_icd10" -> renameColumn("CODE", "ECODE10"),
    "zh_v_edg_hx_icd9" -> renameColumns(List(("CODE", "HCODE9"), ("EFF_START_DATE", "START9"), ("EFF_END_DATE", "END9"), ("DX_ID", "DX_ID9"))),
    "zh_v_edg_hx_icd10" -> renameColumns(List(("CODE", "HCODE10"), ("EFF_START_DATE", "START10"), ("EFF_END_DATE", "END10"), ("DX_ID", "DX_ID10"))))


  join = (dfs: Map[String, DataFrame]) => {
    dfs("problemlist")
      .join(dfs("zh_edg_curr_icd9"), Seq("DX_ID"), "left_outer")
      .join(dfs("zh_edg_curr_icd10"), Seq("DX_ID"), "left_outer")
      .join(dfs("zh_v_edg_hx_icd9"), dfs("problemlist")("DX_ID") === dfs("zh_v_edg_hx_icd9")("DX_ID9") &&
        (dfs("problemlist")("DX_TIMESTAMP").between(dfs("zh_v_edg_hx_icd9")("START9"), dfs("zh_v_edg_hx_icd9")("END9"))), "left_outer")
      .join(dfs("zh_v_edg_hx_icd10"), dfs("problemlist")("DX_ID") === dfs("zh_v_edg_hx_icd10")("DX_ID10") &&
        (dfs("problemlist")("DX_TIMESTAMP").between(dfs("zh_v_edg_hx_icd10")("START10"), dfs("zh_v_edg_hx_icd10")("END10"))), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("problemlist"),
    "DX_TIMESTAMP" -> mapFrom("DX_TIMESTAMP"),
    "LOCALDIAGNOSIS" -> mapFrom("DX_ID", nullIf = Seq("-1")),
    "PATIENTID" -> mapFrom("PAT_ID", nullIf = Seq("-1")),
    "LOCALACTIVEIND" -> mapFrom("PROBLEM_STATUS_C",nullIf = Seq("-1"), prefix = config(CLIENT_DS_ID) + "."),
    /*"MAPPEDDIAGNOSIS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RECORD_TYPE_C").isin("1", "-1") || isnull(df("RECORD_TYPE_C")), df("STANDARDCODE"))
        .when(df("REF_BILL_CODE").isin("IMO0001", "000.0"), null).otherwise(df("REF_BILL_CODE")))
    }),
    "CODETYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RECORD_TYPE_C").isin("1", "-1") || isnull(df("RECORD_TYPE_C")), df("STANDARDCODETYPE"))
        .when(df("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
        .when(df("REF_BILL_CODE_SET_C") === "1", "ICD9")
        .when(df("REF_BILL_CODE_SET_C") === "2", "ICD10")
      )
    }),*/
    "RESOLUTIONDATE" -> mapFrom("RESOLVED_DATE")
  )


  afterMap = (df: DataFrame) => {

    val df1 = df.repartition(1000)
    val drop_diag = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPIC", "DIAGNOSIS", "DIAGNOSIS", "LOCALDIAGNOSIS")

    val addColumn = df1.withColumn("MAPPED_ICD9", coalesce(df1("HCODE9"), df1("ECODE9")))
      .withColumn("MAPPED_ICD10", coalesce(df1("HCODE10"), df1("ECODE10")))

    val fpiv = unpivot(
      Seq("MAPPED_ICD9", "MAPPED_ICD10"),
      Seq("ICD9", "ICD10"), typeColumnName = "STANDARDCODETYPE")
    val fpiv2 = fpiv("STANDARDCODE", addColumn)

    val mappeddiag = fpiv2.withColumn("MAPPEDDIAGNOSIS", when(fpiv2("RECORD_TYPE_C").isin("1", "-1") || isnull(fpiv2("RECORD_TYPE_C")), fpiv2("STANDARDCODE"))
      .when(fpiv2("REF_BILL_CODE").isin("IMO0001", "000.0"), null).otherwise(fpiv2("REF_BILL_CODE")))

    val cdtype = mappeddiag.withColumn("CODETYPE", when(mappeddiag("RECORD_TYPE_C").isin("1", "-1") || isnull(mappeddiag("RECORD_TYPE_C")), mappeddiag("STANDARDCODETYPE"))
      .when(mappeddiag("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "1", "ICD9")
      .when(mappeddiag("REF_BILL_CODE_SET_C") === "2", "ICD10"))

    val ct_window = Window.partitionBy(cdtype("PAT_ID"), cdtype("DX_TIMESTAMP"), cdtype("DX_ID"))

    val fil = cdtype.withColumn("ICD9_CT", sum(when(cdtype("CODETYPE") === lit("ICD9"), 1).otherwise(0)).over(ct_window))
      .withColumn("ICD10_CT", sum(when(cdtype("CODETYPE") === lit("ICD10"), 1).otherwise(0)).over(ct_window))

    val icdfil = fil.withColumn("drop_icd9s",
      when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP >= from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
        when(fil("ICD9_CT").gt(lit(0)) && fil("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .withColumn("drop_icd10s",
        when(lit("'Y'") === drop_diag && expr("DX_TIMESTAMP < from_unixtime(unix_timestamp(\"20151001\",\"yyyyMMdd\"))"),
          when(fil("ICD9_CT").gt(lit(0)) && fil("ICD10_CT").gt(lit(0)), lit("1")).otherwise(lit("0"))).otherwise(lit("0")))
      .filter("PATIENTID is not null and LOCALDIAGNOSIS is not null and DX_TIMESTAMP is not null")


    val groups = Window.partitionBy(icdfil("PATIENTID"), icdfil("LOCALDIAGNOSIS"), icdfil("DX_TIMESTAMP"), icdfil("MAPPEDDIAGNOSIS"),icdfil("CODETYPE")).orderBy(icdfil("UPDATE_DATE").desc)
    val dedup = icdfil.withColumn("rw", row_number.over(groups))
    dedup.filter("rw = 1 and ((codetype = 'ICD9' and drop_icd9s = '0') or (codetype = 'ICD10' and drop_icd10s = '0'))")
  }
}



//test
//  val dpl= new DiagnosisProblemlist(cfg) ; val dp = build(dpl); dp.show(false) ; dp.count(); dp.select("PATIENTID").distinct.count()
