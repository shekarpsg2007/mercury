package com.humedica.mercury.etl.athena.providerpatientrelation

import com.humedica.mercury.etl.core.engine.{Engine, EntitySource}
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * created by mschlomka on 11/15/2018
  */


class ProviderpatientrelationProvpatrel(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("ppr1:athena.providerpatientrelation.ProviderpatientrelationCustomdemographics",
    "ppr2:athena.providerpatientrelation.ProviderpatientrelationPatient",
    "ppr3:athena.providerpatientrelation.ProviderpatientrelationPatientinsurance",
    "ppr4:athena.providerpatientrelation.ProviderpatientrelationPatientprovider",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "ppr1" -> List("DATASRC", "LOCALRELSHIPCODE", "PATIENTID", "PROVIDERID", "STARTDATE", "ENDDATE"),
    "ppr2" -> List("DATASRC", "LOCALRELSHIPCODE", "PATIENTID", "PROVIDERID", "STARTDATE", "ENDDATE"),
    "ppr3" -> List("DATASRC", "LOCALRELSHIPCODE", "PATIENTID", "PROVIDERID", "STARTDATE", "ENDDATE"),
    "ppr4" -> List("DATASRC", "LOCALRELSHIPCODE", "PATIENTID", "PROVIDERID", "STARTDATE", "ENDDATE")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("ppr1")
      .union(dfs("ppr2"))
      .union(dfs("ppr3"))
      .union(dfs("ppr4"))
  }

  afterJoin = (df: DataFrame) => {
    val use_load_ppr = mpvList1(table("cdr.map_predicate_values"), config(GROUPID), config(CLIENT_DS_ID), "ATHENA_DWF", "CALL_LOAD_PROV_PAT_REL", "PROV_PAT_REL", "N/A")

    if (use_load_ppr.head == "Y") {
      val ss = Engine.getSession()
      import ss.implicits._

      val stubRows = Seq[(String, String, String, String)]((null, "PCP", ".", ".."), (null, "PCP", "~", "000000"))
        .toDF("DATASRC", "LOCALRELSHIPCODE", "PATIENTID", "PROVIDERID")
        .withColumn("STARTDATE", current_date())
        .withColumn("ENDDATE", lit(null))
      val df1 = df.union(stubRows)
        .orderBy("PATIENTID", "STARTDATE")

      val addColumn = df1.withColumn("LAGPAT", lag(df1("PATIENTID"), 1).over(Window.orderBy(df1("PATIENTID"), df1("STARTDATE"))))
        .withColumn("LEADPAT", lead(df1("PATIENTID"), 1).over(Window.orderBy(df1("PATIENTID"), df1("STARTDATE"))))
        .withColumn("LAGPROV", lag(df1("PROVIDERID"), 1).over(Window.orderBy(df1("PATIENTID"), df1("STARTDATE"))))
      val add1 = addColumn.withColumn("NEWPROV", when((addColumn("LAGPROV") =!= addColumn("PROVIDERID")) || (addColumn("LAGPAT") =!= addColumn("PATIENTID")), addColumn("PROVIDERID")).otherwise("x"))
        .withColumn("LASTTHISPAT", when(addColumn("LEADPAT") =!= addColumn("PATIENTID"), "LAST").otherwise("x"))
      val fil1 = add1.filter("NEWPROV <> 'x' OR LASTTHISPAT <> 'x'")
      val add2 = fil1.withColumn("LEADST", lead(fil1("STARTDATE"), 1).over(Window.orderBy(fil1("PATIENTID"), fil1("STARTDATE"))))
        .withColumn("LEADLAST", lead(fil1("LASTTHISPAT"), 1).over(Window.orderBy(fil1("PATIENTID"), fil1("STARTDATE"))))
        .withColumn("LEADNWPROV", lead(fil1("NEWPROV"), 1).over(Window.orderBy(fil1("PATIENTID"), fil1("STARTDATE"))))
      add2.withColumn("ENDDATE",
        when(add2("PROVIDERID") === "-1", "9999-12-31")
          .when(add2("LEADNWPROV") === "-1" && (add2("LEADPAT") === add2("PATIENTID")),
            when(substring(add2("LEADST"), 1, 10) === substring(add2("STARTDATE"), 1, 10), add2("STARTDATE")).otherwise(date_sub(add2("LEADST"), 1)))
          .when((add2("LEADLAST") === "LAST") && (add2("NEWPROV") =!= "x") && (add2("LEADNWPROV") === "x"), null)
          .when((add2("LASTTHISPAT") === "x") && (add2("NEWPROV") =!= "x") && (add2("LEADNWPROV") =!= "x"),
            when(substring(add2("LEADST"), 1, 10) === substring(add2("STARTDATE"), 1, 10), add2("STARTDATE")).otherwise(date_sub(add2("LEADST"), 1)))
          .when((add2("LASTTHISPAT") === "LAST") && (add2("NEWPROV") =!= "x"), null)
          .otherwise("9999-12-31"))
        .filter("(ENDDATE is null or ENDDATE <> '9999-12-31') and PATIENTID <> '.'")
    }
    else df
  }

  map = Map(
    "DATASRC" -> mapFrom("DATASRC"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PROVIDERID" -> mapFrom("PROVIDERID"),
    "LOCALRELSHIPCODE" -> mapFrom("LOCALRELSHIPCODE"),
    "STARTDATE" -> mapFrom("STARTDATE"),
    "ENDDATE" -> mapFrom("ENDDATE")
  )

}
