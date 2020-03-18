package com.humedica.mercury.etl.epic_v2.providerpatientrelation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._




/**
 * Created by bhenriksen on 1/25/17.
 */
class ProviderpatientrelationRegistration(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  //Use when zh_pat_pcp is empty

  //no inclusion criteria

  tables = List("temptable:epic_v2.providerpatientrelation.ProviderpatientrelationTemptablereg")


  columnSelect = Map(
    "temptable" -> List("LOCALRELSHIPCODE", "PATIENTID", "PROVIDERID", "STARTDATE")
  )



  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val df1=df.orderBy("PATIENTID", "STARTDATE")
      val addColumn = df1.withColumn("LAGPAT", lag(df1("PATIENTID"),1).over(Window.orderBy(df1("PATIENTID"), df1("STARTDATE"))))
        .withColumn("LEADPAT", lead(df1("PATIENTID"),1).over(Window.orderBy(df1("PATIENTID"), df1("STARTDATE"))))
        .withColumn("LAGPROV", lag(df1("PROVIDERID"),1).over(Window.orderBy(df1("PATIENTID"), df1("STARTDATE"))))
      val add1 = addColumn.withColumn("NEWPROV", when((addColumn("LAGPROV") !== addColumn("PROVIDERID")) || (addColumn("LAGPAT") !== addColumn("PATIENTID")), addColumn("PROVIDERID")).otherwise("x"))
        .withColumn("LASTTHISPAT", when(addColumn("LEADPAT") !== addColumn("PATIENTID"), "LAST").otherwise("x"))
      val fil1 = add1.filter("NEWPROV <> 'x' OR LASTTHISPAT <> 'x'")
      val add2 = fil1.withColumn("LEADST", lead(fil1("STARTDATE"),1).over(Window.orderBy(fil1("PATIENTID"), fil1("STARTDATE"))))
        .withColumn("LEADLAST", lead(fil1("LASTTHISPAT"),1).over(Window.orderBy(fil1("PATIENTID"), fil1("STARTDATE"))))
        .withColumn("LEADNWPROV", lead(fil1("NEWPROV"),1).over(Window.orderBy(fil1("PATIENTID"), fil1("STARTDATE"))))
      add2.withColumn("ENDDATE",
           when(add2("PROVIDERID") === "-1", "9999-12-31")
          .when(add2("LEADNWPROV") === "-1" && (add2("LEADPAT") === add2("PATIENTID")),
            when(substring(add2("LEADST"),1,10) === substring(add2("STARTDATE"),1,10), add2("STARTDATE")).otherwise(date_sub(add2("LEADST"),1)))
          .when((add2("LEADLAST") === "LAST") && (add2("NEWPROV") !== "x") && (add2("LEADNWPROV") === "x"), null)
          .when((add2("LASTTHISPAT") === "x") && (add2("NEWPROV") !== "x") && (add2("LEADNWPROV") !== "x"),
            when(substring(add2("LEADST"),1,10) === substring(add2("STARTDATE"),1,10), add2("STARTDATE")).otherwise(date_sub(add2("LEADST"),1)))
          .when((add2("LASTTHISPAT") === "LAST") && (add2("NEWPROV") !== "x"), null)
          .otherwise("9999-12-31"))

    })
  )





  afterJoin = (df: DataFrame) => {
    df.filter("(ENDDATE is null or ENDDATE <> '9999-12-31') and PATIENTID <> '.'")
  }


  map = Map(
    "DATASRC" -> literal("registration"),
    "LOCALRELSHIPCODE" -> literal("PCP"),
    "STARTDATE" -> mapFrom("STARTDATE"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PROVIDERID" -> mapFrom("PROVIDERID"),
    "ENDDATE" -> mapFrom("ENDDATE")
  )

}