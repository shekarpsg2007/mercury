package com.humedica.mercury.etl.epic_v2.providerpatientrelation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import javax.annotation.concurrent.NotThreadSafe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by bhenriksen on 1/25/17.
 */
class ProviderpatientrelationZhpatpcp(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  tables = List("temptable:epic_v2.providerpatientrelation.ProviderpatientrelationTemptablezh")

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val groups = Window.orderBy(df("PATIENTID"), df("STARTDATE"))
      df.withColumn("rnbr_pat", row_number.over(groups)).withColumn("STARTDATE", substring(df("STARTDATE"),1,10)).withColumn("ENDDATE", substring(df("ENDDATE"),1,10))
        .select("PATIENTID", "PROVIDERID", "STARTDATE", "ENDDATE", "rnbr_pat")
    })
  )



  afterJoin = (df: DataFrame) => {
    val gtt_prv = df.withColumn("rnbr_pat", df("rnbr_pat") + lit(1)).withColumnRenamed("PROVIDERID", "PROVIDERID_prv").withColumnRenamed("PATIENTID", "PATIENTID_prv")
        .withColumnRenamed("STARTDATE", "STARTDATE_prv").withColumnRenamed("ENDDATE", "ENDDATE_prv")
    val gtt_nxt = df.withColumn("rnbr_pat", df("rnbr_pat") - lit(1)).withColumnRenamed("PROVIDERID", "PROVIDERID_nxt").withColumnRenamed("PATIENTID", "PATIENTID_nxt")
      .withColumnRenamed("STARTDATE", "STARTDATE_nxt").withColumnRenamed("ENDDATE", "ENDDATE_nxt")
    val joined = df.join(gtt_prv, Seq("rnbr_pat"), "left_outer")
       .join(gtt_nxt, Seq("rnbr_pat"), "left_outer")
    val joined1 = joined.withColumn("FIRSTCONTIG", when(((coalesce(joined("PATIENTID_prv"), lit("-null-")) !== joined("PATIENTID")) || (joined("PROVIDERID_prv") !== joined("PROVIDERID"))) && ((coalesce(joined("PATIENTID_nxt"), lit("-null-")) === joined("PATIENTID")) && (joined("PROVIDERID_nxt") === joined("PROVIDERID")) && joined("STARTDATE_nxt").between(joined("ENDDATE"), date_add(joined("ENDDATE"),1))), "y").otherwise("n"))
      .withColumn("INTRACONTIG", when((joined("PATIENTID_prv") === joined("PATIENTID")) && (joined("PROVIDERID_prv") === joined("PROVIDERID")) && (coalesce(joined("PATIENTID_nxt"), lit("-null-")) === joined("PATIENTID")) && (joined("PROVIDERID_nxt") === joined("PROVIDERID")) && joined("STARTDATE_nxt").between(joined("ENDDATE"), date_add(joined("ENDDATE"),1)), "y").otherwise("n"))
      .withColumn("LASTCONTIG", when(
        ((coalesce(joined("PATIENTID_prv"), lit("-null-")) === joined("PATIENTID")) && (joined("PROVIDERID_prv") === joined("PROVIDERID")) && joined("STARTDATE").between(joined("ENDDATE_prv"), date_add(joined("ENDDATE_prv"),1))) &&
        ((coalesce(joined("PATIENTID_nxt"), lit("-null-")) !== joined("PATIENTID")) || (joined("PROVIDERID_nxt") !== joined("PROVIDERID")) || !joined("STARTDATE_nxt").between(joined("ENDDATE"), date_add(joined("ENDDATE"),1))), "y").otherwise("n"))
      .withColumnRenamed("PATIENTID_prv", "PRVPAT").withColumnRenamed("PROVIDERID_prv", "PRVPROV").withColumnRenamed("ENDDATE_prv", "PRVEND")
      .withColumnRenamed("PATIENTID_nxt", "NXTPAT").withColumnRenamed("PROVIDERID_nxt", "NXTPROV").withColumnRenamed("ENDDATE_nxt", "NXTEND")
    val fil = joined1.filter("(FIRSTCONTIG = 'n' AND INTRACONTIG = 'n' and LASTCONTIG = 'n') OR (FIRSTCONTIG = 'y' or LASTCONTIG = 'y')")
     fil.withColumn("LEADENDASLAST", lead(fil("ENDDATE"),1).over(Window.orderBy(fil("PATIENTID"), fil("STARTDATE"))))

  }


  map = Map(
    "DATASRC" -> literal("zh_pat_pcp"),
    "LOCALRELSHIPCODE" -> literal("PCP"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PROVIDERID" -> mapFrom("PROVIDERID"),
    "STARTDATE" -> mapFrom("STARTDATE"),
    "ENDDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("FIRSTCONTIG") === "y", df("LEADENDASLAST")).otherwise(df("ENDDATE")))
    })
  )


  afterMap = includeIf("LASTCONTIG <> 'y' or FIRSTCONTIG = 'y'")



}