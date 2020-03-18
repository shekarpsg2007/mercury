package com.humedica.mercury.etl.epic_v2.providerpatientrelation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cdivakaran on 6/14/17.
  */
class ProviderpatientrelationTemptablereg(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  //Use when zh_pat_pcp is empty

  //no inclusion criteria

  tables = List("patreg", "cdr.map_pcp_order", "patreg_epic_v1_history")


  beforeJoin = Map(
    "patreg" -> ((df: DataFrame) => {
      val pcp1 = table("cdr.map_pcp_order").filter("DATASRC = 'registration' and GROUPID='"+config(GROUP)+"' and CLIENT_DS_ID='"+config(CLIENT_DS_ID)+"'")
      val df1 = df.withColumn("GROUPID", lit(config(GROUP)))
      val joined1 = df1.join(pcp1, Seq("GROUPID"), "left_outer")
      val p = joined1.filter("PAT_ID is not null and PAT_ID <> '-1' and coalesce(CUR_PCP_PROV_ID, '-1') <> '000000'" +
        "and (PCP_EXCLUDE_FLG is null OR PCP_EXCLUDE_FLG <> 'Y')")
        .select("PAT_ID", "CUR_PCP_PROV_ID", "REG_DATE", "UPDATE_DATE")
      val df2 = table("patreg_epic_v1_history").withColumn("GROUPID", lit(config(GROUP)))
      val joined2 =  df2.join(pcp1, Seq("GROUPID"), "left_outer")
      val i = joined2.filter("PAT_ID is not null and UPDATE_DATE is not null and coalesce(CUR_PCP_PROV_ID, '-1') <> '000000'" +
        "and (PCP_EXCLUDE_FLG is null OR PCP_EXCLUDE_FLG <> 'Y')")
        .select("PAT_ID", "CUR_PCP_PROV_ID", "REG_DATE", "UPDATE_DATE")
      val un = p.unionAll(i)
      un.withColumn("PATIENTID", un("PAT_ID"))
        .withColumn("PROVIDERID", coalesce(un("CUR_PCP_PROV_ID"), lit("-1")))
        .withColumn("REG_DT", substring(un("REG_DATE"), 1,10))
        .withColumn("UPDT_DT", substring(un("UPDATE_DATE"), 1,10))

    })
  )


  afterJoin = (df: DataFrame) => {
    val df1=df.repartition(1000)
    val groups = Window.partitionBy(df1("PATIENTID")).orderBy(when(isnull(df1("REG_DT")), 0).otherwise(1), df1("UPDT_DT").desc)
    val groups1 = Window.partitionBy(df1("PATIENTID")).orderBy(df1("UPDT_DT"))
    df1.withColumn("rn", row_number.over(groups1)).withColumn("BEST_REG_DT", first("REG_DT").over(groups))
  }


  map = Map(
    "DATASRC" -> literal("registration"),
    "LOCALRELSHIPCODE" -> literal("PCP"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PROVIDERID" -> mapFrom("PROVIDERID"),
    "STARTDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("rn") === 1 && (coalesce(df("PROVIDERID"), lit("-1")) !== "-1"),
        coalesce(df("BEST_REG_DT"), lit("2012-01-01")))
        .otherwise(df("UPDT_DT")))
    }),
    "ENDDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(current_date(),1,10))
    })
  )

}