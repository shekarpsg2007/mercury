package com.humedica.mercury.etl.epic_v2.providerpatientrelation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by cdivakaran on 6/14/17.
  */
class ProviderpatientrelationTemptablezh(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  tables = List("zh_pat_pcp", "cdr.map_pcp_order", "cdr.map_predicate_values")


  beforeJoin = Map(
    "zh_pat_pcp" -> ((df: DataFrame) => {
      val cutoff = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ZH_PAT_PCP", "PROV_PAT_REL", "ZH_PAT_PCP", "CUTOFF_AFTER_DATE")
      df.withColumn("LIST_CUT_OFF_DATE", when(lit("'NO_MPV_MATCHES'").isin (cutoff:_*) , date_add(current_date(), 1)).otherwise(cutoff.head))
        .withColumn("GROUPID", lit(config(GROUP)))
    }),
    "cdr.map_pcp_order" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'zh_pat_pcp' and GROUPID='"+config(GROUP)+"'").drop("CLIENT_DS_ID")

    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_pat_pcp")
      .join(dfs("cdr.map_pcp_order"), Seq("GROUPID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    df.filter("PCP_TYPE_C = 1 AND (DELETED_YN = 'N' OR DELETED_YN is null) AND PAT_ID is not null AND (PCP_PROV_ID is not null and PCP_PROV_ID <> '-1') AND EFF_DATE is not null and " +
      "EFF_DATE < LIST_CUT_OFF_DATE AND (PCP_EXCLUDE_FLG is null or PCP_EXCLUDE_FLG <> 'Y')")
  }

  map = Map(
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROVIDERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("PCP_PROV_ID"), lit("1")))
    }),
    "STARTDATE" -> mapFrom("EFF_DATE"),
    "ENDDATE" -> mapFrom("TERM_DATE")
  )

  afterJoinExceptions = Map(
    "H135772_EP2_BSL" -> ((df: DataFrame) => {
      df.filter("PCP_TYPE_C in (1, 2502, 2503) AND (DELETED_YN = 'N' OR DELETED_YN is null) AND PAT_ID is not null AND (PCP_PROV_ID is not null and PCP_PROV_ID <> '-1') AND EFF_DATE is not null and " +
        "EFF_DATE < LIST_CUT_OFF_DATE AND (PCP_EXCLUDE_FLG is null or PCP_EXCLUDE_FLG <> 'Y')")
    }),
    "H135772_EP2_BCM" -> ((df: DataFrame) => {
      df.filter("PCP_TYPE_C in (1, 2502, 2503) AND (DELETED_YN = 'N' OR DELETED_YN is null) AND PAT_ID is not null AND (PCP_PROV_ID is not null and PCP_PROV_ID <> '-1') AND EFF_DATE is not null and " +
        "EFF_DATE < LIST_CUT_OFF_DATE AND (PCP_EXCLUDE_FLG is null or PCP_EXCLUDE_FLG <> 'Y')")
    })
  )

}