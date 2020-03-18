package com.humedica.mercury.etl.athena.providerpatientrelation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ProviderpatientrelationPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patient",
    "fileIdDates:athena.util.UtilFileIdDates",
    "cdr.map_pcp_order",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "patient" -> List("PATIENT_ID", "PRIMARY_PROVIDER_ID", "FILEID"),
    "fileIdDates" -> List("FILEID", "FILEDATE"),
    "cdr.map_pcp_order" -> List("GROUPID", "CLIENT_DS_ID", "DATASRC", "PCP_EXCLUDE_FLG")
  )

  beforeJoin = Map(
    "patient" -> includeIf("patient_id is not null and primary_provider_id is not null"),
    "cdr.map_pcp_order" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUPID) + "' and client_ds_id = '" + config(CLIENT_DS_ID) + "' and datasrc = 'patient'")
        .drop("GROUPID", "CLIENT_DS_ID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patient")
      .join(dfs("fileIdDates"), Seq("FILEID"), "inner")
      .crossJoin(dfs("cdr.map_pcp_order"))
  }

  afterJoin = (df: DataFrame) => {
    val df1 = df.filter("coalesce(pcp_exclude_flg, 'N') <> 'Y'")

    val groups = Window.partitionBy(to_date(df1("FILEDATE")), df1("PATIENT_ID"))
      .orderBy(df1("FILEDATE").desc_nulls_last, df1("FILEID").desc_nulls_last)

    df1.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }


  map = Map(
    "DATASRC" -> literal("patient"),
    "LOCALRELSHIPCODE" -> literal("PCP"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "PROVIDERID" -> mapFrom("PRIMARY_PROVIDER_ID"),
    "STARTDATE" -> mapFrom("FILEDATE"),
    "ENDDATE" -> ((col: String, df: DataFrame) => {
      val load_ppr = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ATHENA_DWF", "CALL_LOAD_PROV_PAT_REL", "PROV_PAT_REL", "N/A")
      df.withColumn(col, when(lit("Y").isin(load_ppr: _*), current_timestamp()).otherwise(null))
    })
  )

}