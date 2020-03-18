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


class ProviderpatientrelationPatientprovider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patientprovider",
    "patient",
    "chart",
    "fileIdDates:athena.util.UtilFileIdDates",
    "cdr.map_pcp_order",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "patientprovider" -> List("PATIENT_ID", "RECIPIENT_ID", "RECIPIENT_TYPE", "CREATED_DATETIME", "DELETED_DATETIME",
      "PATIENT_PROVIDER_ID", "ROLE", "TYPE", "CHART_ID", "FILEID"),
    "patient" -> List("PATIENT_ID", "ENTERPRISE_ID"),
    "chart" -> List("CHART_ID", "ENTERPRISE_ID"),
    "fileIdDates" -> List("FILEID", "FILEDATE"),
    "cdr.map_pcp_order" -> List("GROUPID", "CLIENT_DS_ID", "DATASRC", "PCP_EXCLUDE_FLG")
  )

  beforeJoin = Map(
    "patientprovider" -> ((df: DataFrame) => {
      val df1 = df.filter("role = 'Primary Care Provider' and recipient_id is not null and type = 'PATIENTPROVIDER'")
      df1.withColumn("incl_val", when(to_date(df1("CREATED_DATETIME")) === to_date(df1("DELETED_DATETIME")), "N").otherwise("Y"))
        .withColumn("PROVIDER_ID", when(df1("RECIPIENT_TYPE") === lit("CLINICALPROVIDER"), concat(lit("cp."), df1("RECIPIENT_TYPE")))
          .when(df1("RECIPIENT_TYPE") === lit("REFERRINGPROVIDERID"), concat(lit("rp."), df1("RECIPIENT_TYPE")))
          .when(df1("RECIPIENT_TYPE") === lit("PROVIDERID"), df1("RECIPIENT_TYPE"))
          .otherwise(null))
        .withColumnRenamed("PATIENT_ID", "PATIENT_ID_pp")
    }),
    "patient" -> renameColumn("PATIENT_ID", "PATIENT_ID_pat"),
    "cdr.map_pcp_order" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUPID) + "' and client_ds_id = '" + config(CLIENT_DS_ID) + "' and datasrc = 'patientprovider'")
        .drop("GROUPID", "CLIENT_DS_ID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patientprovider")
      .join(dfs("chart"), Seq("CHART_ID"), "left_outer")
      .join(dfs("patient"), Seq("ENTERPRISE_ID"), "left_outer")
      .join(dfs("fileIdDates"), Seq("FILEID"), "inner")
      .crossJoin(dfs("cdr.map_pcp_order"))
  }

  afterJoin = (df: DataFrame) => {
    val df1 = df.withColumn("PATIENTID", coalesce(df("PATIENT_ID_pat"), df("PATIENT_ID_pp")))
      .withColumn("STARTDATE", coalesce(df("CREATED_DATETIME"), df("FILEDATE")))
    val fil = df1.filter("patientid is not null and coalesce(pcp_exclude_flg, 'N') <> 'Y' and incl_val = 'Y'")
    val groups = Window.partitionBy(to_date(fil("STARTDATE")), fil("PATIENTID"))
      .orderBy(fil("PATIENT_PROVIDER_ID").desc, fil("CREATED_DATETIME").desc_nulls_last, fil("FILEDATE").desc_nulls_last)
    fil.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and provider_id is not null and startdate is not null")
      .drop("rn")
  }

  map = Map(
    "DATASRC" -> literal("patientprovider"),
    "LOCALRELSHIPCODE" -> literal("PCP"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PROVIDERID" -> mapFrom("PROVIDER_ID"),
    "STARTDATE" -> mapFrom("STARTDATE"),
    "ENDDATE" -> ((col: String, df: DataFrame) => {
      val load_ppr = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ATHENA_DWF", "CALL_LOAD_PROV_PAT_REL", "PROV_PAT_REL", "N/A")
      df.withColumn(col, when(lit("Y").isin(load_ppr: _*), current_timestamp()).otherwise(df("DELETED_DATETIME")))
    })
  )

}