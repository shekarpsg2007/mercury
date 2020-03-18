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


class ProviderpatientrelationCustomdemographics(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("customdemographics",
    "fileIdDates:athena.util.UtilFileIdDates",
    "cdr.map_pcp_order",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "customdemographics" -> List("PATIENT_ID", "CUSTOM_FIELD_NAME", "CUSTOM_FIELD_TYPE", "CUSTOM_FIELD_VALUE",
      "CLIENT_RECORD_NUMBER_ID", "FILEID"),
    "fileIdDates" -> List("FILEID", "FILEDATE"),
    "cdr.map_pcp_order" -> List("GROUPID", "CLIENT_DS_ID", "DATASRC", "PCP_EXCLUDE_FLG")
  )

  beforeJoin = Map(
    "customdemographics" -> ((df: DataFrame) => {
      val cust_field_types = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CUSTOMDEMOGRAPHICS", "&custom_field_types", "PROV_PAT_REL", "CUSTOM_FIELD_NAME")
      df.withColumn("PROVIDERID", concat(when(df("CUSTOM_FIELD_TYPE") === lit("REFERRINGPROVIDER"), lit("rp."))
          .when(df("CUSTOM_FIELD_TYPE") === lit("CLINICALPROVIDER"), lit("cp."))
          .otherwise(lit("")),
        df("CUSTOM_FIELD_VALUE")))
        .filter("custom_field_name = 'Primary Care Physician' or custom_field_type in (" + cust_field_types + ")")
    }),
    "cdr.map_pcp_order" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUPID) + "' and client_ds_id = '" + config(CLIENT_DS_ID) + "' and datasrc = 'customdemographics'")
        .drop("GROUPID", "CLIENT_DS_ID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("customdemographics")
      .join(dfs("fileIdDates"), Seq("FILEID"), "inner")
      .crossJoin(dfs("cdr.map_pcp_order"))
  }

  afterJoin = includeIf("coalesce(pcp_exclude_flg, 'N') <> 'Y'")

  map = Map(
    "DATASRC" -> literal("customdemographics"),
    "LOCALRELSHIPCODE" -> literal("PCP"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "PROVIDERID" -> mapFrom("PROVIDERID"),
    "STARTDATE" -> mapFrom("FILEDATE"),
    "ENDDATE" -> ((col: String, df: DataFrame) => {
      val load_ppr = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ATHENA_DWF", "CALL_LOAD_PROV_PAT_REL", "PROV_PAT_REL", "N/A")
      df.withColumn(col, when(lit("Y").isin(load_ppr: _*), current_timestamp()).otherwise(null))
    })
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("PROVIDERID"), df("STARTDATE"))
      .orderBy(df("CLIENT_RECORD_NUMBER_ID").desc)

    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and patientid is not null and providerid is not null and startdate is not null")
      .drop("rn")
  }

}