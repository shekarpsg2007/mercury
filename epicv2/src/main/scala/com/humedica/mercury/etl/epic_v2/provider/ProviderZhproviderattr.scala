package com.humedica.mercury.etl.epic_v2.provider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by bhenriksen on 1/18/17.
 */
class ProviderZhproviderattr(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  //prov_id, taking latest extract_date

  tables = List("zh_providerattr",
    "zh_identity_ser_id",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "zh_providerattr" -> List("PROV_ID", "CLINICIAN_TITLE", "BIRTH_DATE", "PROV_NAME", "SEX", "DOCTORS_DEGREE", "STAFF_RESOURCE", "EXTRACT_DATE"),
    "zh_identity_ser_id" -> List("IDENTITY_TYPE_ID", "IDENTITY_ID", "PROV_ID", "LINE")
  )

  beforeJoin = Map(
    "zh_providerattr" -> includeIf("PROV_NAME != '*' And PROV_NAME <> '-1'"),
    "zh_identity_ser_id" -> ((df: DataFrame) => {
      val mpv1 = mpv(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ZH_IDENTITY_SER_ID", "ZH_PROVIDER", "ZH_IDENTITY_SER_ID", "IDENTITY_TYPE_ID")
      val mpv2 = mpv1.select("COLUMN_VALUE")
      val joined = df.join(mpv2, df("IDENTITY_TYPE_ID") === mpv2("COLUMN_VALUE"), "inner")
      val groups = Window.partitionBy(joined("PROV_ID")).orderBy(joined("LINE").desc)
      val addColumn = joined.withColumn("rank_npi", row_number.over(groups))
      addColumn.filter("rank_npi = 1").drop("rank_npi")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_providerattr")
      .join(dfs("zh_identity_ser_id"), Seq("PROV_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("zh_providerattr"),
    "DOB" -> mapFrom("BIRTH_DATE"),
    "CREDENTIALS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(isnull(df("DOCTORS_DEGREE")), df("CLINICIAN_TITLE"))
        .when(df("CLINICIAN_TITLE").isNotNull, concat_ws(",", df("CLINICIAN_TITLE"), df("DOCTORS_DEGREE")))
        .otherwise(df("DOCTORS_DEGREE")))
    }),
    "GENDER" -> mapFrom("SEX"),
    "LOCALPROVIDERID" -> mapFrom("PROV_ID"),
    "PROVIDERNAME" -> mapFrom("PROV_NAME"),
    "NPI" -> mapFrom("IDENTITY_ID")
  )


  afterMap = (df: DataFrame) => {
    val list_resource_excl = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),"ZH_PROVIDERATTR", "PROVIDER", "ZH_PROVIDERATTR", "STAFF_RESOURCE")
    val groups = Window.partitionBy(df("PROV_ID")).orderBy(df("EXTRACT_DATE").desc)
    val addColumn = df.withColumn("rownumber", row_number.over(groups))
    addColumn.filter("rownumber = 1 and coalesce(STAFF_RESOURCE,'X') not in (" + list_resource_excl + ") " +
      "and LOCALPROVIDERID is not null")
  }

}

