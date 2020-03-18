package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class ProcedureImmunizations(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("cdr.map_custom_proc", "immunizations")

  columnSelect = Map(
    "immunizations" -> List("IMMUNZATN_ID", "PAT_ID", "IMMUNE_DATE", "NAME", "GIVEN_BY_USER_ID", "IMMUNE_ID",
      "UPDATE_DATE", "IMMNZTN_STATUS_C"),
    "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID","MAPPEDVALUE")
  )

  beforeJoin = Map(
    "immunizations" -> includeIf("IMMUNE_DATE is not null AND PAT_ID is not null"),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'IMMUNIZATIONS'")
        .drop("GROUPID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("immunizations")
      .join(dfs("cdr.map_custom_proc"),
        concat(lit(config(CLIENT_DS_ID) + "."), dfs("immunizations")("IMMUNZATN_ID")) === dfs("cdr.map_custom_proc")("LOCALCODE"),"inner")
  }

  map = Map(
    "DATASRC" -> literal("immunizations"),
    "LOCALCODE" -> mapFrom("IMMUNZATN_ID", prefix = config(CLIENT_DS_ID) + "."),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("IMMUNE_DATE"),
    "LOCALNAME" -> mapFrom("NAME"),
    "ACTUALPROCDATE" -> mapFrom("IMMUNE_DATE"),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "PERFORMINGPROVIDERID" -> mapFrom("GIVEN_BY_USER_ID", nullIf = Seq("-1"))
  )

  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("IMMUNE_ID")).orderBy(df1("UPDATE_DATE").desc_nulls_last)
    df1.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and immnztn_status_c = '1'")
  }

}