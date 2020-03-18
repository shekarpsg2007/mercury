package com.humedica.mercury.etl.asent.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class ProcedureHealthmaintenance(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_health_maintenance", "cdr.map_custom_proc")

  columnSelect = Map(
    "as_health_maintenance" -> List("HMPITEMDE", "HMPSTATUSDE", "PATIENT_MRN", "LASTDONEDTTM", "ENTRYNAME", "LASTUPDATEDTTM", "DEFERRALREASONDE", "PERFORMEDDTTM"),
    "cdr.map_custom_proc" -> List("MAPPEDVALUE", "LOCALCODE", "DATASRC", "GROUPID")
  )

  beforeJoin = Map(
    "as_health_maintenance" -> ((df: DataFrame) => {
      val df2 = df.withColumn("LOCALCODE_hm", when(coalesce(df("HMPITEMDE"), lit("0")) =!= "0", df("HMPITEMDE")).otherwise(
        when(coalesce(df("ENTRYNAME"), lit("0")) =!= "0", df("ENTRYNAME")).otherwise(null))).withColumn("SYSDATE", current_timestamp)
      val groups = Window.partitionBy(df2("PATIENT_MRN"), df2("LOCALCODE_hm"), df2("LASTDONEDTTM")).orderBy(df2("LASTUPDATEDTTM").desc)
      val uni_main = df2.withColumn("rn", row_number.over(groups)).filter("rn = '1'")
      uni_main.filter("HMPSTATUSDE is not null and HMPSTATUSDE not in ('3','8','9') and DEFERRALREASONDE = '0' and date_format(LASTDONEDTTM, 'YYYY-MM-DD') <> '1900-01-01' and LASTDONEDTTM < SYSDATE")
    }),
    "cdr.map_custom_proc" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'Health Maintenance'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_health_maintenance")
      .join(dfs("cdr.map_custom_proc"), concat(lit(config(CLIENT_DS_ID) + "."), dfs("as_health_maintenance")("LOCALCODE_hm")) === dfs("cdr.map_custom_proc")("localcode"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val fil = df.filter("LOCALCODE_hm is not null")
    val groups = Window.partitionBy(fil("PATIENT_MRN"), fil("LOCALCODE_hm"), fil("LASTDONEDTTM"), fil("MAPPEDVALUE")).orderBy(fil("LASTUPDATEDTTM").desc_nulls_last)
    fil.withColumn("rn", row_number.over(groups)).filter("rn = '1'")
  }

  map = Map(
    "DATASRC" -> literal("Health Maintenance"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "LOCALCODE" -> mapFrom("LOCALCODE_hm", prefix = config(CLIENT_DS_ID) + "."),
    "PROCEDUREDATE" -> mapFromDate("LASTDONEDTTM"),
    "LOCALNAME" -> mapFrom("ENTRYNAME"),
    "ACTUALPROCDATE" -> mapFrom("LASTDONEDTTM"),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE")
  )
}