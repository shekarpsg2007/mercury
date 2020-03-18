package com.humedica.mercury.etl.asent.observation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationHealthmaintenance(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "as_health_maintenance",
    "cdr.zcm_obstype_code"
  )

  columnSelect = Map(
    "as_health_maintenance" -> List("HMPITEMDE", "LASTDONEDTTM", "PATIENT_MRN", "HMPSTATUSDE", "ENTRYNAME", "RECORDEDDTTM", "DEFERRALREASONDE", "LASTUPDATEDTTM"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "DATATYPE")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'Health Maintenance'"),
    "as_health_maintenance" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_MRN"), df("RECORDEDDTTM"), df("ENTRYNAME"), df("DEFERRALREASONDE")).orderBy(df("LASTUPDATEDTTM").desc_nulls_last)
      val fil = df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1 AND HMPSTATUSDE NOT IN (3,8,9) AND LASTDONEDTTM <= CURRENT_DATE AND (DEFERRALREASONDE = '0' OR DEFERRALREASONDE IS NULL)")
        .filter(from_unixtime(unix_timestamp(df("LASTDONEDTTM"))) =!= lit("1900-01-01 00:00:00")).drop("rn")
      fil.withColumn("LOCALCODE", when(fil("HMPITEMDE").isNotNull && fil("HMPITEMDE") =!= lit("0"), concat_ws(".", lit(config(CLIENT_DS_ID)), fil("HMPITEMDE")))
        .otherwise(concat_ws(".", lit(config(CLIENT_DS_ID)), fil("ENTRYNAME"))))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_health_maintenance")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("obscode") === dfs("as_health_maintenance")("LOCALCODE"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    df.withColumn("LOCALRESULT", coalesce(df("HMPITEMDE"), df("ENTRYNAME")))
  }

  map = Map(
    "DATASRC" -> literal("Health Maintenance"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "OBSDATE" -> mapFrom("LASTDONEDTTM"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "LOCALRESULT" -> ((col, df) => df.withColumn(col, when(df("LOCALRESULT").isNotNull && df("DATATYPE") === lit("CV"), concat_ws(".", lit(config(CLIENT_DS_ID)), df("LOCALRESULT"))).otherwise(df("LOCALRESULT")))),
    "STATUSCODE" -> mapFrom("HMPSTATUSDE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("OBSDATE"), df("LOCALRESULT"), df("OBSTYPE")).orderBy(df("LASTUPDATEDTTM").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw=1")
  }

}