package com.humedica.mercury.etl.asent.observation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationHmdeferrals(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_health_maintenance", "cdr.zcm_obstype_code")

  columnSelect = Map(
    "as_health_maintenance" -> List("ENTRYNAME", "RECORDEDDTTM", "PATIENT_MRN", "DEFERRALREASONDE", "LASTUPDATEDTTM", "HMPSTATUSDE"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "DATATYPE")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'hm_deferrals'"),
    "as_health_maintenance" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_MRN"), df("RECORDEDDTTM"), df("ENTRYNAME"), df("DEFERRALREASONDE")).orderBy(df("LASTUPDATEDTTM").desc)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1").drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_health_maintenance")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("obscode") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_health_maintenance")("ENTRYNAME")), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENT_MRN"), df("RECORDEDDTTM"), df("ENTRYNAME"), df("DEFERRALREASONDE"), df("DEFERRALREASONDE")).orderBy(df("LASTUPDATEDTTM").desc)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn=1 AND DEFERRALREASONDE !='0' AND HMPSTATUSDE !='3' AND PATIENT_MRN is not null").drop("rn")
  }

  map = Map(
    "DATASRC" -> literal("hm_deferrals"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "OBSDATE" -> mapFrom("RECORDEDDTTM"),
    "LOCALCODE" -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("ENTRYNAME")))),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "LOCALRESULT" -> ((col, df) => df.withColumn(col, when(df("DEFERRALREASONDE").isNotNull && df("DATATYPE") === lit("CV"), concat_ws(".", lit(config(CLIENT_DS_ID)), df("DEFERRALREASONDE"))).otherwise(df("DEFERRALREASONDE"))))
  )

}