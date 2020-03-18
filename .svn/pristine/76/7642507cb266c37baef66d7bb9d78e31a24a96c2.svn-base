package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedReviewedSocialHistory (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("PATIENT_ID", "REV_NAME", "OBS_NAME", "SOCIAL_HISTORY_KEY", "SOCIAL_HISTORY_ANSWER", "REVIEWED_DATE",
    "REV_FILE_DATE", "OBS_FILE_DATE", "DELETED_DATETIME")

  tables = List("revieweddate:athena.util.UtilDedupedPatientSocialHistory",
    "obsfiledate:athena.util.UtilDedupedPatientSocialHistory",
    "cdr.zcm_obstype_code")

  columnSelect = Map(
    "obsfiledate" -> List("PATIENT_ID","SOCIAL_HISTORY_KEY","SOCIAL_HISTORY_NAME","SOCIAL_HISTORY_ANSWER","DELETED_DATETIME","FD_FILEDATE"),
    "revieweddate" -> List("PATIENT_ID","SOCIAL_HISTORY_KEY","SOCIAL_HISTORY_NAME","SOCIAL_HISTORY_ANSWER","FD_FILEDATE"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "OBSCODE", "DATASRC", "CUI")
  )


  beforeJoin = Map(
    "obsfiledate"-> ((df: DataFrame) => {
      val obsdf = df.withColumnRenamed("SOCIAL_HISTORY_NAME","OBS_NAME")
                    .withColumnRenamed("FD_FILEDATE","OBS_FILE_DATE")
                    .withColumnRenamed("PATIENT_ID","PATIENT_ID_obs")

      val zcm = table("cdr.zcm_obstype_code")
        .filter("DATASRC = 'patientsocialhistory' and GROUPID = '"+config(GROUP)+"' and cui <> 'CH002048'")
        .select("OBSCODE")

      obsdf.join(zcm, obsdf("SOCIAL_HISTORY_KEY") === zcm("OBSCODE"), "inner")
           .select("PATIENT_ID_obs", "SOCIAL_HISTORY_KEY", "OBS_NAME", "SOCIAL_HISTORY_ANSWER", "OBS_FILE_DATE", "DELETED_DATETIME")
           .distinct()
      }),
    "revieweddate"-> ((df: DataFrame) => {
      df.filter("SOCIAL_HISTORY_KEY = 'REVIEWED.SOCIALHISTORY'")
        .withColumn("REVIEWED_DATE", expr("from_unixtime(unix_timestamp(SOCIAL_HISTORY_ANSWER,\"MM/dd/yyyy\"))").cast("Date"))
        .withColumnRenamed("FD_FILEDATE", "REV_FILE_DATE")
        .withColumnRenamed("SOCIAL_HISTORY_NAME", "REV_NAME")
        .select("PATIENT_ID", "REVIEWED_DATE", "REV_FILE_DATE", "REV_NAME")
        .distinct()

    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("revieweddate")
      .join(dfs("obsfiledate"), dfs("revieweddate")("PATIENT_ID") === dfs("obsfiledate")("PATIENT_ID_obs") && dfs("revieweddate")("REVIEWED_DATE") >= dfs("obsfiledate")("OBS_FILE_DATE"), "inner")
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENT_ID"), df("REV_FILE_DATE"), df("OBS_NAME"))
      .orderBy(df("REVIEWED_DATE").desc, df("OBS_FILE_DATE").desc, df("OBS_NAME").desc)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw = 1")
  }

}


// test
//  val a = new UtilDedupedReviewedSocialHistory(cfg); val o = build(a); o.count

