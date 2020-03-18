package com.humedica.mercury.etl.athena.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class ObservationPatientobgynhistory(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("cdr.zcm_obstype_code", "obgHist:athena.util.UtilDedupedPatientObgynHistory")

  columnSelect = Map(
    "obgHist" -> List("FILEID", "FD_FILEDATE", "OBGYN_HISTORY_QUESTION", "OBGYN_DATE", "PATIENT_ID", "OBGYN_HISTORY_ANSWER", "DELETED_DATETIME"),
    "cdr.zcm_obstype_code" -> List("OBSTYPE", "OBSCODE", "OBSTYPE_STD_UNITS", "LOCALUNIT", "GROUPID", "DATASRC")
  )


  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'patientobgynhistory'"),
    "obgHist" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_ID"), df("OBGYN_DATE"), df("OBGYN_HISTORY_QUESTION"))
        .orderBy(df("FD_FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last, df("CREATED_DATETIME").desc_nulls_last)
      df.withColumn("rw", row_number.over(groups))
        .withColumn("OBSDATE", df("OBGYN_DATE"))
        .filter("rw=1 and NOT (length(from_unixtime(unix_timestamp(OBSDATE,\"yyyy\")))) <= 2")

        .withColumn("LOCALCODE", expr("CASE WHEN UPPER(OBGYN_HISTORY_QUESTION) LIKE '%NOTES%' THEN NULL ELSE OBGYN_HISTORY_QUESTION END"))
        .withColumn("LOCALRESULT", df("OBGYN_HISTORY_ANSWER"))
        .filter("PATIENT_ID is not null and LOCALCODE is not null and LOCALRESULT is not null and OBSDATE is not null and Deleted_Datetime is null")
        .select("PATIENT_ID", "LOCALCODE", "LOCALRESULT", "OBSDATE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("obgHist")
      .join(dfs("cdr.zcm_obstype_code"), dfs("obgHist")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }


  map = Map(
    "DATASRC" -> literal("patientobgynhistory"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "LOCALRESULT" -> mapFrom("LOCALRESULT"),
    "OBSDATE" -> mapFrom("OBSDATE"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

}


// test
//  val ob = new ObservationPatientobgynhistory(cfg); val obs = build(ob); obs.show ; obs.count;

