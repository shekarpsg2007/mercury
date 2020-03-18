package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class UtilDedupedPatientObgynHistory (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  columns=List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "PATIENT_OBGYN_HISTORY_ID", "HUM_TYPE",
    "PATIENT_ID", "CHART_ID", "OBGYN_HISTORY_KEY", "OBGYN_HISTORY_QUESTION", "OBGYN_HISTORY_ANSWER", "CREATED_DATETIME",
    "CREATED_BY", "DELETED_DATETIME", "DELETED_BY", "FD_FILEDATE", "OBGYN_DATE")

  tables = List("patientobgynhistory",
    "fileExtractDates:athena.util.UtilFileIdDates",
    "pat:athena.util.UtilSplitPatient")

  columnSelect = Map(
    "patientobgynhistory" -> List("FILEID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID",
      "PATIENT_OBGYN_HISTORY_ID", "HUM_TYPE", "PATIENT_ID", "CHART_ID", "OBGYN_HISTORY_KEY", "OBGYN_HISTORY_QUESTION",
      "OBGYN_HISTORY_ANSWER", "CREATED_DATETIME", "CREATED_BY", "DELETED_DATETIME", "DELETED_BY"),
    "fileExtractDates" -> List("FILEID","FILEDATE"),
    "pat" -> List("PATIENT_ID")
  )

  beforeJoin = Map(
    "patientobgynhistory" -> ((df: DataFrame) => {
      val obgDf =  df.withColumn("OBGYN_HISTORY_ANSWER_LENG", length(df("OBGYN_HISTORY_ANSWER")))
        .withColumn("OBGYN_HISTORY_ANSWER_2", regexp_replace(df("OBGYN_HISTORY_ANSWER"),"\\,","  "))

      val obgDf2 = coalesceDate(obgDf, "OBGYN_HISTORY_ANSWER_2", "OBGYN_HISTORY_ANSWER_3")


      obgDf2.withColumn("OBGYN_DATE", when(obgDf2("OBGYN_HISTORY_ANSWER_LENG") === 4 && obgDf2("OBGYN_HISTORY_ANSWER").rlike("[0-9]{4}"),
            from_unixtime(unix_timestamp(concat(lit("01/01/"), obgDf2("OBGYN_HISTORY_ANSWER")), "dd/MM/yyyy")))
          .when(obgDf2("OBGYN_HISTORY_ANSWER_LENG") === 6 && length(regexp_replace(obgDf2("OBGYN_HISTORY_ANSWER"),"\\/","")) === 5,
            from_unixtime(unix_timestamp(concat(lit("01/0"), obgDf2("OBGYN_HISTORY_ANSWER")), "dd/MM/yyyy")))
          .when(obgDf2("OBGYN_HISTORY_ANSWER_LENG") === 7 && length(regexp_replace(obgDf2("OBGYN_HISTORY_ANSWER"),"\\/","")) === 6,
            from_unixtime(unix_timestamp(concat(lit("01/"), obgDf2("OBGYN_HISTORY_ANSWER")), "dd/MM/yyyy")))
          .otherwise(obgDf2("OBGYN_HISTORY_ANSWER_3")))

    }),
    "fileExtractDates" -> renameColumn("FILEDATE", "FD_FILEDATE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    val patJoinType = new UtilSplitTable(config).patprovJoinType
    dfs("patientobgynhistory")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
      .join(dfs("pat"), Seq("PATIENT_ID"), patJoinType)
  }

}

// test
//  val a = new UtilDedupedPatientObgynHistory(cfg); val o = build(a); o.show(100, false); o.count
