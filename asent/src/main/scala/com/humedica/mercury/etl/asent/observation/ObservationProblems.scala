package com.humedica.mercury.etl.asent.observation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationProblems(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_problems",
    "cdr.zcm_obstype_code",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "as_problems" -> List("PROBLEM_DESCRIPTION", "PROBLEM_DESCRIPTION_ID", "RECORDED_DATE", "PATIENT_MRN", "PROBLEM_STATUS_ID", "ENCOUNTER_ID", "PROBLEM_DESCRIPTION", "PROBLEM_CATEGORY_ID"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "DATATYPE")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'problems'"),
    "as_problems" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("PROBLEM_DESCRIPTION_ID")).orderBy(df("RECORDED_DATE").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1 AND (PROBLEM_CATEGORY_ID IN ('0','9','10','19','20','21','22','23','28','36','37','38','42','43','55','67', '5','7','40','30','25','39') OR PROBLEM_CATEGORY_ID IS NULL)" +
          "AND PROBLEM_STATUS_ID <> '6' AND PATIENT_MRN IS NOT NULL AND RECORDED_DATE IS NOT NULL").drop("rn")
    }))

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_problems")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("obscode") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_problems")("problem_description_id")), "inner")
  }


  afterJoin = (df: DataFrame) => {
    val groups2 = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("PROBLEM_DESCRIPTION_ID"), df("OBSTYPE")).orderBy(df("RECORDED_DATE").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups2))
      .filter("rw=1").drop("rw")
  }

  map = Map(
    "DATASRC" -> literal("problems"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "OBSDATE" -> mapFrom("RECORDED_DATE"),
    "LOCALCODE" -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("PROBLEM_DESCRIPTION_ID")))),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "STATUSCODE" -> mapFrom("PROBLEM_STATUS_ID"),
    "LOCALRESULT" -> ((col, df) => {
      val useProbStat = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "OBSERVATION", "OBSERVATION", "PROBLEMS", "RESULT_CONCAT")
      val tempLocalResult = if (useProbStat.contains('Y')) concat_ws(".", df("PROBLEM_STATUS_ID"), df("PROBLEM_DESCRIPTION")) else df("PROBLEM_DESCRIPTION")
      df.withColumn(col, when(tempLocalResult.isNotNull && df("DATATYPE").equalTo("CV"), concat_ws(".", lit(config(CLIENT_DS_ID)), tempLocalResult)).otherwise(tempLocalResult))
    })
  )

}