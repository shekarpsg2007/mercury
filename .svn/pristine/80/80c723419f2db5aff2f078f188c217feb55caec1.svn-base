package com.humedica.mercury.etl.asent.observation


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationResults(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_result_status_de",
    "as_results",
    "cdr.zcm_obstype_code")

  columnSelect = Map(
    "as_zc_result_status_de" -> List("ENTRYNAME", "ID"),
    "as_results" -> List("RESULT_NAME", "RESULT_DATE", "PATIENT_MRN", "UNITS", "ENCOUNTER_ID", "ANSWER_DE_T", "RESULT_STATUS_ID", "RESULT_VALUE", "LAST_UPDATED_DATE", "CHILD_RESULT_ID"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "DATATYPE")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'results'"),

    "as_results" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("RESULT_NAME"), df("RESULT_VALUE"), df("RESULT_DATE")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1 AND RESULT_STATUS_ID != '6' AND PATIENT_MRN IS NOT NULL AND RESULT_DATE IS NOT NULL").drop("rn")
    }
      ))


  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_results")
      .join(dfs("as_zc_result_status_de"), dfs("as_zc_result_status_de")("id") === dfs("as_results")("result_status_id"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("obscode") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_results")("result_name")), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("OBSCODE"), df("OBSTYPE"), df("RESULT_DATE"))
      .orderBy(df("LAST_UPDATED_DATE").desc_nulls_last, df("RESULT_DATE").desc_nulls_last, df("CHILD_RESULT_ID").desc_nulls_last)

    df.withColumn("rw", row_number.over(groups))
      .filter("rw=1 and ").drop("rw")
  }

  map = Map(
    "DATASRC" -> literal("results"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "OBSDATE" -> mapFrom("RESULT_DATE"),
    "LOCALCODE" -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("RESULT_NAME")))),
    "LOCALRESULT" -> ((col, df) => {
      val tempLocalResult = coalesce(df("ANSWER_DE_T"), df("RESULT_VALUE"))
      df.withColumn(col, when(tempLocalResult.isNotNull && df("DATATYPE") === lit("CV"), concat_ws(".", lit(config(CLIENT_DS_ID)), tempLocalResult)).otherwise(tempLocalResult))
    }),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> cascadeFrom(Seq("UNITS", "LOCALUNIT")),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "STATUSCODE" -> mapFrom("ENTRYNAME")
  )


}


// Test
// val bobs = new ObservationResults(cfg) ; val obs = build(bobs) ; obs.show(false); obs.count


