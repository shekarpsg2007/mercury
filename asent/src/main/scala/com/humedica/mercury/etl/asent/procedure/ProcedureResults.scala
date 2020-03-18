package com.humedica.mercury.etl.asent.procedure

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ProcedureResults(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_qo_de",
    "as_results", "cdr.map_custom_proc")

  columnSelect = Map(
    "as_zc_qo_de" -> List("ENTRYNAME", "ID"),
    "as_results" -> List("RESULT_CLINICAL_DATE", "RESULT_DATE", "RESULT_DATE", "RESULT_ID", "RESULT_DATE",
      "RESULT_ID", "RESULT_STATUS_ID", "PATIENT_MRN", "PERFORMED_DATE", "WHO_ORDERED_ID", "PERFORMED_DATE",
      "ENCOUNTER_ID", "CHILD_RESULT_ID", "LAST_UPDATED_DATE"),
    "cdr.map_custom_proc" -> List("MAPPEDVALUE", "LOCALCODE", "DATASRC", "GROUPID")
  )

  beforeJoin = Map(
    "as_results" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ENCOUNTER_ID"), df("PERFORMED_DATE"), df("RESULT_ID")).orderBy(df("LAST_UPDATED_DATE").desc)
      val df2 = df.withColumn("rn", row_number.over(groups))
      df2.filter("rn = '1'").drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_results")
      .join(dfs("as_zc_qo_de"), dfs("as_zc_qo_de")("ID") === dfs("as_results")("RESULT_ID"), "left_outer")
      .join(dfs("cdr.map_custom_proc"), dfs("cdr.map_custom_proc")("LOCALCODE") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_results")("RESULT_ID")) &&
        dfs("cdr.map_custom_proc")("groupid") === lit(config(GROUP)) && dfs("cdr.map_custom_proc")("datasrc") === lit("results"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ENCOUNTER_ID"), df("PERFORMED_DATE"), df("RESULT_ID")).orderBy(df("LAST_UPDATED_DATE").desc)
    val df2 = df.withColumn("rn", row_number.over(groups))
    df2.filter("rn = '1' AND RESULT_STATUS_ID != '6' AND RESULT_ID is not null AND PATIENT_MRN is not null AND PERFORMED_DATE is not null")
  }

  map = Map(
    "DATASRC" -> literal("results"),
    "LOCALCODE" -> mapFrom("RESULT_ID", prefix = config(CLIENT_DS_ID) + "."),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "PROCEDUREDATE" -> mapFromDate("PERFORMED_DATE"),
    "LOCALNAME" -> mapFrom("ENTRYNAME"),
    "ORDERINGPROVIDERID" -> mapFrom("WHO_ORDERED_ID"),
    "ACTUALPROCDATE" -> mapFrom("PERFORMED_DATE", nullIf = Seq("1900-01-01 00:00:00")),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "SOURCEID" -> mapFrom("CHILD_RESULT_ID"),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE")
  )
}
