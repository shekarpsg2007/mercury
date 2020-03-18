package com.humedica.mercury.etl.epic_v2.labresult

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 02/02/2017
  */


class LabresultPatass(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List("Temptablepatass:epic_v2.observation.ObservationTemptablepatass",
    "cdr.zcm_obstype_code", "cdr.map_predicate_values", "cdr.map_unit", "cdr.unit_remap",
    "cdr.metadata_lab", "cdr.unit_conversion")

  beforeJoin = Map(
    "Temptablepatass" -> ((df: DataFrame) => {
      df.filter("MEAS_VALUE is not null and PAT_ID is not null and RECORDED_TIME is not null")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'patass' and GROUPID = '" + config(GROUP) + "'").drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("Temptablepatass")
      .join(dfs("cdr.zcm_obstype_code"),
        concat(lit(config(CLIENT_DS_ID) + ".pa."), dfs("Temptablepatass")("FLO_MEAS_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE") ||
          concat(lit(config(CLIENT_DS_ID) + "."), dfs("Temptablepatass")("FLO_MEAS_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE")
      )
  }


  afterJoin = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("FLO_MEAS_ID"), df1("PAT_ID"), df1("PAT_ENC_CSN_ID"), df1("RECORDED_TIME"), df1("OBSTYPE")).orderBy(df1("RECORDED_TIME").desc)
    val addColumn = df1.withColumn("rn", row_number.over(groups))
      .withColumn("LOCALRESULT_25", when(!substring(df("MEAS_VALUE"), 1, 254).rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
        when(locate(" ", substring(df("MEAS_VALUE"), 1, 254), 25) === 0, expr("substr(MEAS_VALUE,1,length(MEAS_VALUE))"))
          .otherwise(expr("substr(MEAS_VALUE,1,locate(' ', MEAS_VALUE, 25))"))).otherwise(null))
      .withColumn("LOCALRESULT_NUMERIC", when(df("MEAS_VALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), df("MEAS_VALUE")).otherwise(null))
    addColumn.filter("rn =1").drop("rn")
  }

  map = Map(
    "DATASRC" -> literal("patass"),
    "LABRESULTID" -> ((col: String, df: DataFrame) => df.withColumn(col, concat_ws("_", df("FSD_ID"), df("LINE")))),
    "LOCALCODE" -> mapFrom("FLO_MEAS_ID", prefix = config(CLIENT_DS_ID) + ".pa.", nullIf = Seq("-1")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALNAME" -> mapFrom("FLO_MEAS_NAME"),
    "LOCALUNITS" -> mapFrom("UNIT"),
    "NORMALRANGE" -> ((col: String, df: DataFrame) => df.withColumn(col, concat_ws("-", df("MIN_VALUE"), df("MAX_VALUE")))),
    "DATEAVAILABLE" -> mapFrom("RECORDED_TIME"),
    "LOCALRESULT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("MEAS_VALUE"), 1, 254))
    }),
    "LOCALUNITS_INFERRED" -> labresults_extract_uom(),
    "LOCALRESULT_INFERRED" -> extract_value(),
    "RELATIVEINDICATOR" -> labresults_extract_relativeindicator(),
    "LABRESULT_DATE" -> cascadeFrom(Seq("RECORDED_TIME", "ENTRY_TIME"))
  )


  afterMap = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("FSD_ID"), df1("LINE")).orderBy(df1("DATEAVAILABLE").desc)
    val addColumn = df1.withColumn("rn", row_number.over(groups))
    val deduped = addColumn.filter("rn = 1 and OBSTYPE is not null and OBSTYPE = 'LABRESULT'")

    val map_unit = table("cdr.map_unit").withColumnRenamed("CUI", "CUI_MU").withColumnRenamed("LOCALCODE", "LOCALCODE_MU")
    val unit_remap = table("cdr.unit_remap").withColumnRenamed("UNIT", "UNIT_UR").filter("GROUPID='" + config(GROUP) + "'").drop("GROUPID")
    val metadata_lab = table("cdr.metadata_lab").withColumnRenamed("UNIT", "UNIT_ML")
    val unit_conversion = table("cdr.unit_conversion")

    val df_with_cdr = deduped.join(map_unit, lower(coalesce(deduped("LOCALUNITS"), deduped("LOCALUNITS_INFERRED"))) === map_unit("LOCALCODE_MU"), "left_outer")
      .join(unit_remap, deduped("MAPPEDCODE") === unit_remap("LAB") && (coalesce(map_unit("CUI_MU"), lit("CH999999")) === unit_remap("REMAP")), "left_outer")
      .join(metadata_lab, deduped("MAPPEDCODE") === metadata_lab("CUI"), "left_outer")
      .join(unit_conversion, unit_conversion("SRC_UNIT") === coalesce(unit_remap("UNIT_UR"), map_unit("CUI_MU")) && (unit_conversion("DEST_UNIT") === metadata_lab("UNIT_ML")), "left_outer")

    df_with_cdr.withColumn("MAPPEDUNITS", when(deduped("LOCALRESULT_NUMERIC").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), null).otherwise(coalesce(df_with_cdr("UNIT_UR"), df_with_cdr("CUI_MU"))))
  }
}
