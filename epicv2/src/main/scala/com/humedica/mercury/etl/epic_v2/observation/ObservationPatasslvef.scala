package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Auto-generated on 02/01/2017
  */


class ObservationPatasslvef(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patassessment", "cdr.zcm_obstype_code", "zh_date_dimension", "cdr.map_predicate_values")


  columnSelect = Map(
    "patassessment" -> List("PAT_ENC_CSN_ID", "FSD_ID", "MEAS_VALUE", "PAT_ID", "RECORDED_TIME", "FLO_MEAS_ID", "ENTRY_TIME"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "LOCALUNIT", "GROUPID", "DATATYPE"),
    "zh_date_dimension" -> List("EPIC_DTE", "CALENDAR_DT")
  )


  beforeJoin = Map(
    "patassessment" -> ((df: DataFrame) => {
      val flo_meas_id_col_a = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATASSESSMENT", "OBSERVATION", "OBSERVATION", "FLO_MEAS_ID")
      val flo_meas_id_col_b = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATASSESSMENTB", "OBSERVATION", "OBSERVATION", "FLO_MEAS_ID")
      val filA = df.filter("meas_value is not null and pat_id is not null and recorded_time is not null" +
        " and flo_meas_id in (" + flo_meas_id_col_a + ")")
      val patassB = table("patassessment")
        .withColumnRenamed("FLO_MEAS_ID", "FLO_MEAS_ID_B")
        .withColumnRenamed("MEAS_VALUE", "MEAS_VALUE_B")
        .select("FLO_MEAS_ID_B", "MEAS_VALUE_B", "PAT_ENC_CSN_ID", "FSD_ID")
      val filB = patassB.filter("flo_meas_id_b in (" + flo_meas_id_col_b + ")")
      filA.join(filB, Seq("PAT_ENC_CSN_ID", "FSD_ID"), "inner")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
        df.filter("DATASRC = 'patass_lvef' and GROUPID = '"+config(GROUP)+"'").drop("GROUPID")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("patassessment")
      .join(dfs("zh_date_dimension"), dfs("patassessment")("MEAS_VALUE_B") === dfs("zh_date_dimension")("EPIC_DTE"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), concat(lit(config(CLIENT_DS_ID) + "."), dfs("patassessment")("FLO_MEAS_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val flo_meas_id_col_a = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATASSESSMENT", "OBSERVATION", "OBSERVATION", "FLO_MEAS_ID")
    df.withColumn("DATECOL", when(expr("FLO_MEAS_ID in ("+ flo_meas_id_col_a +")"), df("CALENDAR_DT"))
      .otherwise(df("RECORDED_TIME")))
  }
  
  map = Map(
    "DATASRC" -> literal("patass_lvef"),
    "LOCALCODE" -> mapFrom("FLO_MEAS_ID", prefix=config(CLIENT_DS_ID) + ".", nullIf = Seq("-1")),
    "OBSDATE" -> mapFrom("DATECOL"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "LOCALRESULT" -> ((col:String, df:DataFrame) =>
      df.withColumn(col, when(df("DATATYPE") === lit("CV"), concat(lit(config(CLIENT_DS_ID) + "."), substring(df("MEAS_VALUE"),1,254)))
        .otherwise(substring(df("MEAS_VALUE"),1,254)))
      )
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("FLO_MEAS_ID"), df("PAT_ID"), df("PAT_ENC_CSN_ID"), df("RECORDED_TIME"), df("OBSTYPE"))
      .orderBy(df("ENTRY_TIME").desc_nulls_last,df("RECORDED_TIME").desc_nulls_last,df("CALENDAR_DT").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and obsdate is not null")
      .drop("rn")
  }

}