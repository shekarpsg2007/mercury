package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class ProcedurePatass(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "temptablepatass:epic_v2.observation.ObservationTemptablepatass",
    "zh_date_dimension", 
    "cdr.map_custom_proc",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "temptablepatass" -> List("FLO_MEAS_ID","MEAS_VALUE","ENTRY_TIME","FLO_MEAS_NAME","PAT_ENC_CSN_ID","PAT_ID","RECORDED_TIME"),
    "zh_date_dimension" -> List("EPIC_DTE","CALENDAR_DT"),
    "cdr.map_custom_proc" -> List("DATASRC", "LOCALCODE", "CODETYPE", "MAPPEDVALUE", "GROUPID")
    )

  beforeJoin = Map(
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'PATASSESSMENT'").drop("GROUPID")
    })  
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptablepatass")
      .join(dfs("cdr.map_custom_proc"),
        dfs("cdr.map_custom_proc")("LOCALCODE") === concat(lit(config(CLIENT_DS_ID) + "."), dfs("temptablepatass")("FLO_MEAS_ID")), "inner")
      .join(dfs("zh_date_dimension"), dfs("temptablepatass")("MEAS_VALUE") === dfs("zh_date_dimension")("EPIC_DTE"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val patass_excl = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATASSESSMENT", "PROCEDUREDO", "PATASSESSMENT", "EXCLUSION")
    val no_excl = df.filter("FLO_MEAS_ID not in (" + patass_excl + ")")
    val yes_excl = df.filter("FLO_MEAS_ID in (" + patass_excl + ")")
    val fil_no = no_excl.filter("PAT_ID is not null and " +
      "(lower(MEAS_VALUE) is null or (lower(MEAS_VALUE) not in ('incomplete','n','no','not in season','ordered','referred','unsure') and " +
      "(lower(MEAS_VALUE) not like '%cancelled%' and lower(MEAS_VALUE) not like '%decline%' and lower(MEAS_VALUE) not like '%never%' " +
      "and lower(MEAS_VALUE) not like '%not applicable%' and lower(MEAS_VALUE) not like '%not appropriate%' " +
      "and lower(MEAS_VALUE) not like '%not indicated%' and lower(MEAS_VALUE) not like '%refuse%' and lower(MEAS_VALUE) not like '%unable%' " +
      "and lower(MEAS_VALUE) not like '%unknown%')))")
    val fil_yes = yes_excl.filter("PAT_ID is not null")  
    fil_no.union(fil_yes)
  }

  map = Map(
    "DATASRC" -> literal("patass"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> ((col: String, df: DataFrame) => {
      val list_flo_meas_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATASSESSMENT", "PROCEDUREDO", "DATE_DIMENSION", "FLO_MEAS_ID")
      df.withColumn(col, when(df("FLO_MEAS_ID").isin(list_flo_meas_id: _*), df("CALENDAR_DT")).otherwise(df("RECORDED_TIME")))
      }),
    "LOCALCODE" -> mapFrom("FLO_MEAS_ID", nullIf = Seq(null), prefix = config(CLIENT_DS_ID) + "."),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "LOCALNAME" -> mapFrom("FLO_MEAS_NAME"),
    "ACTUALPROCDATE" -> ((col: String, df: DataFrame) => {
      val list_flo_meas_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATASSESSMENT", "PROCEDUREDO", "DATE_DIMENSION", "FLO_MEAS_ID")
      df.withColumn(col, when(df("FLO_MEAS_ID").isin(list_flo_meas_id: _*), df("CALENDAR_DT")).otherwise(df("RECORDED_TIME")))
      })
  )
  
  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("PROCEDUREDATE"), df("LOCALCODE"), df("MAPPEDCODE")).orderBy(df("ENTRY_TIME").desc_nulls_last)
    val addColumn = df.withColumn("rownbr", row_number.over(groups))
    addColumn.filter("rownbr = 1 and PROCEDUREDATE is not null")
  }  
  
}