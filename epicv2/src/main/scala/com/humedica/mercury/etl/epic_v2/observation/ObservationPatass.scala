package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ObservationPatass(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("Temptablepatass:epic_v2.observation.ObservationTemptablepatass",
    "cdr.zcm_obstype_code", "cdr.map_predicate_values")

  beforeJoin=Map(
    "Temptablepatass" -> ((df: DataFrame) => {
      df.filter("MEAS_VALUE is not null and PAT_ID is not null and RECORDED_TIME is not null")
        .withColumnRenamed("MEAS_VALUE", "LOCALRESULT_OBS")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'patass' and GROUPID = '"+config(GROUP)+"'").drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("Temptablepatass")
      .join(dfs("cdr.zcm_obstype_code"),
          concat(lit(config(CLIENT_DS_ID)+".pa."),dfs("Temptablepatass")("FLO_MEAS_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE") ||
            concat(lit(config(CLIENT_DS_ID)+"."),dfs("Temptablepatass")("FLO_MEAS_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE")
      )
  }


  afterJoin = (df: DataFrame) => {
    val flo_meas_id_col = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATASS", "OBSERVATION", "PATASSESSMENT", "FLO_MEAS_ID")
      .replace("'","")
    val groups1 = Window.partitionBy(df("FLO_MEAS_ID"), df("PAT_ID"), df("PAT_ENC_CSN_ID"),df("OBSTYPE")).orderBy(df("RECORDED_TIME").desc)
    val groups2 = Window.partitionBy(df("FLO_MEAS_ID"), df("PAT_ID"), df("PAT_ENC_CSN_ID"),df("RECORDED_TIME"),df("OBSTYPE")).orderBy(df("ENTRY_TIME").desc)
    val addColumn = df.withColumn("rn1", row_number.over(groups1)).withColumn("rn2", row_number.over(groups2))
    addColumn.withColumn("rn" ,when(addColumn("FLO_MEAS_ID") === flo_meas_id_col, addColumn("rn1")).otherwise(addColumn("rn2")))
      .withColumn("LOCALRESULT", substring(when(df("OBSTYPE").isin("CH001834", "CH001835", "CH001836","CH001837","CH001838","CH001839",
        "CH001840", "CH001841", "CH001842", "CH001843", "CH001844", "CH001845", "CH001847", "CH001848","CH001849","CH001850",
        "CH001851","CH001938", "CH001939", "CH001940", "CH002748", "CH001978", "SMOKE_CESS_CONSULT'")
        ,when(df("LOCALRESULT_OBS").isNotNull, concat(lit(config(CLIENT_DS_ID)+"."), df("LOCALRESULT_OBS")))).otherwise(df("LOCALRESULT_OBS")),1,255))
  }

  map = Map(
    "DATASRC" -> literal("patass"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "OBSDATE" -> mapFrom("RECORDED_TIME"),
    "LOCALCODE" -> mapFrom("FLO_MEAS_ID", nullIf = Seq("-1", null), prefix = config(CLIENT_DS_ID)+"."),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = includeIf("OBSTYPE is not null and rn =1 and OBSTYPE <> 'LABRESULT'")

  mapExceptions = Map(
    ("H846629_EP2", "LOCALRESULT") -> ((col: String, df: DataFrame) => {
      val df1 = df.withColumn("SUFFIX", when(df("LOCALRESULT").between(0.117,0.425), lit(" - (Low)")).otherwise(
        when(df("LOCALRESULT").between(0.426,0.625), lit(" - (Medium-Low)")).otherwise(
        when(df("LOCALRESULT").between(0.626,1.101), lit(" - (Medium-High)")).otherwise(
        when(df("LOCALRESULT").between(1.102,1.839), lit(" - (High)")).otherwise(
        when(df("LOCALRESULT").between(1.840,12.971), lit(" - (Complex)")).otherwise(null))))))
    df1.withColumn(col, when((df1("OBSTYPE") === lit("CH002642") && df1("SUFFIX").isNotNull), concat(substring(df1("LOCALRESULT"),1,255),df1("SUFFIX"))).otherwise(df1("LOCALRESULT")))   
    })
  )

}