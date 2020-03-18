package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._

/**
 * Auto-generated on 02/01/2017
 */


class ObservationProblemlist(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("problemlist",
                "zh_v_edg_hx_icd9",
                "zh_edg_curr_icd9",
                "zh_clarity_edg",
                "cdr.zcm_obstype_code")

  columnSelect = Map(
    "problemlist" -> List("DX_ID", "NOTED_DATE", "DATE_OF_ENTRY", "RESOLVED_DATE", "PAT_ID", "PROBLEM_STATUS_C", "UPDATE_DATE", "DX_EXTERNAL_ID"),
    "zh_v_edg_hx_icd9" -> List("DX_ID", "EFF_START_DATE", "EFF_END_DATE", "CODE"),
    "zh_edg_curr_icd9" -> List("DX_ID", "CODE"),
    "zh_clarity_edg" -> List("DX_ID", "RECORD_TYPE_C", "REF_BILL_CODE"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "problemlist" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val fil = df1.filter("coalesce(problem_status_c, '1') <> '3' and pat_id <> '-1'")
      val edg = table("zh_clarity_edg")
      val joined = fil.join(edg, Seq("DX_ID"), "inner")
      val joined1 = joined.withColumn("NOTED_DATE", when(joined("RESOLVED_DATE").isNull || joined("NOTED_DATE") < joined("RESOLVED_DATE"), joined("NOTED_DATE")))
        .withColumn("DATE_OF_ENTRY", when(joined("RESOLVED_DATE").isNull || joined("DATE_OF_ENTRY") < joined("RESOLVED_DATE"), joined("DATE_OF_ENTRY")))
        .withColumn("STATUSCODE", when(joined("PROBLEM_STATUS_C").isNull || joined("PROBLEM_STATUS_C") === "-1", null)
          .otherwise(concat_ws(".", lit(config(CLIENT_DS_ID)), joined("PROBLEM_STATUS_C"))))
        .withColumn("DX_ID", when(joined("DX_ID") === "-1", null).otherwise(joined("DX_ID")))
        .withColumnRenamed("DX_ID", "DX_ID_PL")
      joined1.withColumn("OBSDATE", coalesce(joined1("NOTED_DATE"), joined1("DATE_OF_ENTRY"), joined1("RESOLVED_DATE")))
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUPID) + "' and datasrc = 'problemlist'")
    }),
    "zh_v_edg_hx_icd9" -> ((df: DataFrame) => {
      df.withColumnRenamed("DX_ID", "DX_ID_HX")
        .withColumnRenamed("CODE", "CODE_HX")
    }),
    "zh_edg_curr_icd9" -> ((df: DataFrame) => {
      df.withColumnRenamed("DX_ID", "DX_ID_CURR")
        .withColumnRenamed("CODE", "CODE_CURR")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("problemlist")
      .join(dfs("zh_v_edg_hx_icd9"), dfs("problemlist")("DX_ID_PL") === dfs("zh_v_edg_hx_icd9")("DX_ID_HX") &&
        dfs("problemlist")("OBSDATE").between(dfs("zh_v_edg_hx_icd9")("EFF_START_DATE"), dfs("zh_v_edg_hx_icd9")("EFF_END_DATE")), "left_outer")
      .join(dfs("zh_edg_curr_icd9"), dfs("problemlist")("DX_ID_PL") === dfs("zh_edg_curr_icd9")("DX_ID_CURR"))
  }

  afterJoin = (df: DataFrame) => {
    val df1 = df.withColumn("ICD9", coalesce(df("CODE_HX"), df("CODE_CURR")))
    val fpiv = unpivot(
      Seq("ICD9", "DX_EXTERNAL_ID", "DX_ID_PL"),
      Seq("ICD9", "DX_EXTERNAL_ID", "DX_ID_PL"), typeColumnName = "LOCALVAL")
    val fpiv1 = fpiv("LOCAL_RES", df1)
    val zcm = table("cdr.zcm_obstype_code")
    val joined = fpiv1.join(zcm, concat_ws(".", lit(config(CLIENT_DS_ID)), fpiv1("LOCAL_RES")) === zcm("OBSCODE"), "inner")
    val joined1 = joined.withColumn("LOCAL_RESULT", when(joined("RECORD_TYPE_C").isin("1", "-1") || joined("RECORD_TYPE_C").isNull, joined("LOCAL_RES"))
      .when(joined("REF_BILL_CODE").isin("IMO0001", "000.0"), null)
      .otherwise(joined("REF_BILL_CODE")))
    val groups = Window.partitionBy(joined1("PAT_ID"), joined1("DX_ID_PL"), joined1("LOCAL_RESULT"), joined1("OBSDATE"), joined1("OBSTYPE")).orderBy(joined1("UPDATE_DATE").desc_nulls_last)
    joined1.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and pat_id is not null and obsdate is not null")
      .drop("rn")
  }


  map = Map(
    "DATASRC" -> literal("problemlist"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "OBSDATE" -> mapFrom("OBSDATE"),
    "LOCALCODE" -> mapFrom("LOCAL_RESULT", prefix = config(CLIENT_DS_ID) + "."),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "LOCALRESULT" -> mapFrom("LOCAL_RESULT", prefix = config(CLIENT_DS_ID) + "."),
    "STATUSCODE" -> mapFrom("STATUSCODE")
  )

}