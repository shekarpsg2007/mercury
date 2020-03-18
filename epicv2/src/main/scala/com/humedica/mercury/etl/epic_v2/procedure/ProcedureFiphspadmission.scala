package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Engine
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

/**
  * Created by mschlomka on 6/7/18
  */

class ProcedureFiphspadmission(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("f_ip_hsp_admission", "cdr.map_custom_proc")

  columnSelect = Map(
    "f_ip_hsp_admission" -> List("HOSP_ADM_DTTM", "HOSP_DISCH_DTTM", "ADM_REQUIRED_COUNT", "ADM_RECONCILED_COUNT",
      "DISCH_REQUIRED_COUNT", "DISCH_RECONCILED_COUNT", "PAT_ID", "PAT_ENC_CSN_ID"),
    "cdr.map_custom_proc" -> List("GROUPID", "DATASRC", "LOCALCODE", "CODETYPE", "MAPPEDVALUE")
  )

  beforeJoin = Map(
    "f_ip_hsp_admission" -> ((df: DataFrame) =>{
      val adm = df.filter("adm_required_count = adm_reconciled_count")
        .withColumnRenamed("HOSP_ADM_DTTM", "PROCEDUREDATE")
        .select("PAT_ID", "PAT_ENC_CSN_ID", "PROCEDUREDATE")
      val disch = table("f_ip_hsp_admission").filter("disch_required_count = disch_reconciled_count")
        .withColumnRenamed("HOSP_DISCH_DTTM", "PROCEDUREDATE")
        .select("PAT_ID", "PAT_ENC_CSN_ID", "PROCEDUREDATE")
      adm.union(disch)
        .filter("pat_id is not null and proceduredate is not null")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("f_ip_hsp_admission")
      .join(dfs("cdr.map_custom_proc"), dfs("cdr.map_custom_proc")("GROUPID") === lit(config(GROUPID))
        && dfs("cdr.map_custom_proc")("DATASRC") === lit("f_ip_hsp_admission")
        && dfs("cdr.map_custom_proc")("LOCALCODE") === lit(config(CLIENT_DS_ID) + ".F_IP_HSP_ADMISSION.MEDREC"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("f_ip_hsp_admission"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("PROCEDUREDATE"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALCODE" -> literal(config(CLIENT_DS_ID) + ".F_IP_HSP_ADMISSION.MEDREC"),
    "CODETYPE" -> mapFrom("CODETYPE"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "HOSP_PX_FLAG" -> literal("Y"),
    "ACTUALPROCDATE" -> mapFrom("PROCEDUREDATE")
  )

  afterMap = (df1: DataFrame) => {
    val df=df1.repartition(1000)
    val cols = Engine.schema.getStringList("Procedure").asScala.map(_.split("-")(0).toUpperCase())
    df.select(cols.map(col): _*).distinct
  }

}