package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class ProcedureSmrtdtaelem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("cdr.map_custom_proc", "smrtdta_elem")

  columnSelect = Map(
    "smrtdta_elem" -> List("ELEMENT_ID", "PAT_LINK_ID", "CUR_VALUE_DATETIME", "VALUE_LINE", "CONTACT_SERIAL_NUM"),
    "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID","MAPPEDVALUE")
  )

  beforeJoin =
    Map(
      "smrtdta_elem" -> includeIf("CUR_VALUE_DATETIME is not null and PAT_LINK_ID is not null"),
      "cdr.map_custom_proc" -> ((df: DataFrame) => {
        df.filter("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'SMRTDTA_ELEM'").drop("GROUPID").drop("DATASRC")
      })
    )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("smrtdta_elem")
      .join(dfs("cdr.map_custom_proc"),
        concat(lit(config(CLIENT_DS_ID)+"."), dfs("smrtdta_elem")("ELEMENT_ID")) === dfs("cdr.map_custom_proc")("LOCALCODE"),
        "inner")
  }

      map = Map(
        "DATASRC" -> literal("smrtdta_elem"),
        "LOCALCODE" -> mapFrom("ELEMENT_ID",nullIf=Seq(null), prefix=config(CLIENT_DS_ID)+"."),
        "PATIENTID" -> mapFrom("PAT_LINK_ID"),
        "PROCEDUREDATE" -> mapFrom("CUR_VALUE_DATETIME"),
        "PROCSEQ" -> mapFrom("VALUE_LINE"),
        "ENCOUNTERID" -> mapFrom("CONTACT_SERIAL_NUM"),
        "CODETYPE" -> literal("CUSTOM"),
        "MAPPEDCODE" -> mapFrom("MAPPEDVALUE")
      )


afterMap = (df1: DataFrame) => {
  val df=df1.repartition(1000)
  df.distinct
  }

}
// test
// val a = new ProcedureSmrtdtaelem(cfg); val b = build(a); b.show(false) ; b.count ; b.select("ENCOUNTERID").distinct.count
