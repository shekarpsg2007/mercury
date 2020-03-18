package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Engine
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.JavaConverters._

class ProcedureSmrtdtaelempairs(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("cdr.map_custom_proc", "smrtdta_elem", "cdr.map_predicate_values")

  columnSelect = Map(
    "smrtdta_elem" -> List("ELEMENT_ID", "PAT_LINK_ID", "CUR_VALUE_DATETIME", "VALUE_LINE", "CONTACT_SERIAL_NUM", "SMRTDTA_ELEM_VALUE", "HLV_ID", "UPDATE_DATE", "FILEID"),
    "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID", "MAPPEDVALUE"),
    "cdr.map_predicate_values" -> List("GROUPID", "CLIENT_DS_ID", "DATA_SRC", "ENTITY", "TABLE_NAME", "COLUMN_NAME", "COLUMN_VALUE")
  )

  beforeJoin =
    Map(
      "smrtdta_elem" -> ((df: DataFrame) => {
        val mpv = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SMRTDTA_ELEM_PAIRS", "PROCEDURE", "SMRTDTA_ELEM", "SMRTDTA_ELEM_VALUE")
        val groups = Window.partitionBy(df("HLV_ID"), df("VALUE_LINE"), df("CUR_VALUE_DATETIME"), df("ELEMENT_ID"), df("PAT_LINK_ID"), df("CONTACT_SERIAL_NUM"))
          .orderBy(df("UPDATE_DATE").desc_nulls_last, df("FILEID").desc_nulls_last)
        df.withColumn("rn", row_number.over(groups))
          .filter("rn = 1 and smrtdta_elem_value in (" + mpv + ")")
          .drop("rn","fileid")
      }),
      "cdr.map_custom_proc" -> ((df: DataFrame) => {
        df.filter("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'smrtdta_elem_pairs'").drop("GROUPID").drop("DATASRC")
      })
    )

  join = (dfs: Map[String, DataFrame]) => {
    val left = dfs("smrtdta_elem").withColumnRenamed("pat_link_id","l_pat_link_id")
      .withColumnRenamed("contact_serial_num","l_contact_serial_num")
      .withColumnRenamed("cur_value_datetime","l_cur_value_datetime")
      .withColumnRenamed("element_id","l_element_id")
    val right = dfs("smrtdta_elem").withColumnRenamed("pat_link_id","r_pat_link_id")
      .withColumnRenamed("contact_serial_num","r_contact_serial_num")
      .withColumnRenamed("cur_value_datetime","r_cur_value_datetime")
      .withColumnRenamed("element_id","r_element_id")
    val pairs = left.join(right,
      left("l_pat_link_id") === right("r_pat_link_id") &&
        left("l_contact_serial_num") === right("r_contact_serial_num") &&
        substring(left("l_cur_value_datetime"),1,10) === substring(right("r_cur_value_datetime"),1,10))
    val g_elem = pairs.withColumn("greatest_element_id", greatest("l_element_id","r_element_id"))
      .filter("l_pat_link_id is not null and l_cur_value_datetime is not null and l_element_id is not null " +
        "and r_element_id is not null and l_element_id <> r_element_id")
    g_elem
      .join(dfs("cdr.map_custom_proc"),
        concat(lit(config(CLIENT_DS_ID)+"."), g_elem("greatest_element_id")) === dfs("cdr.map_custom_proc")("LOCALCODE"),
        "inner")
  }

  map = Map(
    "DATASRC" -> literal("smrtdta_elem_pairs"),
    "LOCALCODE" -> mapFrom("GREATEST_ELEMENT_ID",nullIf=Seq(null), prefix=config(CLIENT_DS_ID)+"."),
    "PATIENTID" -> mapFrom("L_PAT_LINK_ID"),
    "PROCEDUREDATE" -> mapFrom("L_CUR_VALUE_DATETIME"),
    "ENCOUNTERID" -> mapFrom("L_CONTACT_SERIAL_NUM"),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE")
  )

  afterMap = (df1: DataFrame) => {
    val df=df1.repartition(1000)
    val cols = Engine.schema.getStringList("Procedure").asScala.map(_.split("-")(0).toUpperCase())
    df.select(cols.map(col): _*).distinct
  }

}

// val p = new ProcedureSmrtdtaelempairs(cfg) ; val pr = build(p) ; pr.show