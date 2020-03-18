package com.humedica.mercury.etl.cerner_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Auto-generated on 08/09/2018
 */


class ProcedureOrderrecon(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("order_recon", "cdr.map_custom_proc", "cdr.map_predicate_values")

  columnSelect = Map(
    "order_recon" -> List("PERSON_ID", "PERFORMED_DT_TM", "ENCNTR_ID", "PERFORMED_PRSNL_ID", "UPDT_DT_TM",
      "RECON_STATUS_CD"),
    "cdr.map_custom_proc" -> List("GROUPID", "DATASRC", "LOCALCODE", "MAPPEDVALUE")
  )

  beforeJoin = Map(
    "order_recon" -> ((df: DataFrame) => {
      val list_recon_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDER_RECON", "PROCEDURE", "ORDER_RECON", "RECON_STATUS_CD")
      df.filter("person_id is not null and performed_dt_tm is not null and recon_status_cd in (" + list_recon_status + ")")
        .withColumn("LOCALCODE", lit(config(CLIENT_DS_ID) + ".MEDREC"))
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and datasrc = 'order_recon'")
        .select("LOCALCODE", "MAPPEDVALUE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("order_recon")
      .join(dfs("cdr.map_custom_proc"), Seq("LOCALCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("order_recon"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "PROCEDUREDATE" -> mapFrom("PERFORMED_DT_TM"),
    "LOCALNAME" -> literal("Medication Reconciliation"),
    "ACTUALPROCDATE" -> mapFrom("PERFORMED_DT_TM"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "PERFORMINGPROVIDERID" -> mapFrom("PERFORMED_PRSNL_ID"),
    "CODETYPE" -> literal("CUSTOM")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ENCOUNTERID"), df("PROCEDUREDATE"), df("MAPPEDVALUE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}