package com.humedica.mercury.etl.epic_v2.rxmedadministrations

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 02/01/2017
 */

class RxmedadministrationsMedadminrec(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_med_unit",
    "medadminrec",
    "medorders:epic_v2.rxordersandprescriptions.RxordersandprescriptionsMedorders",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "zh_med_unit" -> List("DISP_QTYUNIT_C", "NAME"),
    "medadminrec" -> List("ROUTE_C", "PAT_ID","ORDER_MED_ID", "LINE", "SAVED_TIME",
      "MAR_ACTION_C", "DOSE_UNIT_C", "MAR_INF_RATE_UNIT_C", "MAR_ENC_CSN", "TAKEN_TIME", "INFUSION_RATE", "MAR_DURATION_UNIT_C",
      "MAR_DURATION", "SIG")
  )

  beforeJoin =
    Map("medadminrec" -> ((df: DataFrame) =>
    {
      val t_mar_action_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MEDADMINREC", "RXADMIN", "MEDADMINREC", "MAR_ACTION_C")
      val dedup = Window.partitionBy(df("ORDER_MED_ID"), df("LINE")).orderBy(df("SAVED_TIME").desc)
      val addColumn = df.withColumn("rn", row_number.over(dedup))
      addColumn.filter("rn = 1 and PAT_ID is not null AND ORDER_MED_ID is not null AND MAR_ACTION_C in (" + t_mar_action_c + ") ").drop("rn")
        .withColumnRenamed("SAVED_TIME", "SAVED_TIME_mrc")
    }),
    "zh_med_unit" -> ((df: DataFrame) =>
    {
      df.withColumnRenamed("NAME", "NAME_zhmu")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("medadminrec")
      .join(dfs("medorders"), dfs("medadminrec")("ORDER_MED_ID") === dfs("medorders")("rxid"),"inner")
      .join(dfs("zh_med_unit"),
        coalesce(dfs("medadminrec")("dose_unit_c"), dfs("medadminrec")("MAR_INF_RATE_UNIT_C"))
            === dfs("zh_med_unit")("DISP_QTYUNIT_C"), "left_outer")
    }

  map = Map(
    "DATASRC" -> literal("medadminrec"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "RXADMINISTRATIONID" -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("ORDER_MED_ID"), lit("."), df("LINE")))),
    "RXORDERID" -> mapFrom("ORDER_MED_ID"),
    "LOCALINFUSIONDURATION" -> ((col:String,df:DataFrame) => df.withColumn(col,when(df("MAR_DURATION_UNIT_C") === "1", concat(df("MAR_DURATION"), lit(" minutes")))
      .when(df("MAR_DURATION_UNIT_C") === "2", concat(df("MAR_DURATION"), lit(" hours")))
      .when(df("MAR_DURATION_UNIT_C") === "3", concat(df("MAR_DURATION"), lit(" days"))))),
    "LOCALINFUSIONRATE" -> mapFrom("INFUSION_RATE"),
    "LOCALDOSEUNIT" -> mapFrom("NAME_zhmu"),
    "LOCALDRUGDESCRIPTION" -> mapFrom("LOCALDESCRIPTION"),
    "LOCALGENERICDESC" -> mapFrom("LOCALGENERICDESC"),
    "LOCALGPI" -> mapFrom("LOCALGPI"),
    "LOCALMEDCODE" -> mapFrom("LOCALMEDCODE"),
    "LOCALROUTE" -> mapFrom("ROUTE_C", nullIf=Seq("-1"), prefix=(config(CLIENT_DS_ID)) + "."),
    "LOCALTOTALDOSE" -> mapFrom("SIG"),
    "ADMINISTRATIONTIME" -> mapFrom("TAKEN_TIME"),
    "ENCOUNTERID" -> mapFrom("MAR_ENC_CSN", nullIf=Seq("-1"))
  )

}

// TEST
// val r = new RxmedadministrationsMedadminrec(cfg) ; val rxa = build(r) ; rxa.show
