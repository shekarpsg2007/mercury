package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.{EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Created by bhenriksen on 2/16/17.
 */


class ProcedureMedadminrec(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("medadminrec",
    "medorders",
     "cdr.map_custom_proc",
  "cdr.map_predicate_values")


  columnSelect = Map(
    "medadminrec" -> List("MAR_ACTION_C", "PAT_ID", "TAKEN_TIME", "MAR_ENC_CSN", "ORDER_MED_ID", "ROUTE_C", "SAVED_TIME","ROUTE_C"),
    "medorders" -> List("MEDICATION_ID", "NAME", "MED_PRESC_PROV_ID","ORDER_MED_ID")
  )

  beforeJoin = Map(
    "medadminrec" -> ((df: DataFrame) => {
      val act_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MEDADMINREC", "PROCEDUREDO", "MEDADMINREC", "MAR_ACTION_C")
      val route_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MEDADMINREC", "PROCEDUREDO", "MEDADMINREC", "ROUTE_C")
      df.filter("PAT_ID is not null and TAKEN_TIME is not null and MAR_ACTION_C in (" + act_c + ") and ('NO_MPV_MATCHES' in (" + route_c + ") OR ROUTE_C IN (" + route_c + "))")
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'medadminrec'").drop("GROUPID")
    }),
    "medorders" -> ((df: DataFrame) => {
      df.filter("MEDICATION_ID is not null").withColumnRenamed("ORDER_MED_ID", "ORDER_MED_ID_mo").drop("PAT_ID")
    })
  )

  join = (dfs:Map[String,DataFrame]) => {
    dfs("medadminrec")
       .join(dfs("medorders")
          .join(dfs("cdr.map_custom_proc"), concat(lit(config(CLIENT_DS_ID)+"."), dfs("medorders")("MEDICATION_ID")) === dfs("cdr.map_custom_proc")("LOCALCODE"), "inner")
       ,dfs("medorders")("ORDER_MED_ID_mo") === dfs("medadminrec")("ORDER_MED_ID"))
  }


   afterJoin = (df: DataFrame) => {
     val groups = Window.partitionBy(df("PAT_ID"),df("MAR_ENC_CSN"),df("TAKEN_TIME"),df("MEDICATION_ID")).orderBy(df("SAVED_TIME").desc)
     df.withColumn("rw", row_number.over(groups))
       .filter("rw =1")
  }

  map = Map(
    "DATASRC" -> literal("medadminrec"),
    "LOCALCODE" -> mapFrom("MEDICATION_ID", prefix = config(CLIENT_DS_ID) + "."),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("TAKEN_TIME"),
    "LOCALNAME" -> mapFrom("NAME"),
    "ORDERINGPROVIDERID" -> mapFrom("MED_PRESC_PROV_ID"),
    "ENCOUNTERID" -> mapFrom("MAR_ENC_CSN", nullIf=Seq("-1")),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> mapFrom("CODETYPE")
  )
}


// test
// val p = new ProcedureMedadminrec(cfg) ; val pr = build(p) ; pr.show ; pr.count ; pr.select("ENCOUNTERID").distinct.count


