package com.humedica.mercury.etl.asent.procedure

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

/**
  * Created by tzentz on 7/26/17.
  */

class ProcedureVitals(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_vitals", "cdr.map_custom_proc")

  columnSelect = Map(
    "as_vitals" -> List("QODE", "PATIENT_MRN", "PERFORMED_DATETIME", "VITAL_ENTRY_NAME", "PERFORMED_DATETIME",
      "ENCOUNTER_ID", "CHILD_ID", "STATUS_ID"),
    "cdr.map_custom_proc" -> List("MAPPEDVALUE", "LOCALCODE", "DATASRC", "GROUPID")
  )

  //(PARTITION BY v.child_id, m.mappedvalue ORDER BY v.performed_datetime DESC nulls last)
  beforeJoin = Map(
    "as_problems" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("CHILD_ID"), df("MAPPEDVALUE")).orderBy(df("PERFORMED_DATETIME").desc)
      val df2 = df.withColumn("rn", row_number.over(groups))
      df2.filter("rn = '1'")
    })
  )

  // INNER JOIN map_custom_proc m on (m.groupid = '&grpid' and m.datasrc = 'vitals' AND m.localcode = '&cdsid_prefix'||to_char(v.qode))
  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_vitals")
      .join(dfs("cdr.map_custom_proc"), dfs("cdr.map_custom_proc")("LOCALCODE") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_vitals")("QODE")) &&
        dfs("cdr.map_custom_proc")("groupid") === lit(config(GROUP)) && dfs("cdr.map_custom_proc")("datasrc") === lit("vitals"), "inner")
  }

  //Include only where localcode in (Select LocalCode from Map_Custom_Proc where Datasrc = '/datasrc/' and GroupID = '/groupid/'.   Exclude where status = '6'
  afterJoin = (df: DataFrame) => {
    df.filter("STATUS_ID != '6'")
  }

  map = Map(
    "DATASRC" -> literal("vitals"),
    "LOCALCODE" -> mapFrom("QODE", prefix = config(CLIENT_DS_ID) + "."),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "PROCEDUREDATE" -> mapFromDate("PERFORMED_DATETIME"),
    "LOCALNAME" -> mapFrom("VITAL_ENTRY_NAME"),
    "ACTUALPROCDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("PERFORMED_DATETIME") isin ("1900-01-01 00:00:00"), null).
          otherwise(from_unixtime(unix_timestamp(df("PERFORMED_DATETIME"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")))
    }), ////Treat '1900-01-01 00:00:00' as NULL
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "SOURCEID" -> mapFrom("CHILD_ID"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> literal("CUSTOM")
  )

}