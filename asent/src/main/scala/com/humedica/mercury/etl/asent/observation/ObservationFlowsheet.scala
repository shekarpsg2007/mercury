package com.humedica.mercury.etl.asent.observation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationFlowsheet(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_flowsheet", "cdr.zcm_obstype_code")

  columnSelect = Map(
    "as_flowsheet" -> List("RESULTNAME", "DISPLAYDATE", "ORGANIZATIONMRN", "FLOWVALUE", "ENCOUNTERID"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "DATATYPE")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'flowsheet'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_flowsheet")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("obscode") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_flowsheet")("RESULTNAME")))
  }

  afterJoin = (df: DataFrame) => {

    val groups = Window.partitionBy(df("ORGANIZATIONMRN"), df("OBSTYPE"), df("RESULTNAME"), df("DISPLAYDATE")).orderBy(df("FLOWVALUE").asc_nulls_last)
    val groups2 = Window.partitionBy(df("ORGANIZATIONMRN"), df("OBSTYPE"), df("RESULTNAME"), df("DISPLAYDATE"))
    val groups3 = Window.partitionBy(df("ORGANIZATIONMRN"), df("OBSTYPE"), df("RESULTNAME"), df("DISPLAYDATE"), df("FLOWVALUE"))

    df.withColumn("rn", row_number.over(groups))
      .withColumn("cnt", count("*").over(groups2))
      .withColumn("cnt_val", count("*").over(groups3))
      .filter("(rn = 1 AND cnt_val = 1) OR (cnt_val >=2 AND cnt >=2 AND rn = 1)").drop("rn").drop("cnt").drop("cnt_val")

  }

  map = Map(
    "DATASRC" -> literal("flowsheet"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "PATIENTID" -> mapFrom("ORGANIZATIONMRN"),
    "OBSDATE" -> mapFrom("DISPLAYDATE"),
    "LOCALCODE" -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("RESULTNAME")))),
    "OBSRESULT" -> mapFrom("FLOWVALUE"),
    "LOCALRESULT" -> ((col, df) => df.withColumn(col, when(df("FLOWVALUE").isNotNull && df("DATATYPE") === lit("CV"), concat_ws(".", lit(config(CLIENT_DS_ID)), df("FLOWVALUE"))).otherwise(df("FLOWVALUE")))),
    "OBSTYPE" -> mapFrom("OBSTYPE")
  )

  afterMap = (df: DataFrame) => {
    df.filter("OBSDATE is not null ")
  }
}