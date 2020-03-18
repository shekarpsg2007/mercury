package com.humedica.mercury.etl.asent.labresult


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window


class LabresultFlowsheet(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_flowsheet")

  columnSelect = Map(
    "as_flowsheet" -> List("ENCOUNTERID", "RCODE", "RESULTNAME", "ORGANIZATIONMRN", "DISPLAYDATE", "FLOWVALUE", "FLOWSHEETID")
  )


  beforeJoin = Map(
    "as_flowsheet" -> ((df: DataFrame) => {
      df.filter("FLOWSHEETID = '8'")
        .withColumn("LOCALRESULT_25", when(!df("FLOWVALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
          when(locate(" ", df("FLOWVALUE"), 25) === 0, expr("substr(FLOWVALUE,1,length(FLOWVALUE))"))
            .otherwise(expr("substr(FLOWVALUE,1,locate(' ', FLOWVALUE, 25))").cast("Integer"))).otherwise(null))
        .withColumn("LOCALRESULT_NUMERIC", when(df("FLOWVALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), df("FLOWVALUE")).otherwise(null))

    }))


  map = Map(
    "DATASRC" -> literal("flowsheet"),
    "PATIENTID" -> mapFrom("ORGANIZATIONMRN"),
    "LOCALCODE" -> mapFrom("RESULTNAME"),
    "LOCALNAME" -> mapFrom("RESULTNAME"),
    "DATEAVAILABLE" -> mapFrom("DISPLAYDATE"),
    "LABRESULT_DATE" -> mapFrom("DISPLAYDATE"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "LABRESULTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("", df("ENCOUNTERID"), df("RCODE")))
    }),
    "LOCALUNITS" -> ((col, df) => df.withColumn(col, coalesce(when(df("RESULTNAME") === lit("Outside HgbAlc"), "%"), when(df("RESULTNAME") === lit("Outside LDL"), "mg/dl"), when(df("RESULTNAME") === lit("Outside Lead Level"), "mcg/dl")))),
    "LOCALRESULT" -> mapFrom("FLOWVALUE"),
    "LOCALRESULT_INFERRED" -> extract_value(),
    "RELATIVEINDICATOR" -> labresults_extract_relativeindicator(),
    "LOCALUNITS_INFERRED" -> labresults_extract_uom()
  )


  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("LABRESULTID"))
    val df2 = df.withColumn("cnt", count(df("LABRESULTID")).over(groups))
    df2.filter("cnt=1 AND LOCALRESULT_NUMERIC IS NOT NULL")
  }


}