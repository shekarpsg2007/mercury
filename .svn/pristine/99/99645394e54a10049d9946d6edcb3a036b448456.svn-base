package com.humedica.mercury.etl.athena.patientidentifier

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Auto-generated on 09/21/2018
  */


class PatientidentifierCustomdemographics(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("customdemographics",
    "temp_fileid_dates:athena.util.UtilFileIdDates"
  )

  columnSelect = Map(
    "customdemographics" -> List("CUSTOM_FIELD_VALUE", "PATIENT_ID", "FILEID", "custom_field_name"),
    "temp_fileid_dates" -> List("fileid", "filedate")

  )

  beforeJoin = Map(
    "customdemographics" -> ((df: DataFrame) => {

      df.filter(
        "upper(custom_field_name) in('GPP LEGACY ID NUMBER', 'ITIN NUMBER')"
      )

    }))

  join = (dfs: Map[String, DataFrame]) => {

    dfs("customdemographics")
      .join(dfs("temp_fileid_dates"), Seq("fileid"), "left_outer")

  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("patient_id"), df("custom_field_name")).orderBy(df("filedate").desc_nulls_last, df("fileid") desc)
    val df2 = df.withColumn("dedup_row", row_number.over(groups))
    df2.filter("dedup_row = 1")

  }


  map = Map(
    "DATASRC" -> literal("customdemographics"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "IDTYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(upper(df("custom_field_name")) === (lit("GPP LEGACY ID NUMBER")), lit("GPP_ID"))
          .when(upper(df("custom_field_name")) === (lit("ITIN NUMBER")), lit("ITIN"))
      )
    }),
    "IDVALUE" -> mapFrom("Custom_Field_Value")
  )

  afterMap = (df: DataFrame) => {
    df.filter("patientid is not null and idvalue is not null")
  }


}
