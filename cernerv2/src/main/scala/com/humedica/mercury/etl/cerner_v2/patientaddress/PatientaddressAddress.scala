package com.humedica.mercury.etl.cerner_v2.patientaddress

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 08/09/2018
  */

class PatientaddressAddress(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("address", "zh_code_value", "cdr.map_predicate_values")

  columnSelect = Map(
    "address" -> List("UPDT_DT_TM", "PARENT_ENTITY_ID", "PARENT_ENTITY_NAME", "ZIPCODE", "STREET_ADDR", "STREET_ADDR2",
      "ADDRESS_TYPE_CD", "CITY", "STATE_CD", "STATE"),
    "zh_code_value" -> List("CDF_MEANING", "DISPLAY", "CODE_VALUE", "DESCRIPTION")
  )

  beforeJoin = Map(
    "address" -> ((df: DataFrame) => {
      val list_ADDRESS = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ADDRESS", "PATIENT_ADDRESS", "ADDRESS", "ADDRESS_TYPE_CD")
      val fil = df.filter(
        "PARENT_ENTITY_ID is not null " +
          "AND PARENT_ENTITY_NAME = 'PERSON' " +
          "AND updt_dt_tm is not null " +
          " AND(STREET_ADDR is null OR length(STREET_ADDR) < 70)")
     val df1 = fil.filter(fil("address_type_cd").isin(list_ADDRESS: _*))
      df1.withColumn("ZIPCODE", when(df1("ZIPCODE") === "0", null).otherwise(regexp_replace(df1("ZIPCODE"), "^[-]+", "")))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    val join1 = dfs("address")
      .join(dfs("zh_code_value"), dfs("zh_code_value")("CODE_VALUE") === dfs("address")("state_cd"), "left_outer")
      .withColumnRenamed("CDF_MEANING", "CDF_MEANING_ADDRESS").withColumnRenamed("DISPLAY", "DISPLAY_ADDRESS")
    val zh_2 = dfs("zh_code_value")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE2")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION2")
    join1.join(zh_2, zh_2("CODE_VALUE2") === join1("address_type_cd"), "left_outer")
  }



  map = Map(
    "DATASRC" -> literal("address"),
    "ADDRESS_DATE" -> mapFrom("UPDT_DT_TM"),
    "PATIENTID" -> mapFrom("PARENT_ENTITY_ID"),
    "ZIPCODE" -> standardizeZip("ZIPCODE", zip5 = true),
    "ADDRESS_LINE1" -> mapFrom("STREET_ADDR"),
    "ADDRESS_LINE2" -> mapFrom("STREET_ADDR2"),
    "ADDRESS_TYPE" -> mapFrom("DESCRIPTION2"),
    "STATE" -> ((col: String, df: DataFrame) => {
      val df1 = df.withColumn(col, upper(regexp_replace(coalesce(df("CDF_MEANING_ADDRESS"), df("STATE"), df("DISPLAY_ADDRESS")), "[^A-Za-z]", "")))
      df1.withColumn(col, when(df1(col) === lit(""), null).otherwise(df1(col)))
    }),
    "CITY" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("CITY") === "0", null).otherwise(df("CITY")))
    })
  )

  afterMap = (df: DataFrame) => {
    val df1 = df.filter("coalesce(ADDRESS_LINE1, ADDRESS_LINE2 ,CITY, STATE, ZIPCODE) is not null")
    val df2 = df1.filter("length(STATE) = 2 or state is null")
    val groups = Window.partitionBy(df2("ADDRESS_DATE"), df2("PATIENTID"), upper(df2("ADDRESS_LINE1")), upper(df2("ADDRESS_LINE2")),
        upper(df2("STATE")), upper(df2("CITY")))
      .orderBy(df2("ZIPCODE").asc_nulls_last, df2("ADDRESS_LINE1").asc, df2("ADDRESS_LINE2").asc, df2("STATE").asc, df2("CITY").asc)
    df2.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }
}