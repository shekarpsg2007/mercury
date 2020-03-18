package com.humedica.mercury.etl.cerner_v2.patientdetail


import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 08/09/2018
  *
  *
  */

class PatientdetailAddress(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("address", "zh_code_value", "patient:cerner_v2.patient.PatientPatient")

  columnSelect = Map(
    "address" -> List("UPDT_DT_TM", "PARENT_ENTITY_ID", "ZIPCODE", "CITY",  "STATE", "STATE_CD", "BEG_EFFECTIVE_DT_TM"),
    "zh_code_value" -> List( "DISPLAY", "CODE_VALUE"),
    "patient" -> List("PATIENTID" )
  )

  beforeJoin = Map(
    "address" -> ((df: DataFrame) => {

      df.withColumnRenamed("PARENT_ENTITY_ID", "PATIENTID")
        .withColumnRenamed("BEG_EFFECTIVE_DT_TM", "PATDETAIL_TIMESTAMP")
    }))


  join = (dfs: Map[String, DataFrame]) => {
    dfs("address")
      .join(dfs("patient"), Seq("PATIENTID"), "inner")
      .join(dfs("zh_code_value"), dfs("zh_code_value")("CODE_VALUE") === dfs("address")("state_cd"), "left_outer")

  }

  afterJoin = (df: DataFrame) => {

    var dfCity = df.withColumn("PATIENTDETAILTYPE", lit("CITY"))
      .withColumn("LOCALVALUE", when(df("CITY") =!= "0", df("CITY")))

    val groupsCity = Window.partitionBy(dfCity("PATIENTID"), upper(dfCity("LOCALVALUE"))).orderBy(dfCity("PATDETAIL_TIMESTAMP").desc_nulls_last, dfCity("UPDT_DT_TM").desc_nulls_last)
    dfCity = dfCity.withColumn("city_row", row_number.over(groupsCity))
    dfCity = dfCity.filter(" city_row = 1 and LOCALVALUE is not null and PATDETAIL_TIMESTAMP is not null  ")
    dfCity=  dfCity.select("PATIENTID", "PATDETAIL_TIMESTAMP", "PATIENTDETAILTYPE", "LOCALVALUE")


    var dfState = df.withColumn("PATIENTDETAILTYPE", lit("STATE"))
      .withColumn("LOCALVALUE", when(df("STATE").isNull , df("DISPLAY")).otherwise(df("STATE")))


    val groupState = Window.partitionBy(dfState("PATIENTID"), upper(dfState("LOCALVALUE"))).orderBy(dfState("PATDETAIL_TIMESTAMP").desc_nulls_last, dfState("UPDT_DT_TM").desc_nulls_last)

    dfState = dfState.withColumn("state_row", row_number.over(groupState))

    dfState = dfState.filter(" state_row = 1 and LOCALVALUE is not null  and PATDETAIL_TIMESTAMP is not null  ")
    dfState = dfState.select("PATIENTID", "PATDETAIL_TIMESTAMP", "PATIENTDETAILTYPE", "LOCALVALUE")



    var dfZip = df.withColumn("PATIENTDETAILTYPE", lit("ZIPCODE"))
      .withColumn("LOCALVALUE",when(df("ZIPCODE") =!= "0", regexp_replace(df("ZIPCODE"), "^[-]+", "")))

   val satZip = standardizeZip("LOCALVALUE", true)
     dfZip = satZip("LOCALVALUE", dfZip)

    val groupZip = Window.partitionBy(dfZip("PATIENTID"), dfZip("LOCALVALUE")).orderBy(dfZip("PATDETAIL_TIMESTAMP").desc_nulls_last, dfZip("UPDT_DT_TM").desc_nulls_last)
    dfZip = dfZip.withColumn("zip_row", row_number.over(groupZip))
    dfZip = dfZip.filter(" zip_row = 1 and LOCALVALUE is not null and PATDETAIL_TIMESTAMP is not null  ")
    dfZip = dfZip.select("PATIENTID", "PATDETAIL_TIMESTAMP", "PATIENTDETAILTYPE", "LOCALVALUE")


    dfCity.union(dfState)
      .union(dfZip)

  }


  map = Map(
    "DATASRC" -> literal("address"),
    "PATDETAIL_TIMESTAMP" -> mapFrom("PATDETAIL_TIMESTAMP"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PATIENTDETAILTYPE" -> mapFrom("PATIENTDETAILTYPE"),
    "LOCALVALUE" -> mapFrom("LOCALVALUE")
  )


}