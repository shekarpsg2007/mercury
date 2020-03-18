package com.humedica.mercury.etl.epic_v2.appointmentlocation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by sshah on 5/17/17.
  */
class AppointmentlocationDept (config: Map[String, String]) extends EntitySource(config: Map[String, String]){

  tables = List("zh_claritydept")

  beforeJoin = Map(
    "zh_claritydept" -> ((df: DataFrame) => {
      df.groupBy("DEPARTMENT_ID").agg(max("DEPARTMENT_NAME").as("LOCATIONNAME"))
    })
  )


  join = noJoin()

  map = Map(
    "LOCATIONID" -> mapFrom("DEPARTMENT_ID"),
    "LOCATIONNAME" -> mapFrom("LOCATIONNAME")
  )



}