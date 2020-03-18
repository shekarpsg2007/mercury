package com.humedica.mercury.etl.fdr.demo

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
  * Auto-generated on 02/01/2017
  */


class DemoCollection(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  //tables = List("procedure:epic_v2@Procedure")

  //tables = List("procedure:asent@Procedure")

  tables = List("procedure:"+config("SOURCE_EMR")+"@Procedure")


  columns = List("GROUPID", "DATASRC", "FACILITYID", "ENCOUNTERID", "PATIENTID")

}