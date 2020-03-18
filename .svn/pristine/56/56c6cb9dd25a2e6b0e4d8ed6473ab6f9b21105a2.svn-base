package com.humedica.mercury.etl.hl7_v2.patientidentifier

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by abendiganavale on 6/19/17.
  */
class PatientidentifierPid(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F19_C1","LASTUPDATEDATE")
  )

  join = noJoin()

  map = Map(
    "DATASRC" -> literal("hl7_segment_pid_a"),
    "IDTYPE" -> literal("SSN"),
    "IDVALUE" -> standardizeSSN("PID_F19_C1"),
    "PATIENTID" -> mapFrom("PATIENTID")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND IDVALUE IS NOT NULL")
    val groups = Window.partitionBy(fil("PATIENTID"), fil("IDTYPE")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  //TODO mapExceptions

}
