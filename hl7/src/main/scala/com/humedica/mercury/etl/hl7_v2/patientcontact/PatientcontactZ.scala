package com.humedica.mercury.etl.hl7_v2.patientcontact

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by abendiganavale on 6/19/17.
  */
class PatientcontactZ(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_z"
  )

  columnSelect = Map(
    "temptable" -> List("MESSAGEID","PATIENTID","LASTUPDATEDATE","MSH_F10_C1"),
    "hl7_segment_z" -> List("MESSAGEID","Z_F60_C1","SEGMENT_NAME")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("hl7_segment_z"), Seq("MESSAGEID"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_z"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "UPDATE_DT" -> mapFrom("Z_F60_C1", nullIf=Seq("\"\""))
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND UPDATE_DT IS NOT NULL and SEGMENT_NAME = 'ZPD'")
    val groups = Window.partitionBy(fil("PATIENTID"),fil("HOME_PHONE"),fil("WORK_PHONE")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

}
