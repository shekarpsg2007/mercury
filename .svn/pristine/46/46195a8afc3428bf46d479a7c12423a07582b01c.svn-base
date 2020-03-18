package com.humedica.mercury.etl.hl7_v2.appointment

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/26/17.
  */
class AppointmentObr(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_obr_a"
    ,"hl7_segment_orc_a"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F3_C1","MSH_F7_C1","MSH_F9_C1","MSH_F11_C1","PV1_F44_C1","MESSAGEID","LASTUPDATEDATE"),
    "hl7_segment_obr_a" -> List("MESSAGEID","OBR_F2_C1","OBR_F6_C1","OBR_F20_C2","OBR_F4_C4","OBR_F31_C2"),
    "hl7_segment_orc_a" -> List("MESSAGEID","ORC_F12_C1")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_obr_a")
      .join(dfs("temptable"), Seq("MESSAGEID"), "inner")
      .join(dfs("hl7_segment_orc_a"), Seq("MESSAGEID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_obr_a"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "APPOINTMENTDATE" -> ((col:String,df:DataFrame) => df.withColumn(col, substring(df("OBR_F6_C1"),1,14))),
    "APPOINTMENTID" -> mapFrom("OBR_F2_C1"),
    "LOCATIONID" -> mapFrom("OBR_F20_C2"),
    "PROVIDERID" -> mapFrom("ORC_F12_C1", nullIf=Seq("00000000")),
    "APPOINTMENT_REASON" -> mapFrom("OBR_F31_C2"),
    "LOCAL_APPT_TYPE" -> mapFrom("OBR_F4_C4")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND LOCATIONID IS NOT NULL AND APPOINTMENTID IS NOT NULL AND APPOINTMENTDATE IS NOT NULL AND MSH_F9_C1 = 'ORM'")
    val groups = Window.partitionBy(fil("APPOINTMENTID")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

}
