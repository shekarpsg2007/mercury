package com.humedica.mercury.etl.hl7_v2.patientaddress

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by abendiganavale on 6/19/17.
  */
class PatientaddressPid(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
   // ,"hl7_segment_sch_a"
   // ,"hl7_segment_z"
  )

  columnSelect = Map(
    "temptable" -> List("MESSAGEID","PATIENTID","LASTUPDATEDATE","PV1_F44_C1","PID_F11_C1","PID_F11_C2","PID_F11_C3"
    ,"PID_F11_C4","PID_F11_C5","PID_F11_C7")
   // "hl7_segment_sch_a" -> List("MESSAGEID","SCH_F11_C4"),
   // "hl7_segment_z" -> List("MESSAGEID","SEGMENT_NAME","Z_F29_C1")
  )

  /*
  beforeJoinExceptions = Map(
    "H524284_HL7" -> Map(
      "hl7_segment_z" -> ((df: DataFrame) => {
        //Exclude records where (HL7_Segment_Z.Segment_Name = 'ZAS' and HL7_Segment_Z.Z_F29_C1 = 'PHONE').
        excludeIf("SEGMENT_NAME = 'ZAS' AND Z_F29_C1 = 'PHONE'")(df)
      }))
  )
  */

  join = noJoin()

 /*
  joinExceptions = Map(
    "H524284_HL7" -> ((dfs: Map[String, DataFrame]) => {
      dfs("temptable")
        .join(dfs("hl7_segment_sch_a"), Seq("MESSAGEID"), "inner")
        .join(dfs("hl7_segment_z"), Seq("MESSAGEID"), "inner")
    })
  )
*/

  map = Map(
    "DATASRC" -> literal("hl7_segment_pid_a"),
    "ADDRESS_DATE" -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "STATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("PID_F11_C4")) =!= "2" || df("PID_F11_C4") === "\"\"", null)
          .otherwise(df("PID_F11_C4")))
    }),
    "ZIPCODE" -> standardizeZip("PID_F11_C5",zip5=true),
    "ADDRESS_LINE1" -> mapFrom("PID_F11_C1", nullIf=Seq("\"\"")),
    "ADDRESS_LINE2" -> mapFrom("PID_F11_C2", nullIf=Seq("\"\"")),
    "CITY" -> mapFrom("PID_F11_C3", nullIf=Seq("\"\"")),
    "ADDRESS_TYPE" -> mapFrom("PID_F11_C7")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND ADDRESS_DATE IS NOT NULL AND " +
              "(STATE IS NOT NULL OR ZIPCODE IS NOT NULL OR ADDRESS_LINE1 IS NOT NULL OR ADDRESS_LINE2 IS NOT NULL" +
              " OR CITY IS NOT NULL)")
    val groups = Window.partitionBy(fil("PATIENTID"),fil("ADDRESS_LINE1"),fil("ADDRESS_LINE2"),fil("STATE"),fil("ZIPCODE"),fil("CITY")).orderBy(fil("ADDRESS_DATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  mapExceptions = Map(
    ("H285893_HL7", "ADDRESS_DATE") -> mapFrom("LASTUPDATEDATE"),
    ("H524284_HL7", "ADDRESS_DATE") -> mapFrom("LASTUPDATEDATE"),
    ("H524284_HL7_ATH", "ADDRESS_DATE") -> mapFrom("LASTUPDATEDATE"),
    ("H524284_HL7", "ADDRESS_LINE1") -> mapFrom("PID_F11_C2", nullIf=Seq("\"\"")),
    ("H524284_HL7", "ADDRESS_LINE2") -> mapFrom("PID_F11_C1", nullIf=Seq("\"\"")),
    ("H704847_HL7_ELYS", "ADDRESS_DATE") -> mapFrom("LASTUPDATEDATE")
  )

}
