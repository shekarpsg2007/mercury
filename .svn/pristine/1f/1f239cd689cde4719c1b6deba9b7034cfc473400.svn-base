package com.humedica.mercury.etl.hl7_v2.patientcontact

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by abendiganavale on 6/19/17.
  */
class PatientcontactPid(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F13_C1","PID_F14_C1","PV1_F44_C1","LASTUPDATEDATE")
  )

  join = noJoin()

  map = Map(
    "DATASRC" -> literal("hl7_segment_pid_a"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "UPDATE_DT" -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    "HOME_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when((df("PID_F13_C1") === "(000)000-0000") || (df("PID_F13_C1") === "\"\"") , null)
        .otherwise(df("PID_F13_C1"))) //set to null where (000)000-0000 or empty string with double quotes ("")
    }),
    "WORK_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when((df("PID_F14_C1") === "(000)000-0000") || (df("PID_F14_C1") === "\"\"") , null)
        .otherwise(df("PID_F14_C1")))
    }),
    "PERSONAL_EMAIL" -> nullValue()
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND UPDATE_DT IS NOT NULL AND (HOME_PHONE IS NOT NULL OR WORK_PHONE IS NOT NULL)")
    //val ph = df.withColumn("phone", concat_ws("",df("HOME_PHONE"),df("WORK_PHONE")))
    //val fil = ph.filter("PATIENTID IS NOT NULL AND UPDATE_DT IS NOT NULL AND phone is not null")
    //val groups = Window.partitionBy(fil("PATIENTID"),lower(fil("phone"))).orderBy(fil("UPDATE_DT").desc)
    val groups = Window.partitionBy(fil("PATIENTID"),lower(concat_ws("",df("HOME_PHONE"),df("WORK_PHONE")))).orderBy(fil("UPDATE_DT").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  //TODO mapexceptions
  mapExceptions = Map(
    ("H285893_HL7", "UPDATE_DT") -> mapFrom("LASTUPDATEDATE"),
    //("H328218_HL7_GRHS", "UPDATE_DT") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    //("H328218_HL7_Genesis", "UPDATE_DT") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    //("H451171_HL7_EP2", "UPDATE_DT") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    ("H524284_HL7", "UPDATE_DT") -> mapFrom("LASTUPDATEDATE"),
    ("H524284_HL7_ATH", "UPDATE_DT") -> mapFrom("LASTUPDATEDATE"),
    //("H942522_HL7", "UPDATE_DT") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    //("H984216_HL7_CERNER_TNNAS", "UPDATE_DT") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    //("H984216_HL7_CERNER_WIMIL", "UPDATE_DT") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    //("H984216_HL7_CERNER_WIAPP", "UPDATE_DT") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    //("H984216_HL7_CERNER_WIGLE", "UPDATE_DT") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    ("H846629_HL7", "HOME_PHONE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PID_F13_C2").isin("HOME", "P") || df("PID_F13_C2").isNull, df("PID_F13_C1"))
        .when((df("PID_F14_C1") === "(000)000-0000") || (df("PID_F13_C1") === "\"\""), nullValue())
        .otherwise(null))
    }),
    ("H053731_HL7", "PERSONAL_EMAIL") -> mapFrom("PID_F13_C2"),
    ("H285893_HL7", "PERSONAL_EMAIL") -> mapFrom("PID_F13_C2"),
    ("H557454_HL7", "PERSONAL_EMAIL") -> mapFrom("PID_F13_C4"),
    ("H984216_HL7_CERNER_WIAPP", "PERSONAL_EMAIL") -> mapFrom("PID_F13_C4"),
    ("H704847_HL7_ELYS", "UPDATE_DT") -> mapFrom("LASTUPDATEDATE")
  )

}
