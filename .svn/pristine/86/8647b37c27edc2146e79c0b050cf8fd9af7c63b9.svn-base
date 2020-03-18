package com.humedica.mercury.etl.hl7_v2.insurance

import com.humedica.mercury.etl.core.engine.Constants.CLIENT_DS_ID
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/26/17.
  */
class InsuranceIn1(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  tables = List("temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_in1_a")

  columnSelect = Map(
    "temptable" -> List("PATIENTID","ENCOUNTERID","LASTUPDATEDATE","MESSAGEID","PV1_F44_C1","PV1_F20_C1"),
    "hl7_segment_in1_a" -> List("IN1_F8_C1","IN1_F1_C1","IN1_F36_C1","IN1_F13_C1","IN1_F12_C1","IN1_F15_C1"
      ,"IN1_F3_C1","IN1_F4_C1","MESSAGEID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_in1_a")
      .join(dfs("temptable"), Seq("MESSAGEID"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_in1_a"),
    "INS_TIMESTAMP" -> mapFrom("PV1_F44_C1"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "GROUPNBR" -> mapFrom("IN1_F8_C1",nullIf=Seq("\"\"")),
    "INSURANCEORDER" -> ((col:String,df:DataFrame) => df.withColumn(col, regexp_replace(df("IN1_F1_C1"), "^0+(?!$)", ""))),
    "POLICYNUMBER" -> ((col:String,df:DataFrame) => df.withColumn(col,
      when(df("IN1_F36_C1") === "\"\"", null).otherwise(regexp_replace(df("IN1_F36_C1"), "-", "")))),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "PLANTYPE" -> mapFrom("IN1_F15_C1", nullIf=Seq("\"\"")),
    "PAYORCODE" -> mapFrom("IN1_F3_C1", nullIf=Seq("\"\"")),
    "PAYORNAME" -> mapFrom("IN1_F4_C1", nullIf=Seq("\"\"")),
    "PLANCODE" -> mapFrom("IN1_F3_C1", nullIf=Seq("\"\"")),
    "PLANNAME" -> mapFrom("IN1_F4_C1", nullIf=Seq("\"\"")),
    "ENROLLENDDT" -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("IN1_F13_C1"), lit("yyyymmddhh24miss")))
      len.withColumn(col,
        when(length(len("IN1_F13_C1")).lt(len("lenAddm")), expr("rpad(IN1_F13_C1, lenAddm, '0')"))
          .when(length(len("IN1_F13_C1")).gt(len("lenAddm")), expr("substr(IN1_F13_C1, 0, lenAddm)"))
          .otherwise(len("IN1_F13_C1")))
    }),
    "ENROLLSTARTDT" -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("IN1_F12_C1"), lit("yyyymmddhh24miss")))
      len.withColumn(col,
        when(length(len("IN1_F12_C1")).lt(len("lenAddm")), expr("rpad(IN1_F12_C1, lenAddm, '0')"))
          .when(length(len("IN1_F12_C1")).gt(len("lenAddm")), expr("substr(IN1_F12_C1, 0, lenAddm)"))
          .otherwise(len("IN1_F12_C1")))
    })
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID is not null and INS_TIMESTAMP IS NOT NULL")
    val groups = Window.partitionBy(fil("ENCOUNTERID"), fil("INS_TIMESTAMP"), fil("PAYORCODE"), fil("PLANTYPE")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  mapExceptions = Map(
    ("H704847_HL7_CCD", "PAYORCODE") -> mapFrom("IN1_F4_C1", nullIf=Seq("\"\"")),
    ("H704847_HL7_CCD","PLANCODE") -> mapFrom("IN1_F4_C1", nullIf=Seq("\"\"")),
    ("H704847_HL7_CCD", "GROUPNBR") -> nullValue(),
    ("H704847_HL7_OCIE", "PAYORCODE") -> mapFrom("IN1_F4_C1", nullIf=Seq("\"\"")),
    ("H704847_HL7_OCIE","PLANCODE") -> mapFrom("IN1_F4_C1", nullIf=Seq("\"\"")),
    ("H704847_HL7_OCIE", "GROUPNBR") -> nullValue(),
    ("H328218_HL7_Genesis", "PLANTYPE") -> mapFrom("IN1_F15_C1",nullIf=Seq("\"\"") , prefix = config(CLIENT_DS_ID) + "."),
    ("H328218_HL7_Genesis", "PAYORCODE") -> cascadeFrom(Seq("IN1_F3_C1","IN1_F4_C1")),
    ("H328218_HL7_Genesis", "PLANCODE") -> cascadeFrom(Seq("IN1_F3_C1","IN1_F4_C1"), nullIf=Seq("\"\"")),
    ("H328218_HL7_GSHR", "INS_TIMESTAMP") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    ("H328218_HL7_Mercy", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H135772_HL7_PGN_MEM", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H135772_HL7_EP_TXBSL", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H247885_HL7", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H303173_HL7_EL", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H303173_HL7_FR", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H641171_HL7_P", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H641171_HL7_PM", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H984216_HL7_CERNER_TNNAS", "INS_TIMESTAMP") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    ("H984216_HL7_EPIC_WIGLE", "PLANTYPE") -> mapFrom("PV1_F20_C1"),
    ("H984216_HL7_INVIS_ALMOB", "PLANTYPE") -> mapFrom("IN1_F15_C1",nullIf=Seq("\"\"") , prefix = config(CLIENT_DS_ID) + "."),
    ("H984216_HL7_MCK_ALBIR", "PLANTYPE") -> mapFrom("IN1_F15_C1",nullIf=Seq("\"\"") , prefix = config(CLIENT_DS_ID) + "."),
    ("H984216_HL7_MS4_FLPEN", "PLANTYPE") -> mapFrom("IN1_F15_C1",nullIf=Seq("\"\"") , prefix = config(CLIENT_DS_ID) + "."),
    ("H171267_HL7","GROUPNBR") -> mapFrom("IN1_F8_C1",nullIf=Seq("*")),
    ("H667594_HL7", "PLANCODE") -> mapFrom("IN1_F2_C1", prefix = "hl7.")
  )

}
