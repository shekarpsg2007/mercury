package com.humedica.mercury.etl.hl7_v2.allergies

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/26/17.
  */
class AllergiesAl1(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_al1_a"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F3_C1","MSH_F7_C1","MSH_F9_C1","MSH_F11_C1","PV1_F44_C1","MESSAGEID"),
    "hl7_segment_al1_a" -> List("AL1_F2_C1","AL1_F3_C1","AL1_F3_C4","AL1_F6_C1","AL1_F3_C1","AL1_F3_C2","AL1_F3_C3"
      ,"AL1_F3_C4","AL1_F3_C5","MESSAGEID")
  )

  beforeJoinExceptions = Map(
    "H302436_HL7" -> Map(
      "temptable" -> ((df: DataFrame) => {
        includeIf("MSH_F11_C1 ='P'")(df) ////Include where HL7_Segment_MSH_A.MSH_F11_C1 = 'P'
      })),
    "H704847_HL7_ELYS" -> Map(
      "temptable" -> ((df: DataFrame) => {
        includeIf("MSH_F9_C1 ='CCD'")(df) ////Include where HL7_Segment_MSH_A.MSH_F9_C1 = 'CCD'
      }))
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_al1_a")
      .join(dfs("temptable"), Seq("MESSAGEID"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_al1_a"),
    //"ONSETDATE" -> mapFrom("AL1_F6_C1",nullIf=Seq("\"\"")),
    "ONSETDATE" -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("AL1_F6_C1"), lit("yyyymmdd")))
      len.withColumn(col,
        when(length(len("AL1_F6_C1")).lt(len("lenAddm")), expr("rpad(AL1_F6_C1, lenAddm, '0')"))
          .when(length(len("AL1_F6_C1")).gt(len("lenAddm")), expr("substr(AL1_F6_C1, 0, lenAddm)"))
          .otherwise(len("AL1_F6_C1")))
    }),
    "LOCALALLERGENDESC" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("AL1_F3_C4").isNotNull, df("AL1_F3_C5"))
        .when(df("AL1_F3_C1") === "10000002", "OTHER")
        .otherwise(df("AL1_F3_C2")))
    }),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "LOCALALLERGENCD" -> cascadeFrom(Seq("AL1_F3_C4","AL1_F3_C1"),nullIf=Seq("\"\"")),
    "LOCALALLERGENTYPE" -> mapFrom("AL1_F2_C1",nullIf=Seq("\"\"")),
    "LOCALSTATUS" -> mapFrom("AL1_F3_C3"),
    "RXNORM_CODE" -> nullValue()
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND ONSETDATE IS NOT NULL AND LOCALALLERGENCD IS NOT NULL AND LOCALALLERGENTYPE IS NOT NULL")
    //val groups = Window.partitionBy(fil("PATIENTID"), fil("LOCALALLERGENTYPE"), fil("LOCALALLERGENCD")).orderBy(fil("AL1_F6_C1").asc)
    val groups = Window.partitionBy(fil("PATIENTID"), fil("LOCALALLERGENTYPE"), fil("LOCALALLERGENCD")).orderBy(fil("ONSETDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  mapExceptions = Map(
    ("H328218_HL7_Genesis", "LOCALALLERGENTYPE") -> cascadeFrom(Seq("AL1_F2_C1", "AL1_F3_C3"), prefix = "7135."),
    ("H328218_HL7_Genesis", "LOCALALLERGENCD") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("", lit("7135.") ,substring(coalesce(df("AL1_F2_C1"),df("AL1_F3_C3")),32,20)))
    }),
    ("H328218_HL7_Genesis", "LOCALSTATUS") -> mapFrom("AL1_F3_C3", prefix = "7135."),
    ("H328218_HL7_GRHS", "LOCALALLERGENCD") -> cascadeFrom(Seq("AL1_F3_C4","AL1_F3_C1","AL1_F3_C2")),
    ("H704847_HL7_ELYS", "LOCALALLERGENCD") -> mapFrom("AL1_F3_C1"),
    ("H704847_HL7_ELYS", "LOCALALLERGENDESC") -> mapFrom("AL1_F3_C2"),
    ("H704847_HL7_ELYS", "RXNORM_CODE") ->  ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("AL1_F3_C3") === "RxNorm", df("AL1_F3_C1"))
        .otherwise(null))
    }),
    ("H984216_HL7_CERNER_TNNAS", "LOCALALLERGENTYPE") -> cascadeFrom(Seq("AL1_F2_C1", "AL1_F3_C3")),
    ("H984216_HL7_CERNER_TNNAS", "LOCALALLERGENCD") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(upper(coalesce(df("AL1_F3_C4"),df("AL1_F3_C1"))) like "%NOMEN%", concat_ws("", lit("7397."),substring(coalesce(df("AL1_F3_C4"),df("AL1_F3_C1")),32,20)))
        .otherwise(concat_ws("", lit("7397.") ,coalesce(df("AL1_F3_C4"),df("AL1_F3_C1")))))
    }),
    ("H984216_HL7_CERNER_WIMIL", "LOCALALLERGENTYPE") -> cascadeFrom(Seq("AL1_F2_C1", "AL1_F3_C3")),
    ("H984216_HL7_CERNER_WIMIL", "LOCALALLERGENCD") -> cascadeFrom(Seq("AL1_F3_C4", "AL1_F3_C1","AL1_F3_C2"), prefix = "7398."),
    ("H984216_HL7_CERNER_WIMIL", "LOCALALLERGENDESC") -> mapFrom("AL1_F3_C2"),
    ("H984216_HL7_CN_WIAPP", "LOCALALLERGENDESC") -> cascadeFrom(Seq("AL1_F3_C5", "AL1_F3_C2","AL1_F3_C4","AL1_F3_C1")),
    ("H257275_HL7", "LOCALALLERGENCD") -> cascadeFrom(Seq("AL1_F3_C4", "AL1_F3_C1"), prefix = "7948."),
    ("H641171_HL7_P", "LOCALALLERGENCD") -> cascadeFrom(Seq("AL1_F3_C4","AL1_F3_C1"), prefix = "7656."),
    ("H641171_HL7_P", "LOCALALLERGENDESC") -> cascadeFrom(Seq("AL1_F3_C5","AL1_F3_C2")),
    ("H641171_HL7_PM", "LOCALALLERGENCD") -> mapFrom("AL1_F3_C2")
  )

}
