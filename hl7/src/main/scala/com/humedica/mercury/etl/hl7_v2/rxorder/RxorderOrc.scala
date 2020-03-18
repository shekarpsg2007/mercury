package com.humedica.mercury.etl.hl7_v2.rxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 9/25/17.
  */
class RxorderOrc(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_orc_a"
    ,"hl7_segment_rxo_a"
    ,"hl7_segment_rxr_a")

  columnSelect = Map(
    "temptable" -> List("PATIENTID","MESSAGEID","LASTUPDATEDATE","ENCOUNTERID","PV1_F44_C1","MSH_F11_C1"),
    "hl7_segment_orc_a" -> List("ORC_F2_C1","ORC_F5_C1","ORC_F7_C2","ORC_F7_C5","ORC_F9_C1","ORC_F12_C1"
      ,"MESSAGEID"),
    "hl7_segment_rxo_a" -> List("RXO_F1_C1","RXO_F1_C2","RXO_F2_C1","RXO_F3_C1","RXO_F4_C1","RXO_F5_C1"
      ,"RXO_F13_C1","MESSAGEID"),
    "hl7_segment_rxr_a" -> List("RXR_F1_C1","MESSAGEID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("hl7_segment_orc_a"), Seq("MESSAGEID"), "inner")
      .join(dfs("hl7_segment_rxo_a"), Seq("MESSAGEID"), "inner")
      .join(dfs("hl7_segment_rxr_a"),Seq("MESSAGEID"), "inner")
  }

  joinExceptions = Map(
    "H302436_HL7" -> ((dfs: Map[String, DataFrame]) => {
      dfs("temptable")
        .join(dfs("hl7_segment_orc_a"), Seq("MESSAGEID"), "inner")
        .join(dfs("hl7_segment_rxe_a"), Seq("MESSAGEID"), "inner")
        .join(dfs("hl7_segment_rxr_a"),Seq("MESSAGEID"), "inner")
    })
  )

  map = Map(
    "DATASRC" -> literal("hl7_segment_orc_a"),
    "ISSUEDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, unix_timestamp(substring(df("ORC_F9_C1"),1,14), "yyyyMMddHHmmss").cast("timestamp"))
    }),
    "ORDERVSPRESCRIPTION" -> literal("O"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "RXID" -> mapFrom("ORC_F2_C1"),
    "DISCONTINUEDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, unix_timestamp(substring(df("ORC_F7_C5"),1,8), "yyyyMMdd").cast("timestamp"))
    }),
    "LOCALDOSEFREQ" -> mapFrom("ORC_F7_C2"),
    "LOCALDESCRIPTION" -> mapFrom("RXO_F1_C2"),
    "LOCALFORM" -> mapFrom("RXO_F5_C1"),
    "LOCALMEDCODE" -> cascadeFrom(Seq("RXO_F1_C1","RXO_F1_C2")),
    "LOCALPROVIDERID" -> mapFrom("ORC_F12_C1", nullIf = Seq("\"\"")),
    "LOCALROUTE" -> mapFrom("RXR_F1_C1"),
    "LOCALTOTALDOSE" -> mapFrom("RXO_F2_C1"),
    "FILLNUM" -> mapFrom("RXO_F13_C1"),
    "ORDERSTATUS" -> mapFrom("ORC_F5_C1"),
    "VENUE" -> literal("0")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND ISSUEDATE IS NOT NULL AND RXID IS NOT NULL AND ORC_F5_C1 <> 'ER' AND RXR_F1_C1 <> 'OTH'")
    val groups = Window.partitionBy(fil("PATIENTID"),fil("LOCALMEDCODE"),fil("LOCALTOTALDOSE"),fil("LOCALDOSEUNIT"),fil("LOCALPROVIDERID"),fil("ORC_F7_C4")).orderBy(fil("ISSUEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  afterMapExceptions = Map(
    ("H704847_HL7_CCD", (df: DataFrame) => {
      val fil = df.filter("PATIENTID IS NOT NULL AND ISSUEDATE IS NOT NULL AND RXID IS NOT NULL AND ORC_F5_C1 <> 'ER' AND RXR_F1_C1 <> 'OTH'")
      val groups = Window.partitionBy(fil("RXID"), fil("LOCALPROVIDERID")).orderBy(fil("ISSUEDATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")

    })
  )

  mapExceptions = Map(
    ("H302436_HL7","RXID") -> mapFrom("MESSAGEID"),
    ("H302436_HL7","LOCALDOSEFREQ") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ORC_F7_C2") like "%&%", split(df("ORC_F7_C2"),"&")(0)).otherwise(df("ORC_F7_C2")))
    }),
    ("H302436_HL7","LOCALDOSEUNIT") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RXE_F6_C1") === "IV", df("RXE_F5_C1")).otherwise(null))
    }),
    ("H302436_HL7", "LOCALDESCRIPTION") -> mapFrom("RXE_F2_C2"),
    ("H302436_Hl7", "LOCALMEDCODE") -> mapFrom("RXE_F2_C1"),
    ("H302436_HL7", "LOCALTOTALDOSE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RXE_F6_C1") === "IV", df("RXE_F3_C1")).otherwise(null))
    })
  )

}
