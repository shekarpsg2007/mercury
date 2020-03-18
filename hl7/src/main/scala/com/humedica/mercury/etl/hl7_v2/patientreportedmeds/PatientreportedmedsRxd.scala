package com.humedica.mercury.etl.hl7_v2.patientreportedmeds

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 6/20/17.
  */
class PatientreportedmedsRxd(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_rxd_a"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","MESSAGEID","LASTUPDATEDATE","MSH_F9_C1"),
    "hl7_segment_rxd_a" -> List("MESSAGEID","RXD_F2_C1","RXD_F2_C2","RXD_F2_C3","RXD_F3_C1","RXD_F3_C2","RXD_F5_C1","RXD_F6_C1"
        ,"RXD_F6_C2","RXD_F7_C1","RXD_F9_C1")
  )

  beforeJoin = Map(
    "hl7_segment_rxd_a" -> ((df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("RXD_F3_C2"), lit("yyyymmdd")))
      val df1 = len.withColumn("RXD_F3_C2_Date",
        when(length(len("RXD_F3_C2")).lt(len("lenAddm")), expr("rpad(RXD_F3_C2, lenAddm, '0')"))
          .when(length(len("RXD_F3_C2")).gt(len("lenAddm")), expr("substr(RXD_F3_C2, 0, lenAddm)"))
          .otherwise(len("RXD_F3_C2")))
      val len2 = df1.withColumn("lenAddm", datelengthcheck(df1("RXD_F3_C1"), lit("yyyymmdd")))
      len2.withColumn("RXD_F3_C1_Date",
        when(length(len2("RXD_F3_C1")).lt(len2("lenAddm")), expr("rpad(RXD_F3_C1, lenAddm, '0')"))
          .when(length(len2("RXD_F3_C1")).gt(len2("lenAddm")), expr("substr(RXD_F3_C1, 0, lenAddm)"))
          .otherwise(len2("RXD_F3_C1")))
    })
  )

  beforeJoinExceptions = Map(
    "H704847_HL7_ELYS" -> Map(
      "temptable" -> ((df: DataFrame) => {
        includeIf("MSH_F9_C1 = 'CCD'")(df)
      })),
    "H984833_HL7_MEDENT" -> Map(
      "temptable" -> ((df: DataFrame) => {
        includeIf("MSH_F9_C1 = 'CCD'")(df)
      }))
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_rxd_a")
      .join(dfs("temptable"), Seq("MESSAGEID"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_rxd_a"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "REPORTEDMEDID" -> mapFrom("RXD_F7_C1"),
    "LOCALDOSEUNIT" -> mapFrom("RXD_F5_C1"),
    "LOCALFORM" -> mapFrom("RXD_F6_C1"),
    "LOCALROUTE" -> mapFrom("RXD_F6_C2"),
    "DISCONTINUEDATE" -> mapFrom("RXD_F3_C2_Date"),
    "LOCALDRUGDESCRIPTION" -> mapFrom("RXD_F2_C2"),
    "LOCALMEDCODE" -> mapFrom("RXD_F2_C1"),
    "MEDREPORTEDTIME" -> mapFrom("RXD_F3_C1_Date"),
    //"LOCALNDC" -> mapFrom("RXD_F9_C1"), //TODO include only if value is an 11-digit number (including leading zeroes)
    "LOCALNDC" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("RXD_F9_C1")) === "11", df("RXD_F9_C1"))
        .otherwise(null))
    }),
    "RXNORM_CODE" -> nullValue()
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID is not null and REPORTEDMEDID IS NOT NULL")
    val groups = Window.partitionBy(fil("PATIENTID"), fil("REPORTEDMEDID"), fil("MEDREPORTEDTIME")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  mapExceptions = Map(
    ("H704847_HL7_ELYS", "RXNORM_CODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RXD_F2_C3") === "RxNorm", df("RXD_F2_C1"))
        .otherwise(null))
    }),
    ("H984833_HL7_MEDENT", "RXNORM_CODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("RXD_F2_C3") === "RxNorm", df("RXD_F2_C1"))
        .otherwise(null))
    })
  )

}
