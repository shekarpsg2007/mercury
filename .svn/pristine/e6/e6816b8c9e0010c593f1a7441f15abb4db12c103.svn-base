package com.humedica.mercury.etl.hl7_v2.encounterprovider

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 6/14/17.
  */
class EncounterproviderPv1(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {



  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","ENCOUNTERID","LASTUPDATEDATE","PV1_F44_C1","PV1_F7_C1","PV1_F8_C1","PV1_F9_C1","PV1_F17_C1","PV1_F52_C1","MSH_F7_C1","MSH_F4_C1")
  )


  //TODO beforejoin exceptions if any
  beforeJoinExceptions = Map(
    "H770635_HL7_NG" -> Map(
      "temptable" -> ((df: DataFrame) => {
        val df1 = df.withColumn("PV1_F7", concat_ws("",df("PV1_F7_C2"),df("PV1_F7_C3"),df("PV1_F7_C4")))
        val df2 = df1.withColumn("PV1_F8", concat_ws("",df1("PV1_F8_C2"),df1("PV1_F8_C3"),df1("PV1_F8_C4")))
        val df3 = df2.withColumn("PV1_F9", concat_ws("",df2("PV1_F9_C2"),df2("PV1_F9_C3"),df2("PV1_F9_C4")))
        df3.withColumn("PV1_F17",concat_ws("",df3("PV1_F17_C2"),df3("PV1_F17_C3"),df3("PV1_F17_C4")))
      })
    )
  )

  join = noJoin()

  map = Map(
    "DATASRC" -> literal("hl7_segment_pv1_a"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "ENCOUNTERTIME" -> mapFrom("PV1_F44_C1")
  )

  afterMap = (df: DataFrame) => {

    val fpiv = unpivot(
      Seq("PV1_F7_C1","PV1_F8_C1","PV1_F9_C1","PV1_F17_C1"),
      Seq("HL7 Attending provider", "HL7 Referring provider", "HL7 Consulting provider", "HL7 Admitting provider"),typeColumnName = "PROVIDERROLE")

    val df1 = fpiv("PROVIDERID",df)

    val fil = df1.filter("PATIENTID IS NOT NULL AND ENCOUNTERTIME IS NOT NULL AND PROVIDERID IS NOT NULL")
    val groups = Window.partitionBy(fil("ENCOUNTERID")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  afterMapExceptions = Map(
    ("H704847_HL7_CCD", (df: DataFrame) => {
      val fpiv = unpivot(
        Seq("PV1_F7_C1","PV1_F8_C1","PV1_F9_C1","PV1_F17_C1"),
        Seq("HL7 Attending provider", "HL7 Referring provider", "HL7 Consulting provider", "HL7 Admitting provider"),typeColumnName = "PROVIDERROLE")

      val df1 = fpiv("PROVIDERID",df)

      val df2 = df1.withColumn("PROVIDERID", concat(df1("MSH_F4_C1"), lit("."), df1("PROVIDERID")))

      val fil = df2.filter("PATIENTID IS NOT NULL AND ENCOUNTERID IS NOT NULL AND ENCOUNTERTIME IS NOT NULL")
      val groups = Window.partitionBy(fil("ENCOUNTERID"),fil("PROVIDERID"),fil("PROVIDERROLE")).orderBy(fil("LASTUPDATEDATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    }),
    ("H704847_HL7_OCIE", (df: DataFrame) => {
      val fpiv = unpivot(
        Seq("PV1_F7_C1","PV1_F8_C1","PV1_F9_C1","PV1_F17_C1"),
        Seq("HL7 Attending provider", "HL7 Referring provider", "HL7 Consulting provider", "HL7 Admitting provider"),typeColumnName = "PROVIDERROLE")

      val df1 = fpiv("PROVIDERID",df)

      val df2 = df1.withColumn("PROVIDERID", concat(df1("MSH_F4_C1"), lit("."), df1("PROVIDERID")))

      val fil = df2.filter("PATIENTID IS NOT NULL AND ENCOUNTERID IS NOT NULL AND ENCOUNTERTIME IS NOT NULL")
      val groups = Window.partitionBy(fil("ENCOUNTERID"),fil("PROVIDERID"),fil("PROVIDERROLE")).orderBy(fil("LASTUPDATEDATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    }),
    ("H770635_HL7_NG", (df: DataFrame) => {
      val fpiv = unpivot(
        Seq("PV1_F7","PV1_F8","PV1_F9","PV1_F17"),
        Seq("HL7 Attending provider", "HL7 Referring provider", "HL7 Consulting provider", "HL7 Admitting provider"),typeColumnName = "PROVIDERROLE")

      val df1 = fpiv("PROVIDERID",df)

      val fil = df1.filter("PATIENTID IS NOT NULL AND ENCOUNTERTIME IS NOT NULL")
      val groups = Window.partitionBy(fil("ENCOUNTERID"),fil("PROVIDERID"),fil("PROVIDERROLE")).orderBy(fil("LASTUPDATEDATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    })
  )

  //todo mapExceptions

  mapExceptions = Map(
    ("H451171_HL7_MDT", "ENCOUNTERTIME") -> cascadeFrom(Seq("PV1_F44_C1", "LASTSUPDATEDATE")),
    /*("H542284_HL7_ATH", "ENCOUNTERTIME") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, unix_timestamp(substring(df("EVN_F2_C1"),1,14), "yyyyMMddHHmmss").cast("timestamp"))
    }),*/
    //("H542284_HL7_ATH", "ENCOUNTERTIME") -> mapFrom("EVN_F2_C1"),
    ("H542284_HL7_ATH", "ENCOUNTERTIME") -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("EVN_F2_C1"), lit("yyyymmddhh24miss")))
      len.withColumn(col,
        when(length(len("EVN_F2_C1")).lt(len("lenAddm")), expr("rpad(EVN_F2_C1, lenAddm, '0')"))
          .when(length(len("EVN_F2_C1")).gt(len("lenAddm")), expr("substr(EVN_F2_C1, 0, lenAddm)"))
          .otherwise(len("EVN_F2_C1")))
    }),
    ("H984216_HL7_CERNER_WIMIL", "PROVIDERID") -> mapFrom("PV1_F52_C1"),
    ("H984216_HL7_CERNER_WIMIL", "PROVIDERROLE") -> literal("Other Healthcare Provider"),
    ("H984833_HL7_MEDENT", "PROVIDERROLE") -> mapFrom("PV1_F7_C3")
  )
}
