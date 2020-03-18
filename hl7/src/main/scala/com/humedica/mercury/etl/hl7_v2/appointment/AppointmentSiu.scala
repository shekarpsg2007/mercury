package com.humedica.mercury.etl.hl7_v2.appointment


import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/26/17.
  */
class AppointmentSiu(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_sch_a"
    ,"hl7_segment_ail_a"
    ,"hl7_segment_aip_a"
    ,"hl7_segment_rgs_a"
    ,"cdr.map_predicate_values"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F3_C1","MSH_F4_C1","MSH_F7_C1","MSH_F9_C1","MSH_F11_C1","PV1_F44_C1","PV1_F7_C1","MESSAGEID","LASTUPDATEDATE"),
    "hl7_segment_sch_a" -> List("MESSAGEID","SCH_F11_C1","SCH_F11_C4","SCH_F1_C1","SCH_F2_C1","SCH_F7_C1","SCH_F25_C1"),
    "hl7_segment_ail_a" -> List("MESSAGEID","AIL_F3_C1","AIL_F3_C2"),
    "hl7_segment_aip_a" -> List("MESSAGEID","AIP_F3_C1", "AIP_F3_C2", "IDX"),
    "hl7_segment_rgs_a" -> List("MESSAGEID","RGS_F3_C1")
  )

  beforeJoin = Map(
    "hl7_segment_sch_a" -> ((df: DataFrame) => {
      val list_sch_f25_c1_incl = mpvList(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SIU", "APPOINTMENT", "HL7_SEGMENT_SCH_A", "SCH_F25_C1")
      val addColumn = df.withColumn("nullColumn",lit("'NO_MPV_MATCHES'"))
      addColumn.withColumn("include_rows", when(addColumn("nullColumn") isin(list_sch_f25_c1_incl:_*),"y"))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("hl7_segment_sch_a"), Seq("MESSAGEID"), "inner")
      .join(dfs("hl7_segment_ail_a"), Seq("MESSAGEID"), "left_outer")
      .join(dfs("hl7_segment_aip_a"), Seq("MESSAGEID"), "left_outer")
  }

  joinExceptions = Map(
    "H542284_HL7" -> ((dfs: Map[String, DataFrame]) => {
      dfs("temptable")
        .join(dfs("hl7_segment_sch_a"), Seq("MESSAGEID"), "inner")
        .join(dfs("hl7_segment_ail_a"), Seq("MESSAGEID"), "left_outer")
        .join(dfs("hl7_segment_aip_a"), Seq("MESSAGEID"), "inner")
    }),
    "H171267_HL7_PHS" -> ((dfs: Map[String, DataFrame]) => {
      dfs("temptable")
        .join(dfs("hl7_segment_sch_a"), Seq("MESSAGEID"), "inner")
        .join(dfs("hl7_segment_aip_a"), Seq("MESSAGEID"), "left_outer")
        .join(dfs("hl7_segment_ail_a"), Seq("MESSAGEID"), "left_outer")
        .join(dfs("hl7_segment_rgs_a"), Seq("MESSAGEID"), "left_outer")
    }),
    "H557454_HL7" -> ((dfs: Map[String, DataFrame]) => {
      dfs("temptable")
        .join(dfs("hl7_segment_sch_a"), Seq("MESSAGEID"), "inner")
        .join(dfs("hl7_segment_aip_a"), Seq("MESSAGEID"), "left_outer")
        //.join(dfs("hl7_segment_ail_a"), Seq("MESSAGEID"), "left_outer")
        .join(dfs("hl7_segment_rgs_a"), Seq("MESSAGEID"), "inner")
    })
  )

  map = Map(
    "DATASRC" -> literal("SIU"),
    "PATIENTID" -> mapFrom("PATIENTID"),
   /* "APPOINTMENTDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(date_format(df("SCH_F11_C1"), "yyyymmddhhmm"), date_format(df("SCH_F11_C4"), "yyyymmddhhmm")))
    }),*/
    "APPOINTMENTDATE" -> cascadeFrom(Seq("SCH_F11_C1","SCH_F11_C4")),
    "APPOINTMENTID" -> cascadeFrom(Seq("SCH_F1_C1", "SCH_F2_C1")),
    "LOCATIONID" -> todo("AIL_F3_C1"),
    "PROVIDERID" -> mapFrom("AIP_F3_C1"),
    "APPOINTMENT_REASON" -> mapFrom("SCH_F7_C1")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("APPOINTMENTID")).orderBy(df("LASTUPDATEDATE").desc, df("MESSAGEID").desc, df("IDX"))
    val addColumn = df.withColumn("rn", row_number.over(groups))
   addColumn.filter("rn = 1 and PATIENTID IS NOT NULL AND LOCATIONID IS NOT NULL AND APPOINTMENTID IS NOT NULL AND APPOINTMENTDATE IS NOT NULL").drop("rn")
  }

  afterMapExceptions = Map(
    ("H171267_HL7_PHS", (df: DataFrame) => {
      val groups = Window.partitionBy(df("APPOINTMENTID")).orderBy(df("LASTUPDATEDATE").desc, df("MESSAGEID").desc, df("IDX"))
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 and include_rows = 'y' and PATIENTID IS NOT NULL AND LOCATIONID IS NOT NULL " +
        "AND APPOINTMENTID IS NOT NULL AND APPOINTMENTDATE IS NOT NULL AND MSH_F9_C2 NOT IN ('S15','S26')").drop("rn")
    })
  )

  mapExceptions = Map(
    ("H135772_HL7_EP_TXBSL", "APPOINTMENTID") -> mapFrom("SCH_F2_C1"),
    ("H135772_HL7_EP_TXBSL", "LOCATIONID") -> cascadeFrom(Seq("AIL_F3_C1", "MSH_F4_C1")),
    ("H542284_HL7_ATH", "PROVIDERID") -> mapFrom("PV1_F7_C1"),
    ("H171267_HL7_PHS", "LOCATIONID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("RGS_F3_C1"), when(df("AIL_F3_C1").isNull, "None").otherwise(concat_ws("",df("AIL_F3_C1"),lit("_"),df("AIL_F3_C2")))))
    }),
    ("H406239_HL7_CENTRAL_V1", "LOCATIONID") -> cascadeFrom(Seq("AIL_F3_C2","MSH_F4_C1")),
    ("H406239_HL7_EAST_V1", "LOCATIONID") -> cascadeFrom(Seq("AIL_F3_C2","MSH_F4_C1")),
    ("H406239_HL7_WEST_V1", "LOCATIONID") -> cascadeFrom(Seq("AIL_F3_C2","MSH_F4_C1")),
    ("H557454_HL7", "LOCATIONID") -> mapFrom("RGS_F3_C1"),
    ("H641171_HL7_PM", "LOCALTIONID") -> cascadeFrom(Seq("AIL_F3_C1","AIL_F3_C2")),
    ("H770635_HL7_NG", "PROVIDERID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("AIP_F3_C1").isNull, concat_ws("",df("AIP_F3_C2"),df("AIP_F3_C3"),df("AIP_F3_C4")))
        .otherwise(null))
    })
)

}
