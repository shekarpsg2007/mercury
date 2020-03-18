package com.humedica.mercury.etl.hl7_v2.patientdetail

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/30/17.
  */
class PatientdetailRace(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F7_C1","PID_F29_C1","PID_F30_C1","PID_F22_C1","PID_F9_C2","PID_F5_C2"
      ,"PID_F8_C1","PID_F15_C1","PID_F9_C1","PID_F5_C1","PID_F16_C1","PID_F3_C1","PID_F10_C1","PID_F17_C1"
      ,"PID_F17_C2","MSH_F7_C1","MSH_F9_C1","MSH_F11_C1","PV1_F44_C1","LASTUPDATEDATE","PID_F10_C2")
  )


  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val fil = df.filter("PATIENTID is not null")
      fil.groupBy("PATIENTID","PID_F7_C1","PID_F29_C1","PID_F30_C1","PID_F22_C1","PID_F9_C2","PID_F5_C2"
        ,"PID_F8_C1","PID_F15_C1","PID_F9_C1","PID_F5_C1","PID_F16_C1","PID_F3_C1","PID_F10_C1","PID_F17_C1"
        ,"PID_F17_C2","PID_F10_C2")
        .agg(max("LASTUPDATEDATE").as("UPDATE_DATE"))
    })
  )


  beforeJoinExceptions = Map(
    "H416989_HL7" -> Map(
      "temptable" -> ((df: DataFrame) => {
        //Include where HL7_Segment_MSH_A.MSH_F9_C1 in ('SIU','ADT')
        includeIf("MSH_F9_C1 in ('SIU','ADT')")(df)
      })),
    "H416989_HL7_32" -> Map(
      "temptable" -> ((df: DataFrame) => {
        //exclude where PID_f5_c1 like '%TESTPT%' or PID_f5_c1 like '%CERTPT%'
        excludeIf("PID_F5_C1 like '%TESTPT%' OR PID_F5_C1 like '%CERTPT%'")(df)
      })),
    "H416989_HL7_55" -> Map(
      "temptable" -> ((df: DataFrame) => {
        //Include where HL7_Segment_MSH_A.MSH_F9_C1 in ('SIU','ADT', 'ORM')
        includeIf("MSH_F9_C1 in ('SIU','ADT','ORM')")(df)
      })),
    "H416989_HL7_86" -> Map(
      "temptable" -> ((df: DataFrame) => {
        //exclude where PID_f5_c1 like '%TESTPT%' or PID_f5_c1 like '%CERTPT%'
        excludeIf("PID_F5_C1 like '%TESTPT%' OR PID_F5_C1 like '%CERTPT%'")(df)
      })),
    "H302436_HL7" -> Map(
      "temptable" -> ((df: DataFrame) => {
        ////Include only where MSH_F11_C1 = 'P'
        includeIf("MSH_F11_C1 = 'P'")(df)
      })),
    "H908583_HL7" -> Map(
      "temptable" -> ((df: DataFrame) => {
        //Include where HL7_Segment_MSH_A.MSH_F9_C1 in ('SIU','ADT')
        includeIf("MSH_F9_C1 in ('SIU','ADT')")(df)
      })),
    "H641171_HL7_PM" -> Map(
      "temptable" -> ((df: DataFrame) => {
        excludeIf("upper(PID_F5_C2)='ABC' and upper(PID_F5_C1) = 'TEST'")(df)
      })),
    "H171267_HL7_GE_IDX" -> Map(
      "temptable" -> ((df: DataFrame) => {
        excludeIf("PID_F5_C1 = 'ZZTEST' OR (PID_F5_C2 = 'TEST' AND PID_F5_C1 = 'TEST')")(df)
      })),
    "H171267_HL7_PHS" -> Map(
      "temptable" -> ((df: DataFrame) => {
        excludeIf("PID_F5_C2 = 'TEST' AND PID_F5_C1 = 'TEST'")(df)
      }))
    /*("H542284_HL7") -> Map(
      "temptable" -> ((df: DataFrame) => {
        //TODO Exclude records where (HL7_Segment_Z.Segment_Name = 'ZAS' and HL7_Segment_Z.Z_F29_C1 = 'PHONE').
        //excludeIf("SEGMENT_NAME = 'ZAS' AND Z_F29_C1 = 'PHONE'")(df)
    })),
    ("H542284_HL7_ATH") -> Map(
      "temptable" -> ((df: DataFrame) => {
        //TODO Exclude where length(PID_F2_C1) < 3
        //val len = "PID_F2_C1".length()
        //excludeIf("len < 3")
    }))

    ("H101623_HL7") -> Map(
      "temptable" -> ((df: DataFrame) => {
        /**
          * include only where (MSH_F3_C1 or MSH_F4_C1) = 'EAGLE 2000'and (PV1_F41_C1 not in ('CP', 'VA') or PV1_F41_C1 is null)
          * deduplicate on the CDR patientID||PV1_F44_C1 order by PV1_F41_C1 in the following order: 'BL', 'IA', 'AC', 'PA', 'VA', 'CP' then by MSH_F7_C1 desc.  After this sort, only row number 1 should be inserted into the table.   client informed us any patientid in PID_F2_C1 that is not 10 digits and starting with a '1' are test patients.
          */
        val deduped = bestRowPerGroup(List(concat("PATIENTID","PV1_F44_C1")), "PV1_F44_C1")(df)
    }))
   */
  )


  join = noJoin()


  map = Map(
    "DATASRC" -> literal("hl7_segment_pid_a"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PATIENTDETAILTYPE" -> literal("RACE"),
    "PATDETAIL_TIMESTAMP" -> mapFrom("UPDATE_DATE"), //TODO : As per ETL get max(MSH_F7_C1)
    "LOCALVALUE" -> mapFrom("PID_F10_C1") //TODO
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("LOCALVALUE IS NOT NULL")
    val groups = Window.partitionBy(fil("PATIENTID"), lower(fil("LOCALVALUE"))).orderBy(fil("PATDETAIL_TIMESTAMP").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  mapExceptions = Map(
    ("H285893_HL7", "LOCALVALUE") -> mapFrom(null), //TODO
    ("H171267_HL7", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7708."),
    ("H328218_HL7_Genesis", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7135."),
    ("H328218_HL7_UIHC", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7133."),
    ("H328218_HL7_GRHS", "LOCALVALUE") -> mapFrom("PID_10_C1", prefix = "7136."),
    ("H524284_HL7_ATH", "LOCALVALUE") -> cascadeFrom(Seq("PID_F10_C1","PID_F22_C1"), prefix = "6964."),
    ("H704847_HL7", "LOCALVALUE") -> cascadeFrom(Seq("PID_F10_C2","PID_F10_C1")),
    ("H704847_HL7_CCD", "LOCALVALUE") -> cascadeFrom(Seq("PID_F10_C2", "PID_F10_C1"),nullIf=Seq("\"\"")),
    ("H704847_HL7_OCIE", "LOCALVALUE") -> cascadeFrom(Seq("PID_F10_C2", "PID_F10_C1"),nullIf=Seq("\"\"")),
    ("H704847_HL7_ELYS", "LOCALVALUE") -> nullValue(),
    ("H729838_HL7", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "3941."),
    ("H908583_HL7", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "1481."),
    ("H302436_HL7", "LOCALVALUE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PID_F10_C1").isNotNull, concat_ws("",lit("1981."),df("PID_F10_C1")))
        .otherwise(null))
    }),
    ("H984216_HL7_CERNER_FLJAC", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7396."),
    ("H984216_HL7_CERNER_TNNAS", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7397."),
    ("H984216_HL7_CERNER_WIMIL", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7398."),
    ("H984216_HL7_CN_WIAPP", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7399."),
    ("H984216_HL7_INVIS_ALMOB", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7401."),
    ("H984216_HL7_INVIS_INEVA", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7402."),
    ("H984216_HL7_MCK_ALBIR", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7403."),
    ("H984216_HL7_MS4_FLPEN", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7404."),
    ("H984216_HL7_SOAR_ININD", "LOCALVALUE") -> mapFrom("PID_F10_C1", prefix = "7405.")
  )

}
