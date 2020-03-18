package com.humedica.mercury.etl.hl7_v2.temptable

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


/**
  * Created by abendiganavale on 5/26/17.
  */
class TemptableAll(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  cacheMe = true


  tables = List("hl7_segment_msh_a"
    ,"hl7_segment_pid_a"
    ,"hl7_segment_pv1_a"
    //,"hl7_segment_z" TODO H542284_HL7 exception
  )

  columnSelect = Map(
    "hl7_segment_msh_a" -> List("MSH_F3_C1", "MSH_F7_C1", "MSH_F9_C1", "MSH_F12_C1", "MSH_F4_C1","MESSAGEID","FILE_ID", "MSH_F11_C1","MSH_F10_C1"),
    "hl7_segment_pid_a" -> List("PID_F2_C1", "PID_F2_C4", "PID_F3_C1", "PID_F3_C4", "PID_F7_C1", "PID_F29_C1","PID_F30_C1","PID_F22_C1"
      ,"PID_F22_C2","PID_F5_C2","PID_F9_C2","PID_F8_C1","PID_F15_C1","PID_F15_C2","PID_F5_C1","PID_F9_C1"
      ,"PID_F16_C1","PID_F16_C2","PID_F5_C3","PID_F10_C1","PID_F10_C2","PID_F17_C1","PID_F17_C2","MESSAGEID"
      ,"PID_F18_C1","PID_F11_C1","PID_F11_C2","PID_F11_C3","PID_F11_C4","PID_F11_C5","PID_F11_C7"
      ,"PID_F13_C1","PID_F14_C1","PID_F19_C1","PID_F13_C4"),
    "hl7_segment_pv1_a" -> List("PV1_F2_C1","PV1_F3_C1","PV1_F3_C4","PV1_F3_C5","PV1_F7_C1","PV1_F7_C2","PV1_F7_C3","PV1_F7_C4","PV1_F7_C6"
      ,"PV1_F8_C1","PV1_F8_C2","PV1_F8_C3","PV1_F8_C4","PV1_F8_C6","PV1_F9_C1","PV1_F9_C2","PV1_F9_C3","PV1_F9_C4","PV1_F9_C6"
      ,"PV1_F10_C1","PV1_F14_C1","PV1_F17_C1","PV1_F17_C2","PV1_F17_C3","PV1_F17_C4","PV1_F17_C6","PV1_F36_C1","PV1_F39_C1","PV1_F44_C1"
      ,"PV1_F45_C1","PV1_F50_C1","PV1_F52_C1","PV1_F20_C1","MESSAGEID")
  )

  columns = List("MESSAGEID", "FILE_ID", "PATIENTID", "ENCOUNTERID","LASTUPDATEDATE","PV1_F44_C1_DIAG"
    ,"MSH_F3_C1", "MSH_F7_C1", "MSH_F9_C1", "MSH_F12_C1", "MSH_F4_C1", "MSH_F11_C1","MSH_F10_C1"
    ,"PID_F2_C1", "PID_F2_C4", "PID_F3_C1", "PID_F3_C4", "PID_F7_C1", "PID_F29_C1","PID_F30_C1","PID_F22_C1","PID_F22_C2"
    ,"PID_F5_C2","PID_F9_C2","PID_F8_C1","PID_F15_C1","PID_F15_C2","PID_F5_C1","PID_F9_C1","PID_F16_C1","PID_F16_C2"
    ,"PID_F5_C3","PID_F10_C1","PID_F10_C2","PID_F17_C1","PID_F17_C2","PID_F18_C1","PID_F11_C1","PID_F11_C2","PID_F11_C3"
    ,"PID_F11_C4","PID_F11_C5","PID_F11_C7","PID_F13_C1","PID_F14_C1","PID_F19_C1","PID_F13_C4"
    ,"PV1_F2_C1","PV1_F3_C1","PV1_F3_C4","PV1_F3_C5","PV1_F7_C1","PV1_F7_C2","PV1_F7_C3","PV1_F7_C4","PV1_F7_C6","PV1_F8_C1"
    ,"PV1_F8_C2","PV1_F8_C3","PV1_F8_C4","PV1_F8_C6","PV1_F9_C1","PV1_F9_C2","PV1_F9_C3","PV1_F9_C4","PV1_F9_C6","PV1_F10_C1"
    ,"PV1_F14_C1","PV1_F17_C1","PV1_F17_C2","PV1_F17_C3","PV1_F17_C4","PV1_F17_C6","PV1_F20_C1","PV1_F36_C1","PV1_F39_C1","PV1_F44_C1"
    ,"PV1_F45_C1","PV1_F50_C1","PV1_F52_C1","PV1_F44_C1_Diag")

  beforeJoin = Map(
    "hl7_segment_pv1_a"  -> ((df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("PV1_F44_C1"), lit("yyyymmddhh24mi")))
      len.withColumn("PV1_F44_C1_Diag",
        when(length(len("PV1_F44_C1")).lt(len("lenAddm")), expr("rpad(PV1_F44_C1, lenAddm, '0')"))
          .when(length(len("PV1_F44_C1")).gt(len("lenAddm")), expr("substr(PV1_F44_C1, 0, lenAddm)"))
          .otherwise(len("PV1_F44_C1")))
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_msh_a")
      .join(dfs("hl7_segment_pid_a"), Seq("MESSAGEID"), "left_outer")
      .join(dfs("hl7_segment_pv1_a"), Seq("MESSAGEID"), "left_outer")
  }

  map = Map(
    //"PATIENTID" -> mapFrom("PID_F3_C1"),
    "PATIENTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PID_F3_C4").isNotNull, concat_ws("",df("PID_F3_C4"),lit("."),df("PID_F3_C1")))
        .otherwise(df("PID_F3_C1")))
    }),
    "ENCOUNTERID" -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PID_F18_C1"), substring(df("PV1_F2_C1"),1,1)))),
    "PV1_F7_C1" -> mapFrom("PV1_F7_C1", nullIf=Seq("0000000000")),
    "PV1_F8_C1" -> mapFrom("PV1_F8_C1", nullIf=Seq("0000000000")),
    "PV1_F9_C1" -> mapFrom("PV1_F9_C1", nullIf=Seq("0000000000")),
    "PV1_F17_C1" -> mapFrom("PV1_F17_C1", nullIf=Seq("0000000000")),
    "PID_F18_C1" -> ((col:String,df:DataFrame) => df.withColumn(col, ltrim(df("PID_F18_C1")))),
    "PV1_F44_C1" -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("PV1_F44_C1"), lit("yyyymmddhh24miss")))
      len.withColumn(col,
        when(length(len("PV1_F44_C1")).lt(len("lenAddm")), expr("rpad(PV1_F44_C1, lenAddm, '0')"))
          .when(length(len("PV1_F44_C1")).gt(len("lenAddm")), expr("substr(PV1_F44_C1, 0, lenAddm)"))
          .otherwise(len("PV1_F44_C1")))
    }),
    "PV1_F45_C1" -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("PV1_F45_C1"), lit("yyyymmddhh24miss")))
      len.withColumn(col,
        when(length(len("PV1_F45_C1")).lt(len("lenAddm")), expr("rpad(PV1_F45_C1, lenAddm, '0')"))
          .when(length(len("PV1_F45_C1")).gt(len("lenAddm")), expr("substr(PV1_F45_C1, 0, lenAddm)"))
          .otherwise(len("PV1_F45_C1")))
    }),
    "LASTUPDATEDATE" -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("MSH_F7_C1"), lit("yyyymmddhh24mi")))
      len.withColumn(col,
        when(length(len("MSH_F7_C1")).lt(len("lenAddm")), expr("rpad(MSH_F7_C1, lenAddm, '0')"))
          .when(length(len("MSH_F7_C1")).gt(len("lenAddm")), expr("substr(MSH_F7_C1, 0, lenAddm)"))
          .otherwise(len("MSH_F7_C1")))
    }),
    "PID_F7_C1" -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("PID_F7_C1"), lit("yyyymmddhh24miss")))
      len.withColumn(col,
        when(length(len("PID_F7_C1")).lt(len("lenAddm")), from_unixtime(unix_timestamp(expr("rpad(PID_F7_C1, lenAddm, '0')"), "yyyyMMddHHmmss")))
          .when(length(len("PID_F7_C1")).gt(len("lenAddm")), from_unixtime(unix_timestamp(expr("substr(PID_F7_C1, 0, lenAddm)"), "yyyyMMddHHmmss")))
          .otherwise(from_unixtime(unix_timestamp(len("PID_F7_C1"), "yyyyMMddHHmmss"))))
    }),
    "PID_F29_C1" -> ((col: String, df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("PID_F29_C1"), lit("yyyymmddhh24miss")))
      len.withColumn(col,
        when(length(len("PID_F29_C1")).lt(len("lenAddm")), expr("rpad(PID_F29_C1, lenAddm, '0')"))
          .when(length(len("PID_F29_C1")).gt(len("lenAddm")), expr("substr(PID_F29_C1, 0, lenAddm)"))
          .otherwise(len("PID_F29_C1")))
    })
  )

  afterMapExceptions = Map(
    ("H704847_HL7_OCIE", (df: DataFrame) => {
      df.filter("PID_F3_C1 not like 'Dr.%' AND " +
        "PID_F3_C1 not in ('99999999','12','25','NONE','NA','26','24','N','M','CHOE','UNASSIGNED','NGUYEN','#12','Orbush')")
    }),
    ("H704847_HL7_CCD", (df: DataFrame) => {
      df.filter("PID_F3_C1 not like 'Dr.%' AND " +
        "PID_F3_C1 not in ('99999999','12','25','NONE','NA','26','24','N','M','CHOE','UNASSIGNED','NGUYEN','#12','Orbush')")
    })

  )

  mapExceptions = Map(
    //Patientid Exceptions
    ("H458934_HL7", "PATIENTID") -> mapFrom("PID_F2_C1"),
    ("H542284_HL7_ATH", "PATIENTID") -> mapFrom("PID_F2_C1"),
    ("H542284_HL7_ATH", "MEDICALRECORDNUMBER") -> mapFrom("PID_F2_C1"),
    //todo H667594 patientid
    ("H704847_HL7_CCD", "PATIENTID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("",
        when(df("MSH_F4_C1").isNotNull, concat_ws("", df("MSH_F4_C1"), lit("."))).otherwise(null)
        ,when(df("PID_F3_C1").isNotNull,
          when(df("PID_F3_C4").isNotNull, concat_ws("", df("PID_F3_C4"), lit("."), df("PID_F3_C1")))
            .otherwise(df("PID_F3_C1")))
          .otherwise(when(df("PID_F2_C4").isNotNull, concat_ws("", df("PID_F2_C4"), lit("."), df("PID_F2_C1"))).otherwise(df("PID_F2_C4"))))
      )
    }),
    ("H704847_HL7_OCIE", "PATIENTID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("",
        when(df("MSH_F4_C1").isNotNull, concat_ws("", df("MSH_F4_C1"), lit("."))).otherwise(null)
        ,when(df("PID_F3_C1").isNotNull,
          when(df("PID_F3_C4").isNotNull, concat_ws("", df("PID_F3_C4"), lit("."), df("PID_F3_C1")))
          .otherwise(df("PID_F3_C1")))
        .otherwise(when(df("PID_F2_C4").isNotNull, concat_ws("", df("PID_F2_C4"), lit("."), df("PID_F2_C1"))).otherwise(df("PID_F2_C4"))))
      )
    }),
    ("H704847_HL7", "PATIENTID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("MSH_F9_C1") === "ORU", concat_ws("",df("MSH_F12_C1"), df("PID_F3_C4"), df("PID_F3_C1")))
        .otherwise(concat(df("MSH_F3_C1"), df("PID_F3_C1"))))
    }),
    ("H729838_HL7", "PATIENTID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PID_F3_C4").isNotNull, concat_ws("",df("PID_F3_C4"),lit("."),df("PID_F3_C1")))
        .otherwise(df("PID_F3_C1")))
    }),
    ("H772763_HL7", "PATIENTID") -> mapFrom("PID_F2_C1"),
    ("H908583_HL7", "PATIENTID") -> cascadeFrom(Seq("PID_F3_C1","PV1_F4_C1")),
    ("H984216_HL7_CN_WIAPP", "PATIENTID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(length(df("PID_F3_C1")) === "8", concat_ws("",df("MSH_F4_C1"),lit("."),df("PID_F2_C1")))
        .otherwise(concat_ws("",df("MSH_F4_C1"),lit("."),df("PID_F3_C1"))))
    }),
    ("H984216_HL7_INVIS_INEVA", "PATIENTID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("",df("PID_F3_C1"),df("PID_F3_C2")))
    }),
    ("H984216_HL7_MS4_FLPEN", "PATIENTID") -> cascadeFrom(Seq("PID_F3_C1", "PID_F2_C1")),
    ("H984216_HL7_SOAR_ININD", "PATIENTID") -> mapFrom("PID_F2_C1"),
    ("H984442_HL7_ATH", "PATIENTID") -> mapFrom("PID_F2_C1"),
    ("H984787_HL7_ATH_QM_WIAPP", "PATIENTID") -> mapFrom("PID_F2_C1"),
    //Encounterid Exceptions
    ("H171267_HL7", "ENCOUNTERID") -> ((col:String,df:DataFrame) => {
      df.withColumn(col, when(df("PV1_F19_C1").isNotNull, concat_ws("",df("PV1_F19_C1"),df("PV1_F2_C1"))).otherwise(null))
    }),
    ("H247885_HL7", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F50_C1"), substring(df("PV1_F2_C1"),1,1)))),
    ("H302436_HL7", "ENCOUNTERID") -> ((col:String,df:DataFrame) => {
      df.withColumn(col, when(df("PV1_F2_C1") === "S", concat_ws("",df("PID_F18_C1"),df("PV1_F2_C1"),df("PV1_F44_C1")))
        .otherwise(concat_ws("",df("PID_F18_C1"),df("PV1_F2_C1"))))
    }),
    ("H302436_HL7_Epic", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F19_C1"), substring(df("PV1_F2_C1"),1,1)))),
    ("H303173_HL7_HPHC", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PID_F18_C1"),df("PV1_F2_C1")))),
    ("H328218_HL7_Mercy", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F19_C1"), substring(df("PV1_F2_C1"),1,1)))),
    ("H328218_HL7_UIHC", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F19_C1"), substring(df("PV1_F2_C1"),1,1)))),
    //todo H416989_HL7 Encounterid
    ("H542284_HL7_ATH", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F19_C1"), substring(df("PV1_F2_C1"),1,1)))),
    ("H641171_HL7_P", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F19_C1"), substring(df("PV1_F2_C1"),1,1)))),
    ("H641171_HL7_PM", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F19_C1"), lit("_"), df("PV1_F2_C1")))),
    ("H704847_HL7_CCD", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat_ws("", coalesce(df("PID_F3_C1"), df("PID_F2_C1")), df("MSH_F4_C1"), df("PV1_F2_C1"), df("PV1_F44_C1")))),
    ("H704847_HL7_OCIE", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat_ws("", coalesce(df("PID_F3_C1"), df("PID_F2_C1")), df("MSH_F4_C1"), df("PV1_F2_C1"), df("PV1_F44_C1")))),
    ("H770635_HL7_NG", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F50_C1"), substring(df("PV1_F2_C1"),1,1)))),
    ("H984216_HL7_CERNER_WIMIL", "ENCOUNTERID") -> ((col:String,df:DataFrame) => df.withColumn(col, concat(df("PV1_F19_C1"), substring(df("PV1_F2_C1"),1,1)))),
    ("H984833_HL7_MEDENT", "ENCOUNTERID") -> mapFrom("PV1_F19_C1")
  )

}
