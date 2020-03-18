package com.humedica.mercury.etl.hl7_v2.diagnosis

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/26/17.
  */
class DiagnosisDg1(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_dg1_a"
    //,"hl7_segment_evn_a" //only for H319 and H101
    //,"hl7_segment_ft1_a" //only for H319 and H101
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PV1_F44_C1","MESSAGEID", "ENCOUNTERID","LASTUPDATEDATE"),
    "hl7_segment_dg1_a" -> List("DG1_F5_C1","DG1_F3_C1","DG1_F3_C3","DG1_F6_C1","DG1_F16_C1","DG1_F15_C1","DG1_F5_C2","DG1_F2_C1","MESSAGEID")
  )

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("PV1_F44_C1"), lit("yyyymmddhh24mi")))
      len.withColumn("PV1_F44_C1_Date",
        when(length(len("PV1_F44_C1")).lt(len("lenAddm")), expr("rpad(PV1_F44_C1, lenAddm, '0')"))
          .when(length(len("PV1_F44_C1")).gt(len("lenAddm")), expr("substr(PV1_F44_C1, 0, lenAddm)"))
          .otherwise(len("PV1_F44_C1")))
    }),
    "hl7_segment_dg1_a" -> ((df: DataFrame) => {
      val len = df.withColumn("lenAddm", datelengthcheck(df("DG1_F5_C1"), lit("yyyymmddhh24mi")))
      val df1 = len.withColumn("DG1_F5_C1_Date",
        when(length(len("DG1_F5_C1")).lt(len("lenAddm")), expr("rpad(DG1_F5_C1, lenAddm, '0')"))
          .when(length(len("DG1_F5_C1")).gt(len("lenAddm")), expr("substr(DG1_F5_C1, 0, lenAddm)"))
          .otherwise(len("DG1_F5_C1")))
      val len2 = df1.withColumn("lenAddm", datelengthcheck(df1("DG1_F5_C2"), lit("yyyymmddhh24mi")))
      len2.withColumn("DG1_F5_C2_Date",
        when(length(len2("DG1_F5_C2")).lt(len2("lenAddm")), expr("rpad(DG1_F5_C2, lenAddm, '0')"))
          .when(length(len2("DG1_F5_C2")).gt(len2("lenAddm")), expr("substr(DG1_F5_C2, 0, lenAddm)"))
          .otherwise(len2("DG1_F5_C2")))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("hl7_segment_dg1_a"), Seq("MESSAGEID"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_dg1_a"),
    "DX_TIMESTAMP" -> cascadeFrom(Seq("DG1_F5_C1_Date","PV1_F44_C1_Date")),
    "LOCALDIAGNOSIS" -> mapFrom("DG1_F3_C1"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "PRIMARYDIAGNOSIS" ->  ((col:String,df:DataFrame) => df.withColumn(col, when(df("DG1_F15_C1") === "1", "1").otherwise("0"))),
    "LOCALACTIVEIND" -> mapFrom("DG1_F6_C1"),
    "LOCALADMITFLG" -> ((col:String,df:DataFrame) => df.withColumn(col, when(df("DG1_F6_C1").isin("Admitting Dx", "A"), df("DG1_F6_C1")).otherwise(null))),
    "LOCALDIAGNOSISSTATUS" -> mapFrom("DG1_F6_C1"),
    "LOCALDISCHARGEFLG" -> ((col:String,df:DataFrame) => df.withColumn(col, when(df("DG1_F6_C1").isin("Discharge Dx", "D"), df("DG1_F6_C1")).otherwise(null))),
    "LOCALDIAGNOSISPROVIDERID" -> mapFrom("DG1_F16_C1"),
    "RESOLUTIONDATE" -> mapFrom("DG1_F5_C2_Date"),
    "MAPPEDDIAGNOSIS" -> mapFrom("DG1_F3_C1"),
    "CODETYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
         when(df("DG1_F2_C1") like "I%10%", lit("ICD10"))
        .when(df("DG1_F2_C1") like "I%9%", lit("ICD9"))
        .when(upper(df("DG1_F2_C1")) like "%SNOMED%", lit("SNOMED"))
        .otherwise(null))
    })
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("DX_TIMESTAMP"), df("LOCALDIAGNOSIS"), df("LOCALDIAGNOSISSTATUS")).orderBy(df("PRIMARYDIAGNOSIS").desc,df("LASTUPDATEDATE").desc,df("MESSAGEID").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1 and PATIENTID is not null AND DX_TIMESTAMP is not null AND LOCALDIAGNOSIS IS NOT NULL").drop("rn")
  }

  afterMapExceptions = Map(
    ("H704847_HL7_CCD", (df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALDIAGNOSIS")).orderBy(df("DX_TIMESTAMP").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 and PATIENTID is not null AND DX_TIMESTAMP is not null AND LOCALDIAGNOSIS IS NOT NULL").drop("rn")
    }),
    ("H704847_HL7_OCIE", (df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALDIAGNOSIS")).orderBy(df("DX_TIMESTAMP").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 and PATIENTID is not null AND DX_TIMESTAMP is not null AND LOCALDIAGNOSIS IS NOT NULL").drop("rn")
    })
  )

  mapExceptions = Map(
    ("H704847_HL7_CCD", "LOCALADMITFLG") -> nullValue(),
    ("H704847_HL7_CCD", "LOCALDISCHARGEFLG") -> nullValue(),
    ("H704847_HL7_CCD", "LOCALDIAGNOSISPROVIDERID") -> nullValue(),
    ("H704847_HL7_CCD", "PRIMARYDIAGNOSIS") -> nullValue(),
    ("H704847_HL7_CCD", "CODETYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DG1_F3_C3") like "I%10%", lit("ICD10"))
        .when(df("DG1_F3_C3") like "I%9%", lit("ICD9"))
        .otherwise(null))
    }),
    ("H704847_HL7_CCD", "LOCALACTIVEIND") -> nullValue(),
    ("H704847_HL7_OCIE", "LOCALADMITFLG") -> nullValue(),
    ("H704847_HL7_OCIE", "LOCALDISCHARGEFLG") -> nullValue(),
    ("H704847_HL7_OCIE", "LOCALDIAGNOSISPROVIDERID") -> nullValue(),
    ("H704847_HL7_OCIE", "PRIMARYDIAGNOSIS") -> nullValue(),
    ("H704847_HL7_OCIE", "CODETYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DG1_F3_C3") like "I%10%", lit("ICD10"))
        .when(df("DG1_F3_C3") like "I%9%", lit("ICD9"))
        .otherwise(null))
    }),
    ("H704847_HL7_OCIE", "LOCALACTIVEIND") -> nullValue(),
    ("H704847_HL7_ELYS", "DATASRC") -> literal("hl7_problems"),
    ("H328218_HL7_UIHC", "LOCALACTIVEIND") -> mapFrom("DG1_F6_C1", prefix = config(CLIENT_DS_ID) + "."),
    ("H328218_HL7_Genesis", "LOCALACTIVEIND") -> mapFrom("DG1_F6_C1", prefix = config(CLIENT_DS_ID) + "."),
    ("H328218_HL7_Genesis","LOCALADMITFLG") -> ((col:String,df:DataFrame) => df.withColumn(col, when(df("DG1_F6_C1").isin("Admitting Dx", "A"), concat_ws("",lit("7135."),df("DG1_F6_C1")))
        .otherwise(null))),
    ("H328218_HL7_Genesis","LOCALDISCHARGEFLG") -> ((col:String,df:DataFrame) => df.withColumn(col, when(df("DG1_F6_C1").isin("Admitting Dx", "D"), concat_ws("",lit("7135."),df("DG1_F6_C1")))
      .otherwise(null))),
    ("H328218_HL7_Genesis", "LOCALDIAGNOSISSTATUS") -> mapFrom("DG1_F6_C1", prefix = config(CLIENT_DS_ID) + "."),
    ("H328218_HL7_UIHC","LOCALADMITFLG") -> ((col:String,df:DataFrame) => df.withColumn(col, when(df("DG1_F6_C1").isin("Admitting Dx", "A"), concat_ws("",lit("7135."),df("DG1_F6_C1")))
      .otherwise(null))),
    ("H328218_HL7_UIHC","LOCALDISCHARGEFLG") -> ((col:String,df:DataFrame) => df.withColumn(col, when(df("DG1_F6_C1").isin("Admitting Dx", "D"), concat_ws("",lit("7135."),df("DG1_F6_C1")))
      .otherwise(null))),
    ("H328218_HL7_UIHC", "LOCALDIAGNOSISSTATUS") -> mapFrom("DG1_F6_C1", prefix = config(CLIENT_DS_ID) + ".")
    /* //TODO
    ("H101623_HL7", "DX_TIMESTAMP")
    ("H101623_HL7", "LOCALDIAGNOSIS")
    */
  )

}
