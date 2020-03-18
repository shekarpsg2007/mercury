package com.humedica.mercury.etl.hl7_v2.clinicalencounter

import com.humedica.mercury.etl.core.engine.Constants.CLIENT_DS_ID
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/26/17.
  */
class ClinicalencounterPv1(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_evn_a"
    // ,"hl7_segment_dg1_a" TODO only for H101
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F3_C1","PV1_F3_C4","PID_F18_C1","MSH_F4_C1","MSH_F7_C1","MSH_F9_C1","MSH_F11_C1","PV1_F44_C1"
      ,"PV1_F2_C1","PV1_F45_C1","PV1_F3_C4","PV1_F14_C1","PV1_F36_C1","PV1_F2_C1","MESSAGEID","LASTUPDATEDATE", "ENCOUNTERID","PV1_F39_C1"),
    "hl7_segment_evn_a" -> List("MESSAGEID","EVN_F2_C1","EVN_F4_C1")
  )

  beforeJoin = Map(
      "temptable" -> ((df: DataFrame) => {
        val fil = df.filter("PATIENTID is not null and ENCOUNTERID is not null and PV1_F44_C1 is not null and MSH_F9_C1 <> 'SIU' and MSH_F11_C1 = 'P'")
        val groups = Window.partitionBy(fil("ENCOUNTERID")).orderBy(fil("LASTUPDATEDATE").desc,fil("MESSAGEID").desc)
        val addColumn = fil.withColumn("rn", row_number.over(groups))
        addColumn.filter("rn = 1").drop("rn")
      })
  )

  beforeJoinExceptions = Map(
    "H704847_HL7_OCIE" -> Map(
      "temptable" -> ((df: DataFrame) => {
        val fil = df.filter("PATIENTID IS NOT NULL and ENCOUNTERID IS NOT NULL and PV1_F44_C1 IS NOT NULL and MSH_F9_C1 <> 'SIU'")
        val groups = Window.partitionBy(fil("ENCOUNTERID")).orderBy(fil("LASTUPDATEDATE").desc,fil("MESSAGEID").desc)
        val addColumn = fil.withColumn("rn", row_number.over(groups))
        addColumn.filter("rn = 1").drop("rn")
      })),
    "H704847_HL7_CCD" -> Map(
      "temptable" -> ((df: DataFrame) => {
        val fil = df.filter("PATIENTID is not null and ENCOUNTERID is not null and PV1_F44_C1 is not null and MSH_F9_C1 <> 'SIU'")
        val groups = Window.partitionBy(fil("ENCOUNTERID")).orderBy(fil("LASTUPDATEDATE").desc,fil("MESSAGEID").desc)
        val addColumn = fil.withColumn("rn", row_number.over(groups))
        addColumn.filter("rn = 1").drop("rn")
    })),
    "H542284_HL7_ATH" -> Map(
      "temptable" -> ((df: DataFrame) => {
        val fil = df.filter("PATIENTID is not null and ENCOUNTERID is not null and PV1_F44_C1 is not null and MSH_F9_C1 <> 'SIU' and PV1_F19_C1 is not null")
        val groups = Window.partitionBy(fil("ENCOUNTERID")).orderBy(fil("LASTUPDATEDATE").desc,fil("MESSAGEID").desc)
        val addColumn = fil.withColumn("rn", row_number.over(groups))
        addColumn.filter("rn = 1").drop("rn")
      }))
  )

  join = noJoin()

  joinExceptions = Map( //TODO
    "H542284_HL7" -> ((dfs: Map[String, DataFrame]) => {
      dfs("temptable")
        .join(dfs("hl7_segment_z"), Seq("MESSAGEID"), "left outer")
    }),
    "H542284_HL7_ATH" -> ((dfs: Map[String, DataFrame]) => {
      dfs("temptable")
        .join(dfs("hl7_segment_evn"), Seq("MESSAGEID"), "left outer")
    }),
    "H846629_HL7" -> ((dfs: Map[String, DataFrame]) => {
      dfs("temptable")
        .join(dfs("hl7_segment_evn"), Seq("MESSAGEID"), "left outer")
    })
  )

  map = Map(
    "DATASRC" -> literal("hl7_segment_pv1_a"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"), // do not have to mention
    "PATIENTID" -> mapFrom("PATIENTID"), // do not have to mention
    "DISCHARGETIME" -> mapFrom("PV1_F45_C1"),
    "ADMITTIME" -> mapFrom("PV1_F44_C1"),
    "LOCALADMITSOURCE" -> mapFrom("PV1_F14_C1"),
    "LOCALDISCHARGEDISPOSITION" -> mapFrom("PV1_F36_C1"),
    "FACILITYID" -> mapFrom("PV1_F3_C4"),
    "ARRIVALTIME" -> mapFrom("PV1_F44_C1"),
    "LOCALPATIENTTYPE" -> mapFrom("PV1_F2_C1"),
    "APRDRG_CD" -> nullValue(),
    "APRDRG_SOI" -> nullValue(),
    "LOCALNDC" -> nullValue(),
    "ELOS" -> nullValue(),
    "LOCALDRG" -> nullValue(),
    "LOCALDRGTYPE" -> nullValue()
  )

/*
  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID is not null and ENCOUNTERID is not null and ARRIVALTIME is not null and MSH_F9_C1 <> 'SIU' and MSH_F11_C1 = 'P'")
    val groups = Window.partitionBy(fil("ENCOUNTERID")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  afterMapExceptions = Map(
    ("H704847_HL7_CCD", (df: DataFrame) => {
      val fil = df.filter("PATIENTID is not null and ENCOUNTERID is not null and ARRIVALTIME is not null and MSH_F9_C1 <> 'SIU'")
      val groups = Window.partitionBy(fil("ENCOUNTERID")).orderBy(fil("LASTUPDATEDATE").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    }),
    ("H704847_HL7_OCIE", (df: DataFrame) => {
      val fil = df.filter("PATIENTID is not null and ENCOUNTERID is not null and ARRIVALTIME is not null and MSH_F9_C1 <> 'SIU'")
      val groups = Window.partitionBy(fil("ENCOUNTERID")).orderBy(fil("LASTUPDATEDATE").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    })
  )
*/

  mapExceptions = Map(
    //("H328218_HL7_Genesis", "ARRIVALTIME") -> cascadeFrom(Seq("PV1_F44_C1", "LASTSUPDATEDATE")),
    //("H328218_HL7_Genesis", "ADMITTIME") -> cascadeFrom(Seq("PV1_F44_C1", "LASTUPDATEDATE")),
    ("H328218_HL7_Genesis", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = config(CLIENT_DS_ID) + "."),
    ("H328218_HL7_Genesis", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = config(CLIENT_DS_ID) + "."),
    ("H328218_HL7_Genesis", "LOCALPATIENTTYPE") -> mapFrom("PV1_F2_C1", prefix = config(CLIENT_DS_ID) + "."),
    ("H704847_HL7_CCD","FACILITYID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PV1_F3_C4").isNotNull, concat(df("MSH_F4_C1"), lit("."), df("PV1_F3_C4")))
        .otherwise(df("MSH_F4_C1")))
    }),
    ("H704847_HL7_OCIE","FACILITYID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PV1_F3_C4").isNotNull, concat(df("MSH_F4_C1"), lit("."), df("PV1_F3_C4")))
        .otherwise(df("MSH_F4_C1")))
    }),
   /* ("H542284_HL7_ATH", "ARRIVALTIME") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, unix_timestamp(df("EVN_F2_C1"), "yyyyMMddHHmmss").cast("timestamp"))
    }),
    ("H542284_HL7_ATH", "ADMITTIME") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, unix_timestamp(df("EVN_F2_C1"), "yyyyMMddHHmmss").cast("timestamp"))
    }),*/
    ("H542284_HL7_ATH", "ARRIVALTIME") -> mapFrom("EVN_F2_C1"),
    ("H542284_HL7_ATH", "ADMITTIME") -> mapFrom("EVN_F2_C1"),
    ("H451171_HL7_MDT", "ARRIVALTIME") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    ("H451171_HL7_MDT", "ADMITTIME") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    ("H451171_HL7_MDT", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H942522_HL7", "ARRIVALTIME") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    ("H942522_HL7", "ADMITTIME") -> cascadeFrom(Seq("PV1_F44_C1","LASTUPDATEDATE")),
    ("H135772_HL7_MDT_STJ", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H247885_HL7", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H302436_HL7", "FACILITYID") -> cascadeFrom(Seq("PV1_F3_C4","PV1_F39_C1")),
    ("H303173_HL7_EL", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "6684."),
    ("H591965_HL7_CFHA", "FACILITYID") -> mapFrom("PV1_F3_C1"),
    ("H770635_HL7_NG", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H770635_HL7_NG", "LOCALPATIENTTYPE") -> mapFrom("PV1_F2_C1", prefix = "7688."),
    ("H984216_HL7_CERNER_FLJAC", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CERNER_FLJAC", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7396."),
    ("H984216_HL7_CERNER_FLJAC", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7396."),
    ("H984216_HL7_CERNER_FLJAC", "LOCALADMITTYPE") -> mapFrom("PV1_F2_C1", prefix = "7396."),
    ("H984216_HL7_CN_WIAPP", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CN_WIAPP", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7399."),
    ("H984216_HL7_CN_WIAPP", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7399."),
    ("H984216_HL7_CN_WIAPP", "LOCALADMITTYPE") -> mapFrom("PV1_F2_C1", prefix = "7399."),
    ("H328218_HL7_GRHS", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7136."),
    ("H328218_HL7_GRHS", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7136."),
    ("H328218_HL7_GRHS", "LOCALPATIENTTYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(upper(df("PV1_F2_C1")) === "INPATIENT", concat_ws("",lit("7136."),df("PV1_F2_C1"),lit("_"),df("PV1_F18_C1"),lit("_"),df("PV1_F10_C1")))
        .otherwise(concat_ws("",lit("7136."),df("PV1_F2_C1"),lit("_"),df("PV1_F18_C1"))))
    }),
    ("H328218_HL7_UIHC", "LOCALADMITSOURCE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PV1_F2_C1").isNotNull && df("PV1_F18_C1").isNotNull, concat_ws("",lit("7134."),df("PV1_F2_C1"),lit("_"),df("PV1_F18_C1")))
          .when(df("PV1_F2_C1").isNotNull && df("PV1_F18_C1").isNull, concat_ws("",lit("7134."),df("PV1_F2_C1")))
          .when(df("PV1_F2_C1").isNull && df("PV1_F18_C1").isNotNull, concat_ws("",lit("7134."),df("PV1_F18_C1")))
        .otherwise(null))
    }),
    ("H328218_HL7_UIHC", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7133."),
    ("H328218_HL7_UIHC", "LOCALPATIENTTYPE") -> mapFrom("PV1_F2_C1", prefix = "7133."),
    ("H328218_HL7_MERCY", "LOCALPATIENTTYPE") -> mapFrom("PV1_F2_C1", prefix = "7133."),
    ("H908583_HL7", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "1481."),
    ("H908583_HL7", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "1481."),
    ("H908583_HL7", "LOCALPATIENTTYPE") -> mapFrom("PV1_F2_C1", prefix = "1481."),
    ("H984216_HL7_CERNER_TNNAS", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7397."),
    ("H984216_HL7_CERNER_TNNAS", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7397."),
    ("H984216_HL7_CERNER_TNNAS", "LOCALPATIENTTYPE") -> mapFrom("PV1_F2_C1", prefix = "7397."),
    ("H984216_HL7_CERNER_WIMIL", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7398."),
    ("H984216_HL7_CERNER_WIMIL", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7398."),
    ("H984216_HL7_CERNER_WIMIL", "LOCALPATIENTTYPE") -> mapFrom("PV1_F2_C1", prefix = "7398."),
    ("H984216_HL7_INVIS_ALMOB", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7401."),
    ("H984216_HL7_INVIS_ALMOB", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7401."),
    ("H984216_HL7_INVIS_ALMOB", "LOCALPATIENTTYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("",lit("7401."),df("PV1_F2_C1"),df("PV1_F18_C1")))
    }),
    ("H984216_HL7_INVIS_INEVA", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7402."),
    ("H984216_HL7_INVIS_INEVA", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7402."),
    ("H984216_HL7_INVIS_INEVA", "LOCALPATIENTTYPE") ->  ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PV1_F2_C1") === "E" && !df("PV1_F18_C1").isin("H","W"), "7402.Emergency")
        .when(df("PV1_F2_C1") === "I" && !df("PV1_F18_C1").isin("H","W"), "7402.Inpatient")
        .when(df("PV1_F18_C1").isin("H","W"), "7402.Observation")
        .otherwise("7402.Other"))
    }),
    ("H984216_HL7_MCK_ALBIR", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7403."),
    ("H984216_HL7_MCK_ALBIR", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7403."),
    ("H984216_HL7_MCK_ALBIR", "LOCALPATIENTTYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("",lit("7403."),df("PV1_F2_C1"),df("PV1_F18_C1")))
    }),
    ("H984216_HL7_MS4_FLPEN", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7404."),
    ("H984216_HL7_MS4_FLPEN", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7404."),
    ("H984216_HL7_SOAR_ININD", "LOCALADMITSOURCE") -> mapFrom("PV1_F14_C1", prefix = "7405."),
    ("H984216_HL7_SOAR_ININD", "LOCALDISCHARGEDISPOSITION") -> mapFrom("PV1_F36_C1", prefix = "7405."),
    ("H984216_HL7_SOAR_ININD", "LOCALPATIENTTYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PV1_F18_C1").isNotNull, concat_ws("",lit("7405."),df("PV1_F2_C1"),lit("_"),df("PV1_F18_C1")))
      .otherwise("PV1_F2_C1"))
    }),
    ("H846629_HL7","LOCALPATIENTTYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("EVN_F4_C1") === "ENC_CREATE", "TELEPHONE").otherwise(df("PV1_F2_C1")))
    }),
    ("H641171_HL7_PM","LOCALPATIENTTYPE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PV1_F18_C1").isNotNull, concat_ws("",df("PV1_F2_C1"),lit("_"),df("PV1_F18_C1")))
        .otherwise("PV1_F2_C1"))
    }),
    ("H303173_HL7_FR","LOCALPATIENTTYPE") -> ((col:String,df:DataFrame) => df.withColumn(col, concat_ws("",df("PV1_F2_C1"),lit("_"),df("PV1_F3_C1")))),
    ("H171267_HL7", "LOCALPATIENTTYPE") -> mapFrom("PV1_F2_C1", prefix = "7708.")
    ////todo H101 and H416989_hl7_55 localpatienttype
  )

}