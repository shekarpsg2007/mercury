package com.humedica.mercury.etl.hl7_v2.labresult

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{unix_timestamp, _}
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/31/17.
  */
class LabresultObx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_obr_a"
    ,"hl7_segment_obx_a"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F3_C1","MSH_F7_C1","MSH_F9_C1","MSH_F11_C1","PV1_F44_C1","MESSAGEID","LASTUPDATEDATE","FILE_ID"),
    "hl7_segment_obr_a" -> List("OBR_F2_C1", "OBR_F2_C1", "OBR_F15_C1", "OBR_F4_C2", "OBR_F7_C1","OBR_F14_C1","OBR_F3_C2",
      "OBR_F6_C1", "OBR_F7_C1", "OBR_F3_C1","OBR_F18_C1","MESSAGEID"),
    "hl7_segment_obx_a" -> List("OBX_F14_C1", "OBX_F3_C1", "OBX_F3_C2", "OBX_F3_C4", "MESSAGEID", "IDX",
      "OBX_F6_C1", "OBX_F7_C1", "OBX_F8_C1", "OBX_F11_C1", "OBX_RESULTDATA")
  )

  beforeJoin = Map(
    "hl7_segment_obr_a" -> ((df: DataFrame) => {
      val obr_f6_c1_len = df.withColumn("lenAddm", datelengthcheck(df("OBR_F6_C1"), lit("yyyymmddhh24miss")))
      val df1 = obr_f6_c1_len.withColumn("OBR_F6_C1_NEW",
        when(length(obr_f6_c1_len("OBR_F6_C1")).lt(obr_f6_c1_len("lenAddm")), from_unixtime(unix_timestamp(expr("rpad(OBR_F6_C1, lenAddm, '0')"), "yyyyMMddHHmmss")))
          .when(length(obr_f6_c1_len("OBR_F6_C1")).gt(obr_f6_c1_len("lenAddm")), from_unixtime(unix_timestamp(expr("substr(OBR_F6_C1, 0, lenAddm)"), "yyyyMMddHHmmss")))
          .otherwise(from_unixtime(unix_timestamp(obr_f6_c1_len("OBR_F6_C1"), "yyyyMMddHHmmss"))))
      val obr_f7_c1_len = df1.withColumn("lenAddm", datelengthcheck(df1("OBR_F7_C1"), lit("yyyymmddhh24miss")))
      val df2 = obr_f7_c1_len.withColumn("OBR_F7_C1_NEW",
        when(length(obr_f7_c1_len("OBR_F7_C1")).lt(obr_f7_c1_len("lenAddm")), from_unixtime(unix_timestamp(expr("rpad(OBR_F7_C1, lenAddm, '0')"), "yyyyMMddHHmmss")))
          .when(length(obr_f7_c1_len("OBR_F7_C1")).gt(obr_f7_c1_len("lenAddm")), from_unixtime(unix_timestamp(expr("substr(OBR_F7_C1, 0, lenAddm)"), "yyyyMMddHHmmss")))
          .otherwise(from_unixtime(unix_timestamp(obr_f7_c1_len("OBR_F7_C1"), "yyyyMMddHHmmss"))))
      val obr_f14_c1_len = df2.withColumn("lenAddm", datelengthcheck(df2("OBR_F14_C1"), lit("yyyymmddhh24miss")))
      obr_f14_c1_len.withColumn("OBR_F14_C1_NEW",
        when(length(obr_f14_c1_len("OBR_F14_C1")).lt(obr_f14_c1_len("lenAddm")), from_unixtime(unix_timestamp(expr("rpad(OBR_F14_C1, lenAddm, '0')"), "yyyyMMddHHmmss")))
          .when(length(obr_f14_c1_len("OBR_F14_C1")).gt(obr_f14_c1_len("lenAddm")), from_unixtime(unix_timestamp(expr("substr(OBR_F14_C1, 0, lenAddm)"), "yyyyMMddHHmmss")))
          .otherwise(from_unixtime(unix_timestamp(obr_f14_c1_len("OBR_F14_C1"), "yyyyMMddHHmmss"))))
    }),
    "hl7_segment_obx_a" -> ((df: DataFrame) => {
      val obx_f14_c1_len = df.withColumn("lenAddm", datelengthcheck(df("OBX_F14_C1"), lit("yyyymmddhh24miss")))
      obx_f14_c1_len.withColumn("OBX_F14_C1_NEW",
        when(length(obx_f14_c1_len("OBX_F14_C1")).lt(obx_f14_c1_len("lenAddm")), from_unixtime(unix_timestamp(expr("rpad(OBX_F14_C1, lenAddm, '0')"), "yyyyMMddHHmmss")))
          .when(length(obx_f14_c1_len("OBX_F14_C1")).gt(obx_f14_c1_len("lenAddm")), from_unixtime(unix_timestamp(expr("substr(OBX_F14_C1, 0, lenAddm)"), "yyyyMMddHHmmss")))
            .otherwise(from_unixtime(unix_timestamp(obx_f14_c1_len("OBX_F14_C1"), "yyyyMMddHHmmss"))))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
    dfs("temptable")
      .join(dfs("hl7_segment_obr_a"), Seq("MESSAGEID"), "left_outer")
      .join(dfs("hl7_segment_obx_a"), Seq("MESSAGEID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val addColumn = df.withColumn("LOCALRESULT_25", when(!df("OBX_RESULTDATA").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
      when(locate(" ", df("OBX_RESULTDATA"), 25) === 0, expr("substr(OBX_RESULTDATA,1,length(OBX_RESULTDATA))"))
        .otherwise(expr("substr(OBX_RESULTDATA,1,locate(' ', OBX_RESULTDATA, 25))"))).otherwise(null))
      .withColumn("LOCALRESULT_NUMERIC", when(df("OBX_RESULTDATA").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), df("OBX_RESULTDATA")).otherwise(null))
    addColumn.filter("PATIENTID IS NOT NULL")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_obx_a"),
    "LOCALCODE" -> cascadeFrom(Seq("OBX_F3_C1", "OBX_F3_F2")),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "LOCALNAME" -> mapFrom("OBX_F3_C2"),
    "LOCALUNITS" -> mapFrom("OBX_F6_C1"),
    "NORMALRANGE" -> mapFrom("OBX_F7_C1"),
    "DATEAVAILABLE" -> mapFrom("OBX_F14_C1_NEW"),
    "LOCALRESULT" -> ((col:String,df:DataFrame) => df.withColumn(col, substring(df("OBX_RESULTDATA"),1,100))),
    "LOCAL_LOINC_CODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("OBX_F3_C3") like "%LONIC%", df("OBX_F3_C1"))
        .otherwise(null))
    }),
    "LOCALSPECIMENTYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("OBR_F15_C1") like "%&%&%", split(df("OBR_F15_C1"), "&")(1))
          .otherwise(split(df("OBR_F15_C1"), "&")(0)))
    }),  //when obr.obr_f15_c1 like '%&%&%' then take substring between first '&' and second '&', else take everything up to the first '&'
    "LOCALTESTNAME" -> mapFrom("OBR_F4_C2"),
    "STATUSCODE" -> mapFrom("OBX_F11_C1"),
    "DATECOLLECTED" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, coalesce(df("OBR_F7_C1_NEW"), df("OBR_F6_C1_NEW"), df("OBR_F14_C1_NEW")))
    }),
    "LABORDEREDDATE" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, coalesce(df("OBR_F6_C1_NEW"),df("OBR_F7_C1_NEW")))
    }),
    "LABORDERID" -> mapFrom("OBR_F3_C1"),
    "RESULTSTATUS" -> mapFrom("OBX_F11_C1"),
    "LABRESULT_DATE" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, coalesce(df("OBX_F14_C1_NEW"),df("OBR_F7_C1_NEW"),df("OBR_F6_C1_NEW"),df("OBR_F14_C1_NEW")))
    }),
    "LOCALUNITS_INFERRED" -> labresults_extract_uom(),
    "LOCALRESULT_INFERRED" -> extract_value(),
    "RELATIVEINDICATOR" -> labresults_extract_relativeindicator(),
    "RESULTTYPE" -> labresults_extract_resulttype("OBX_RESULTDATA","OBX_F8_C1")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"),df("LOCALNAME"),df("LOCALUNITS"),df("NORMALRANGE"),df("DATEAVAILABLE")
      ,df("LOCALRESULT"),df("LOCAL_LOINC_CODE"),df("LOCALSPECIMENTYPE"),df("LOCALTESTNAME"),df("STATUSCODE"),df("DATECOLLECTED")
      ,df("LABORDEREDDATE"),df("LABORDERID"),df("RESULTSTATUS"),df("LABRESULT_DATE")).orderBy(df("MESSAGEID").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("LABRESULTID IS NOT NULL AND LOCALCODE IS NOT NULL and LABRESULT_DATE is not null and rn = 1")
  }

  afterMapExceptions = Map(
    ("H704847_HL7_CCD", (df: DataFrame) => {
      val groups = Window.partitionBy(df("LABORDERID"), df("ENCOUNTERID"),df("PATIENTID"),df("DATECOLLECTED")
        ,df("DATEAVAILABLE"),df("RESULTSTATUS"),df("LOCALRESULT"), df("LOCALCODE"), df("LOCALNAME"), df("NORMALRANGE")
        ,df("LOCALUNITS"), df("STATUSCODE"), df("LABORDEREDDATE"), df("LOCALSPECIMENTYPE"), df("LOCAL_LOINC_CODE")).orderBy(df("MESSAGEID").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val fil = addColumn.filter("OBR_F3_C2 = 'LAB' AND rn = 1")
      val labseq = Window.partitionBy(fil("MESSAGEID")).orderBy(fil("LABRESULT_DATE").desc)
      val addColumn2 = fil.withColumn("rn1", row_number.over(labseq))
      val lab = addColumn2.withColumn("LABRESULTID", concat_ws("", addColumn2("rn1"), lit("."), addColumn2("MESSAGEID")))
      lab.filter("PATIENTID IS NOT NULL AND LABRESULTID IS NOT NULL AND LOCALCODE IS NOT NULL and LABRESULT_DATE is not null")
    }),
    ("H704847_HL7_OCIE", (df: DataFrame) => {
      val groups = Window.partitionBy(df("LABORDERID"), df("ENCOUNTERID"),df("PATIENTID"),df("DATECOLLECTED")
        ,df("DATEAVAILABLE"),df("RESULTSTATUS"),df("LOCALRESULT"), df("LOCALCODE"), df("LOCALNAME"), df("NORMALRANGE")
        ,df("LOCALUNITS"), df("STATUSCODE"), df("LABORDEREDDATE"), df("LOCALSPECIMENTYPE"), df("LOCAL_LOINC_CODE")).orderBy(df("MESSAGEID").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val fil = addColumn.filter("OBR_F18_C1 = 'LAB' AND rn = 1")
      val labseq = Window.partitionBy(fil("MESSAGEID")).orderBy(fil("LABRESULT_DATE").desc)
      val addColumn2 = fil.withColumn("rn1", row_number.over(labseq))
      val lab = addColumn2.withColumn("LABRESULTID", concat_ws("", addColumn2("rn1"), lit("."), addColumn2("MESSAGEID")))
      lab.filter("PATIENTID IS NOT NULL AND LABRESULTID IS NOT NULL AND LOCALCODE IS NOT NULL and LABRESULT_DATE is not null")
    })
  )

  mapExceptions = Map(
    ("H704847_HL7_CCD", "LOCALCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(substring(df("OBX_F3_C1"), 1, 4) === "LOI-", df("OBX_F3_C1"))
        .otherwise(df("OBX_F3_C2")))
    }),
    ("H704847_HL7_OCIE", "LOCALCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(substring(df("OBX_F3_C1"), 1, 4) === "LOI-", df("OBX_F3_C1"))
        .otherwise(df("OBX_F3_C2")))
    }),
    ("H704847_HL7_CCD", "LOCAL_LOINC_CODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(substring(df("OBX_F3_C1"), 1, 4) === "LOI-", regexp_replace(substring(df("OBX_F3_C1"),5,999),"^0+(?!$)", ""))
        .otherwise(null))
    }),
    ("H704847_HL7_OCIE", "LOCAL_LOINC_CODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(substring(df("OBX_F3_C1"), 1, 4) === "LOI-", regexp_replace(substring(df("OBX_F3_C1"),5,999),"^0+(?!$)", ""))
        .otherwise(null))
    }),
    ("H984833_HL7_MEDENT", "LABRESULTID") -> mapFrom("OBX_F3_C4"),
    ("H302436_HL7", "DATEAVAILABLE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(unix_timestamp(substring(df("OBX_F14_C1"),1,14), "yyyyMMddHHmmss").cast("timestamp"),
        unix_timestamp(substring(df("OBR_F7_C1"),1,14), "yyyyMMddHHmmss").cast("timestamp")))
    })
  )

}
