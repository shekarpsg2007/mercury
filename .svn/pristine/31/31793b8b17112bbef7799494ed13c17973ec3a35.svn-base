package com.humedica.mercury.etl.hl7_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 7/21/17.
  */
class ObservationObx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_obx_a"
    ,"cdr.zcm_obstype_code"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","MESSAGEID","LASTUPDATEDATE","MSH_F9_C1"),
    "hl7_segment_obx_a" -> List("OBX_F14_C1", "OBX_F3_C1", "OBX_F3_C2", "OBX_F3_C1", "OBX_F14_C1"
     ,"MESSAGEID", "IDX", "OBX_RESULTDATA", "OBX_F11_C1"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "LOCALUNIT", "GROUPID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("hl7_segment_obx_a"), Seq("MESSAGEID"), "left_outer")
      .join(dfs("cdr.zcm_obstype_code"), dfs("hl7_segment_obx_a")("OBX_F3_C2") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_obx_a"),
    /*"OBSDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, unix_timestamp(df("OBX_F14_C1"), "yyyyMMddHHmmss").cast("timestamp"))
    }),*/
    "OBSDATE" -> mapFrom("OBX_F14_C1"),
    "LOCALCODE" -> mapFrom("OBX_F3_C2"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "LOCALRESULT" -> ((col:String,df:DataFrame) => df.withColumn(col, substring(df("OBX_RESULTDATA"),1,255))),
    "STATUSCODE" -> mapFrom("OBX_F11_C1"),
    "OBSTYPE" -> mapFrom("OBSTYPE")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"),df("OBSDATE"),df("OBSTYPE")).orderBy(df("STATUSCODE").desc,df("LOCAL_OBS_UNIT").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("PATIENTID IS NOT NULL AND LOCALCODE IS NOT NULL AND OBSDATE IS NOT NULL AND OBSTYPE IS NOT NULL and rn = 1")
  }

  /*
  mapExceptions = Map(
    ("H302436_HL7","LOCALCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("OBX_F6_C1").isNull, df("OBX_F3_C4"))
        .otherwise(concat_ws("",df("OBX_F3_C4"),lit("_"),df("OBX_F6_C1"))))
    }),
    ("H641171_HL7_P","LOCALCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("OBX_F6_C1").isNull, df("OBX_F3_C1"))
        .otherwise(concat_ws("",df("OBX_F3_C1"),lit("_"),df("OBX_F6_C1"))))
    }),
    ("H641171_HL7_PM","LOCALCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("", coalesce(df("OBX_F3_C2"),df("OBX_F3_C4"))
        ,when(df("OBX_F6_C1").isNotNull,"_").otherwise(null)
        ,df("OBX_F6_C1")))
    }),
    ("H984216_HL7_CERNER_TNNAS","LOCALCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("", coalesce(df("OBX_F3_C2"),df("OBX_F3_C1"))
        ,when(df("OBX_F6_C1").isNotNull,"_").otherwise(null)
        ,df("OBX_F6_C1")))
    }),
    ("H984216_HL7_CN_WIAPP","LOCALCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("", coalesce(df("OBX_F3_C2"),df("OBX_F3_C4"))
        ,when(df("OBX_F6_C1").isNotNull,"_").otherwise(null)
        ,df("OBX_F6_C1")))
    }),
    ("H984216_HL7_CERNER_TNNAS","OBSDATE") -> ((col:String,df:DataFrame) => {
      df.withColumn(col,
        coalesce(unix_timestamp(substring(df("OBX_F14_C1"),1,14), "yyyyMMddHHmmss").cast("timestamp"),
          df("PV1_F44_C1")))
    }),
    ("H984216_HL7_CN_WIAPP","OBSDATE") -> ((col:String,df:DataFrame) => {
      df.withColumn(col,
        coalesce(unix_timestamp(substring(df("OBX_F14_C1"),1,14), "yyyyMMddHHmmss").cast("timestamp"),
          df("PV1_F44_C1")))
    })
  )
*/
}
