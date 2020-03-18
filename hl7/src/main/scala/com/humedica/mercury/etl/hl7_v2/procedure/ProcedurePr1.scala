package com.humedica.mercury.etl.hl7_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants.{CLIENT_DS_ID, GROUP}
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 7/25/17.
  */
class ProcedurePr1(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_pr1_a"
    ,"cdr.map_custom_proc")

  columnSelect = Map(
    "temptable" -> List("PATIENTID","MESSAGEID","LASTUPDATEDATE","ENCOUNTERID","PV1_F44_C1","MSH_F11_C1"),
    "hl7_segment_pr1_a" -> List("PR1_F2_C1","PR1_F3_C1","PR1_F3_C2","PR1_F4_C1","PR1_F5_C1","IDX"
                                ,"PR1_F11_C1","MESSAGEID"),
    "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID","MAPPEDVALUE", "CODETYPE")
  )

  beforeJoin = Map(
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      val fil = df.filter("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'hl7_segment_pr1_a'")
      fil.withColumnRenamed("GROUPID", "GROUPID_mcp").withColumnRenamed("DATASRC", "DATASRC_mcp")
    }),
    "hl7_segment_pr1_a" -> ((df: DataFrame) => {
      df.withColumn("LOCAL_CODE", when(df("PR1_F3_C1") === "\"\"" || df("PR1_F3_C1").isNull, df("PR1_F3_C2")).otherwise(df("PR1_F3_C1")))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_pr1_a")
      .join(dfs("temptable"), Seq("MESSAGEID"), "inner")
      .join(dfs("cdr.map_custom_proc"), concat(lit(config(CLIENT_DS_ID) + "."), dfs("hl7_segment_pr1_a")("LOCAL_CODE")) === dfs("cdr.map_custom_proc")("LOCALCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_pr1_a"),
    "LOCALCODE" -> mapFrom("LOCAL_CODE"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    /*"PROCEDUREDATE" -> ((col:String,df:DataFrame) => {
      df.withColumn(col,
        coalesce(unix_timestamp(substring(df("PR1_F5_C1"),1,8), "yyyyMMdd").cast("timestamp"),
          df("PV1_F44_C1")))
    }),*/
    "PROCEDUREDATE" -> cascadeFrom(Seq("PR1_F5_C1","PV1_F44_C1")),
    "LOCALNAME" -> mapFrom("PR1_F4_C1"),
    "PROSEQ" -> mapFrom("IDX"),
    "MAPPEDCODE" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, when(df("PR1_F3_C1") === "\"\"" || df("PR1_F3_C1").isNull, df("MAPPEDVALUE"))
                        .otherwise(substring(df("PR1_F3_C1"),1,5)))
    }),
    //if PR1_f3_C1 is null or = '""' send map_custom_proc.mappedvalue else  truncate localcode to length of 5 and send that
    "CODETYPE" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, when(df("MAPPEDVALUE").isNotNull, "CUSTOM")
                        .when(df("PR1_F2_C1") like "%SNOMED%","SNOMED")
                        .when(df("PR1_F2_C1") like "%I%9%","ICD9")
                        .when(df("PR1_F2_C1") like "%I%10%","ICD10")
                        .when(df("PR1_F2_C1") like "%CPT%","CPT4")
                        .when(substring(df("PR1_F3_C1"),1,5) rlike "^[0-9]{4}[0-9A-Z]$","CPT4")
                        .when(substring(df("PR1_F3_C1"),1,5) rlike "^[A-Z]{1,1}[0-9]{4}$","HCPCS")
                        .otherwise(null))
    }),
    "PERFORMINGPROVIDERID" -> mapFrom("PR1_F11_C1", nullIf=Seq("\"\""))
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND PROCEDUREDATE IS NOT NULL AND LOCALCODE IS NOT NULL")
    val groups = Window.partitionBy(fil("PATIENTID"), fil("ENCOUNTERID"), fil("PROCEDUREDATE"), fil("LOCALCODE"), fil("PROCSEQ"), fil("PERFORMINGPROVIDERID")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  afterMapExceptions = Map(
    ("H302436_HL7", (df: DataFrame) => {
      val fil = df.filter("PATIENTID IS NOT NULL AND PROCEDUREDATE IS NOT NULL AND LOCALCODE IS NOT NULL AND MSH_F11_C1 = 'P'")
      val groups = Window.partitionBy(fil("PATIENTID"), fil("ENCOUNTERID"), fil("PROCEDUREDATE"), fil("LOCALCODE"), fil("PROCSEQ"), fil("PERFORMINGPROVIDERID")).orderBy(fil("LASTUPDATEDATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    })
  )

  mapExceptions = Map(
    ("H984216_HL7_INVIS_INEVA", "LOCALNAME") -> mapFrom("PR1_F3_C2"),
    ("H302436_HL7", "MAPPEDCODE") -> mapFrom("PR1_F3_C1")
  )
}
