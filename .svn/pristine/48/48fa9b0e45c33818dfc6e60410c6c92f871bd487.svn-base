package com.humedica.mercury.etl.hl7_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 7/25/17.
  */
class ProcedureRxa(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_rxa_a")

  columnSelect = Map(
    "temptable" -> List("PATIENTID","MESSAGEID","LASTUPDATEDATE"),
    "hl7_segment_rxa_a" -> List("RXA_F3_C1","RXA_F5_C2","RXA_F5_C3","RXA_F5_C4","MESSAGEID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_rxa_a")
      .join(dfs("temptable"), Seq("MESSAGEID"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_rxa_a"),
    "LOCALCODE" -> mapFrom("RXA_F5_C2"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    //"PROCEDUREDATE" -> mapFromDate("RXA_F3_C1","YYYYMMddHH24mmss"),
    "PROCEDUREDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, unix_timestamp(substring(df("RXA_F3_C1"),1,12), "yyyyMMddHHmm").cast("timestamp"))
    }),
    "LOCALNAME" -> mapFrom("RXA_F5_C4"),
    "MAPPEDCODE" -> mapFrom("RXA_F5_C2"),
    "CODETYPE" -> mapFrom("RXA_F5_C3")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND PROCEDUREDATE IS NOT NULL AND LOCALCODE IS NOT NULL")
    val groups = Window.partitionBy(fil("PATIENTID"), fil("PROCEDUREDATE"), fil("LOCALCODE")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

}
