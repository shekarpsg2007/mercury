package com.humedica.mercury.etl.hl7_v2.immunization

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/30/17.
  * Only for CCD
  */
class ImmunizationRxa(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_rxa_a"
  )

  columnSelect = Map(
    "temptable" -> List("PATIENTID","PID_F3_C1","MSH_F7_C1","MSH_F9_C1","MESSAGEID","LASTUPDATEDATE","MESSAGEID", "PID_F18_C1"),
    "hl7_segment_rxa_a" -> List("RXA_F3_C1","RXA_F5_C1","RXA_F5_C4","RXA_F9_C2","RXA_F21_C1","MESSAGEID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_rxa_a")
      .join(dfs("temptable"), Seq("MESSAGEID"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_rxa_a"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "LOCALDEFERREDREASON" -> mapFrom("RXA_F21_C1"),
    "LOCALGPI" -> mapFrom("RXA_F5_C1"),
    "LOCALROUTE" -> mapFrom("RXA_F9_C2"),
    "ADMINDATE" -> mapFromDate("RXA_F3_C1","YYYYMMddHH24mmss"),
    "LOCALIMMUNIZATIONCD" -> mapFrom("RXA_F5_C1"),
    "LOCALIMMUNIZATIONDESC" -> mapFrom("RXA_F5_C4")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PID_F18_C1 is not null and MSH_F9_C1 <> 'SIU'")
    val groups = Window.partitionBy(fil("PATIENTID"),fil("LOCALIMMUNIZATIONCD"),fil("ADMINDATE")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

}
