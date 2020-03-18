package com.humedica.mercury.etl.epic_v2.patientreportedmeds

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._


class PatientreportedmedsPatenccurrmeds(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "zh_claritymed",
    "pat_enc_curr_meds",
    "medorders"
  )
        
  columnSelect = Map(
    "zh_claritymed" -> List("NAME", "MEDICATION_ID"),
    "pat_enc_curr_meds" -> List("PAT_ID", "CURRENT_MED_ID", "PAT_ENC_CSN_ID", "MEDICATION_ID", "CONTACT_DATE","UPDATE_DATE","IS_ACTIVE_YN"),
    "medorders" -> List("ORDER_MED_ID","UPDATE_DATE","MEDICATION_ID")
  )

  beforeJoin = Map(
    "pat_enc_curr_meds" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("CURRENT_MED_ID"),df("PAT_ENC_CSN_ID")).orderBy(df("UPDATE_DATE").desc_nulls_last)
      val addColumn = df.withColumn("rownumber", row_number.over(groups))
      addColumn.filter("PAT_ID is not null and rownumber = 1 and IS_ACTIVE_YN = 'Y'")
    }),
    "zh_claritymed" -> ((df: DataFrame) => {
      df.dropDuplicates(Seq("MEDICATION_ID", "NAME")).withColumnRenamed("MEDICATION_ID", "MEDICATION_ID_zh")
    }),
    "medorders" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ORDER_MED_ID")).orderBy(df("UPDATE_DATE").desc_nulls_last)
      val addColumn = df.withColumn("rownumber", row_number.over(groups)).filter("rownumber = 1").drop("rownumber")
      addColumn.withColumnRenamed("MEDICATION_ID","MEDICATION_ID_med")
    })    
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("pat_enc_curr_meds")
      .join(dfs("medorders"), dfs("pat_enc_curr_meds")("CURRENT_MED_ID") === dfs("medorders")("ORDER_MED_ID"), "left_outer")
      .join(dfs("zh_claritymed"), coalesce(dfs("pat_enc_curr_meds")("MEDICATION_ID"),dfs("medorders")("MEDICATION_ID_med"))
        === dfs("zh_claritymed")("MEDICATION_ID_zh") ,"left_outer")
  }

  map = Map(
    "DATASRC" -> literal("pat_enc_curr_meds"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "REPORTEDMEDID" -> mapFrom("CURRENT_MED_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALDRUGDESCRIPTION" -> mapFrom("NAME"),
    "LOCALMEDCODE" -> cascadeFrom(Seq("MEDICATION_ID","MEDICATION_ID_med")),
    "MEDREPORTEDTIME" -> mapFrom("CONTACT_DATE")
  )

  afterMap = (df: DataFrame) => {
    df.filter("LOCALMEDCODE is not null")
  }
}