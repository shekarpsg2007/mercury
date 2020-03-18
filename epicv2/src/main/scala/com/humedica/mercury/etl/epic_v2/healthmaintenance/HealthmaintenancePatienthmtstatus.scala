package com.humedica.mercury.etl.epic_v2.healthmaintenance

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class HealthmaintenancePatienthmtstatus(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("PT_HMT_STATUS",
    "HM_HISTORY",
    "CDR.MAP_SCREENING_TESTS",
    "CDR.MAP_PREDICATE_VALUES")

  columnSelect = Map(
    "PT_HMT_STATUS" -> List("PAT_ID","QUALIFIED_HMT_ID","HMT_LAST_UPDATE_DT","HMT_DUE_STATUS_C","IDEAL_RETURN_DT"),
    "HM_HISTORY" -> List("PAT_ID","HM_TOPIC_ID","HM_HX_DATE","HM_TYPE_C")
  )

  beforeJoin = Map(
    "CDR.MAP_SCREENING_TESTS" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").drop("GROUPID")
    }),
    "PT_HMT_STATUS" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PAT_ID"), df("QUALIFIED_HMT_ID")).orderBy(df("HMT_LAST_UPDATE_DT").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 and HMT_DUE_STATUS_C in ('2','3','4')")
    }),
    "HM_HISTORY" -> ((df: DataFrame) => {
      val list_hm_type_c = mpvClause(table("CDR.MAP_PREDICATE_VALUES"), config(GROUP), config(CLIENT_DS_ID), "HM_HISTORY", "PROCEDUREDO", "HM_HISTORY", "HM_TYPE_C")
      val fil = df.filter("HM_TYPE_C in (" + list_hm_type_c + ") or 'NO_MPV_MATCHES' in (" + list_hm_type_c + ")")
      val groups = Window.partitionBy(fil("PAT_ID"), fil("HM_TOPIC_ID")).orderBy(fil("HM_HX_DATE").desc_nulls_last)
      val addColumn = fil.withColumn("rnb", row_number.over(groups))
      addColumn.filter("rnb = 1").withColumnRenamed("PAT_ID","PAT_ID_hm")
    })    
  )

  join = (dfs: Map[String, DataFrame]) => {
    val joined = dfs("PT_HMT_STATUS")
      .join(dfs("HM_HISTORY"), dfs("PT_HMT_STATUS")("PAT_ID") === dfs("HM_HISTORY")("PAT_ID_hm") && 
        dfs("PT_HMT_STATUS")("QUALIFIED_HMT_ID") === dfs("HM_HISTORY")("HM_TOPIC_ID"), "outer")
    val addColumn =joined.withColumn("LOCALCODE", concat(lit(config(CLIENT_DS_ID) + "."), 
      coalesce(joined("QUALIFIED_HMT_ID"),joined("HM_TOPIC_ID"))))
    addColumn.join(table("CDR.MAP_SCREENING_TESTS"), addColumn("LOCALCODE") === table("CDR.MAP_SCREENING_TESTS")("LOCAL_CODE"), "inner")
  }
  
  map = Map(
    "DATASRC" -> literal("patient_hmt_status"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "HM_CUI" -> mapFrom("HTS_CODE"),
    "PATIENTID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("PAT_ID"),df("PAT_ID_hm")))
      }),
    "DOCUMENTED_DATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("HMT_LAST_UPDATE_DT"),df("HM_HX_DATE")))
      }),
    "LAST_SATISFIED_DATE" -> mapFrom("HM_HX_DATE"),
    "NEXT_DUE_DATE" -> mapFrom("IDEAL_RETURN_DT")
  )

}

