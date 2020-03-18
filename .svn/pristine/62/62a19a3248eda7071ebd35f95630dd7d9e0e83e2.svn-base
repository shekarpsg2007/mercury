package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilDedupedAllergy (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns=List("ALLERGY_CODE", "ALLERGY_CONCEPT_TYPE", "ALLERGY_ID", "ALLERGY_NAME", "ALLERGY_REACTION_NAME",
    "CHART_ID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "CREATED_BY", "CREATED_DATETIME",
    "DEACTIVATED_BY", "DEACTIVATED_DATETIME", "DELETED_BY", "DELETED_DATETIME", "HUM_TYPE", "NOTE", "ONSET_DATE",
    "PATIENT_ID", "REACTIVATED_BY", "REACTIVATED_DATETIME", "RXNORM_CODE", "FILEID")

  tables = List("allergy",
    "fileExtractDates:athena.util.UtilFileIdDates",
    "pat:athena.util.UtilSplitPatient")

  columnSelect = Map(
    "allergy" -> List("ALLERGY_CODE", "ALLERGY_CONCEPT_TYPE", "ALLERGY_ID", "ALLERGY_NAME", "ALLERGY_REACTION_NAME",
      "CHART_ID", "CONTEXT_ID", "CONTEXT_NAME", "CONTEXT_PARENTCONTEXTID", "CREATED_BY", "CREATED_DATETIME",
      "DEACTIVATED_BY", "DEACTIVATED_DATETIME", "DELETED_BY", "DELETED_DATETIME", "HUM_TYPE", "NOTE", "ONSET_DATE",
      "PATIENT_ID", "REACTIVATED_BY", "REACTIVATED_DATETIME", "RXNORM_CODE", "FILEID"),
    "fileExtractDates" -> List("FILEID","FILEDATE"),
    "pat" -> List("PATIENT_ID")
  )

  beforeJoin = Map(
    "allergy" -> includeIf("hum_type = 'PATIENTALLERGY'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    val patJoinType = new UtilSplitTable(config).patprovJoinType
    dfs("allergy")
      .join(dfs("fileExtractDates"), Seq("FILEID"), "left_outer")
      .join(dfs("pat"), Seq("PATIENT_ID"), patJoinType)
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ALLERGY_ID"), df("HUM_TYPE"))
      .orderBy(df("FILEDATE").desc_nulls_last, df("FILEID").desc_nulls_last)
    df.withColumn("dedupe_row", row_number.over(groups))
      .filter("dedupe_row=1 and deleted_datetime is null and coalesce(patient_id, chart_id) is not null")
  }


}
