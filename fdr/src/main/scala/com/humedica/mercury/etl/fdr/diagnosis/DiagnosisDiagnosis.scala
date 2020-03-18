package com.humedica.mercury.etl.fdr.diagnosis

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/27/2017
 */


class DiagnosisDiagnosis(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("Backend:fdr.diagnosis.DiagnosisBackend", "cdr.map_prob_list")


  columns = List("CLIENT_DS_ID", "MPI", "DIAG_DT", "CODE_TYPE", "DIAG_CD", "PROBLEM_LIST_IND")



  join = (dfs: Map[String, DataFrame]) => {
    dfs("Backend")
      .join(dfs("cdr.map_prob_list"), Seq("GROUPID", "DATASRC"), "left_outer")

  }

  map = Map(
    "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
    "MPI" -> mapFrom("GRP_MPI"),
    "DIAG_DT" -> mapFrom("DX_TIMESTAMP"),
    "CODE_TYPE" -> mapFrom("CODETYPE"),
    "DIAG_CD" -> mapFrom("MAPPEDDIAGNOSIS"),
    "PROBLEM_LIST_IND" -> ((col: String, df: DataFrame) => df.withColumn(col, when(isnull(df("IS_PROBLEMLIST")), lit("0")).otherwise(lit("1"))))
  )


  afterMap = (df: DataFrame) => {
    df.filter("DIAG_CD is not null")
  }


}
