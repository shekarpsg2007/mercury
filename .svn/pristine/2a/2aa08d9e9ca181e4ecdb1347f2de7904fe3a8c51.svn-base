package com.humedica.mercury.etl.fdr.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class ProcedureProcedures(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  tables = List("Backend:fdr.procedure.ProcedureBackend")

  columns=List("CLIENT_DS_ID", "MPI", "PROC_DTM", "CODE_TYPE", "PROC_CD", "PROV_ID")

  map = Map(
    "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
    "MPI" -> mapFrom("GRP_MPI"),
    "PROC_DTM" -> mapFrom("PROCEDUREDATE"),
    "CODE_TYPE" -> mapFrom("CODETYPE"),
    "PROC_CD" -> mapFrom("MAPPEDCODE")
  )


  afterMap = (df: DataFrame) => {
    df.filter("CODE_TYPE is not null and PROC_CD is not null")
  }

}