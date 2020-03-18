package com.humedica.mercury.etl.fdr.visit


import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 02/01/2017
 */


class VisitVisits(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinicalencounter:"+config("EMR")+"@Clinicalencounter", "cdr.map_patient_type",
    "mpitemp:fdr.mpi.MpiPatient")


  columns = List("CLIENT_DS_ID", "MPI", "VISIT_ID", "VISIT_TYPE", "VISIT_START_DTM", "VISIT_END_DTM")


  beforeJoin = Map(
    "cdr.map_patient_type" -> includeIf("GROUPID = '"+config(GROUP)+"'")

  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinicalencounter")
      .join(dfs("mpitemp"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
      .join(dfs("cdr.map_patient_type"),dfs("clinicalencounter")("LOCALPATIENTTYPE") === dfs("cdr.map_patient_type")("LOCAL_CODE"), "left_outer")
  }


  map = Map(
    "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
    "MPI" -> mapFrom("MPI"),
    "VISIT_ID" -> mapFrom("ENCOUNTERID"),
    "VISIT_TYPE" -> mapFrom("CUI"),
    "VISIT_START_DTM" -> mapFrom("ARRIVALTIME"),
    "VISIT_END_DTM" -> mapFrom("DISCHARGETIME")
  )


}