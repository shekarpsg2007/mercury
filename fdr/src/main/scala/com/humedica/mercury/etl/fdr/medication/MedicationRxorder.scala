package com.humedica.mercury.etl.fdr.medication

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame


class MedicationRxorder (config: Map[String, String]) extends EntitySource(config: Map[String, String]){

  tables=List("Rxordersandprescriptions:"+config("EMR")+"@Rxordersandprescriptions", "mpitemp:fdr.mpi.MpiPatient")

  columns=List("CLIENT_DS_ID", "MPI", "MEDICATION_DTM", "MEDICATION_NAME", "SIGNATURE", "NDC", "DCC", "RXORDER_IND", "RXADMIN_IND", "PAT_REPORTED_IND")


  join = (dfs: Map[String, DataFrame]) => {
    dfs("Rxordersandprescriptions")
      .join(dfs("mpitemp"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
  }


  map = Map(
    "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
    "MPI" -> mapFrom("MPI"),
    "MEDICATION_DTM" -> mapFrom("ISSUEDATE"),
    "MEDICATION_NAME" -> cascadeFrom(Seq("LOCALDESCRIPTION", "LOCALGENERICDESC")),
    "SIGNATURE" -> mapFrom("SIGNATURE"),
    "NDC" -> mapFrom("MAPPEDNDC"),
    "DCC" -> mapFrom("DCC"),
    "RXORDER_IND" -> literal("1"),
    "RXADMIN_IND" -> literal("0"),
    "PAT_REPORTED_IND" -> literal("0")
  )
}
