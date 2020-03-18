package com.humedica.mercury.etl.fdr.medication

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame


class MedicationRxpatientreported (config: Map[String, String]) extends EntitySource(config: Map[String, String]){

  tables=List("Patientreportedmeds:"+config("EMR")+"@Patientreportedmeds", "mpitemp:fdr.mpi.MpiPatient")

  columns=List("CLIENT_DS_ID", "MPI", "MEDICATION_DTM", "MEDICATION_NAME", "SIGNATURE", "NDC", "DCC", "RXORDER_IND", "RXADMIN_IND", "PAT_REPORTED_IND")

  join = (dfs: Map[String, DataFrame]) => {
    dfs("Patientreportedmeds")
      .join(dfs("mpitemp"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
  }

  map = Map(
    "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
    "MPI" -> mapFrom("MPI"),
    "MEDICATION_DTM" -> mapFrom("MEDREPORTEDTIME"),
    "MEDICATION_NAME" -> mapFrom("LOCALDRUGDESCRIPTION"),
    "NDC" -> mapFrom("MAPPEDNDC"),
    "DCC" -> mapFrom("DCC"),
    "RXORDER_IND" -> literal("0"),
    "RXADMIN_IND" -> literal("0"),
    "PAT_REPORTED_IND" -> literal("1")
  )
}