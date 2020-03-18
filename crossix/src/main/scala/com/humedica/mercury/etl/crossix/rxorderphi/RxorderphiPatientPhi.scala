package com.humedica.mercury.etl.crossix.rxorderphi

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderphiPatientPhi(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI","DOB", "FIRST_NAME", "LAST_NAME", "MAPPED_GENDER", "MAPPED_ETHNICITY", "MAPPED_RACE", "MAPPED_ZIPCODE")

    tables = List("temptable:crossix.parient.PatientSummaryWithoutGRPMPITempTableReg", "cdr.patient_mpi", "common.all_groupids", "common.grp_mpi_xref_iot")

    columnSelect = Map(
        "temptable:crossix.parient.PatientSummaryWithoutGRPMPITempTableReg" -> List(),
        "cdr.patient_mpi" -> List("CLIENT_DS_ID","PATIENTID","GRP_MPI"),
        "common.all_groupids" -> List(),
        "common.grp_mpi_xref_iot" -> List("GRP_MPI", "NEW_ID")
    )

    beforeJoin = Map(
        "cdr.patient_mpi" -> ((df: DataFrame) => {df.distinct().withColumnRenamed("GRP_MPI", "CDR_GRP_MPI")}),
        "common.grp_mpi_xref_iot" -> ((df: DataFrame) => {df.withColumnRenamed("GRP_MPI", "G_GRP_MPI").withColumnRenamed("NEW_ID", "G_NEW_ID")})
    )

    join = (dfs: Map[String, DataFrame])  => {
        var patient_phi = dfs("cdr.patient_mpi")
        var patientSummaryWithoutGRPMPITempTableReg = dfs("temptable:crossix.parient.PatientSummaryWithoutGRPMPITempTableReg")
        var all_groupids = dfs("common.all_groupids")
        var grp_mpi_xref_iot = dfs("common.grp_mpi_xref_iot")

        var patSummaryJoin = patientSummaryWithoutGRPMPITempTableReg.join(patient_phi, Seq("PATIENTID"), "left_outer")
        patSummaryJoin = all_groupids.join(patSummaryJoin, Seq("GROUPID"), "inner")
        patSummaryJoin.join(grp_mpi_xref_iot, patSummaryJoin("CDR_GRP_MPI")===grp_mpi_xref_iot("G_GRP_MPI"), "left_outer")
    }
}


//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")