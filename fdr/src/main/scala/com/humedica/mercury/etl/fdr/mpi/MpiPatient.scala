package com.humedica.mercury.etl.fdr.mpi

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import com.humedica.mercury.etl.fdr.mpi.MPIUtils

class MpiPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]){


  cacheMe = true

  tables=List("Patient:fdr.patient.PatientPatients")
  //tables = List("fdr.patsmall")

  columns =List("GROUPID", "CLIENT_DS_ID", "PATIENTID", "MPI")

  afterMap = (df2: DataFrame) => {

    val df = df2.drop("GROUPID").drop("PATIENTID") // these got added during mapping - discard

    val mpi = new MPIUtils(config("DB_URL"), config("DB_USER"), config("DB_PASSWORD"))

    println("CREATING PATIENT BATCH TABLE")
    val batchTable = mpi.createPatientBatchTable(config(CLIENT_DS_ID))
    println("UPLOADING PATIENT DATA")
    val success = mpi.uploadPatientData(batchTable, df)

    if (success) {
      println("PROCESSING PATIENT DATA")
      val array1 = mpi.processPatientData(config(GROUP),batchTable)
      val mpiTable=array1(0)
      val goldenrecordTable=array1(1)

      println("FETCHING DATA")
      val data = mpi.fetchPatientMPI(config(CLIENT_DS_ID), mpiTable)
      val goldenrecord = mpi.fetchGoldenRecord(config(CLIENT_DS_ID), goldenrecordTable)



      goldenrecord.write.parquet(config(OUT_DATA_ROOT)+"/GOLDENRECORD")


      data
    } else null
  }
}

