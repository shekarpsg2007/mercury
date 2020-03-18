package com.humedica.mercury.etl.fdr.mpi

import java.sql.{CallableStatement, Connection, DriverManager, Types}

import com.humedica.mercury.etl.core.engine.Engine
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by jrachlin on 9/14/17.
  */
class MPIUtils(url: String, user: String, pw: String) {


  val prop = new java.util.Properties
  prop.setProperty("driver", "oracle.jdbc.OracleDriver")
  prop.setProperty("user", user)
  prop.setProperty("password", pw)

  /**
    * Create a temporary batch table for receiving patient data
    * @param name
    * @return
    */
  def createPatientBatchTable(name: String): String = {
    try {
      Class.forName("oracle.jdbc.OracleDriver")
      val con: Connection = DriverManager.getConnection(url, user, pw)
      println(name)
      val cstmt: CallableStatement = con.prepareCall("{call mdm_repository.fdr_api_pkg.create_pat_batch_table(?,?)}")
      cstmt.setString(1, name)
      cstmt.registerOutParameter(2, Types.VARCHAR)
      cstmt.execute()
      val batchTable = cstmt.getString(2)
      cstmt.close()
      con.close()

      return batchTable

    } catch {
      case ex: Exception => println("Exception: " + ex.getMessage()); ex.printStackTrace(); null

    }
  }

  /**
    * Upload patient data
    * @param targetTable Table to receive patient data
    * @param df Dataframe containing patient data
    * @return
    */
  def uploadPatientData(targetTable: String, df: DataFrame, limit: Int = 0): Boolean = {
    if (limit > 0)
      df.limit(limit).write.mode("append").jdbc(url, targetTable, prop)
    else df.write.mode("append").jdbc(url, targetTable, prop)

    true
  }



  def processPatientData(group: String, batchTable: String): Array[String] = {
    try {
      Class.forName("oracle.jdbc.OracleDriver")
      val con: Connection = DriverManager.getConnection(url, user, pw)
      val cstmt: CallableStatement = con.prepareCall("{call mdm_repository.fdr_api_pkg.process_pat_batch_table(?,?,?,?)}")
      cstmt.setString(1, group)
      cstmt.setString(2, batchTable)
      println(batchTable)
      cstmt.registerOutParameter(3, Types.VARCHAR)
      cstmt.registerOutParameter(4, Types.VARCHAR)
      cstmt.execute()
      val mpiTable = "mdm_repository."+cstmt.getString(3)
      val goldenRecordTable = "mdm_repository."+cstmt.getString(4)
      println(mpiTable)
      println(goldenRecordTable)
      val arr = Array(mpiTable, goldenRecordTable)
      cstmt.close()
      con.close()
      return arr
    } catch {
      case ex: Exception => println("Exception: " + ex.getMessage()); ex.printStackTrace(); null

    }
  }


  def fetchPatientMPI(cdsId: String, mpiTable: String): DataFrame = {
 //   println(mpiTable)

    val df = Engine.spark.read.jdbc(url, mpiTable, prop)

    // convert cdsid from decimal(38,10) to int - talk to ian about this.
    df.filter("CLIENT_DS_ID = "+cdsId).withColumn("CLIENT_DS_ID", regexp_extract(df("CLIENT_DS_ID"), "[0-9]*", 0))
  }



  def fetchGoldenRecord(cdsId: String, goldenRecordTable: String): DataFrame = {
    //   println(mpiTable)

    Engine.spark.read.jdbc(url, goldenRecordTable, prop)

  }

}
