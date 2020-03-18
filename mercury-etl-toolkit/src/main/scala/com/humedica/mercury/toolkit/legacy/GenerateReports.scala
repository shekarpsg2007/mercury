package com.humedica.mercury.toolkit.legacy

import com.humedica.mercury.etl.core.engine.Metrics
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jrachlin on 5/18/17.
  */
object GenerateReports {
  def main(args: Array[String]): Unit = {

    // Check command line, read arguments
    if (args.length != 3 && args.length != 4) {
      println("USAGE: GenerateReports <CDR-Table> <Spark-Table> <Report-Path> [<DATASRC>]")
      System.exit(1)
    }

    val (cdrPath,spkPath,rptPath) = (args(0), args(1), args(2))
    val src = if (args.length == 4) args(3) else null

    // Initialize context
    val conf = new SparkConf().setAppName("GenerateReports")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Read tables and optionally filter for data sourceb
    val cdrRaw = sqlContext.read.parquet(cdrPath)
    val cdr = if (src != null) cdrRaw.filter("DATASRC LIKE '"+src+"'") else cdrRaw

    val spkRaw = sqlContext.read.parquet(spkPath)
    val spk = if (src != null) spkRaw.filter("DATASRC LIKE '"+src+"'") else spkRaw

    val summary = Metrics.report(cdr, spk)
    val delta = Metrics.deltaReport(cdr, spk)

    summary.write.parquet(rptPath+"/SUMMARY")
    delta.write.parquet(rptPath+"/DELTA")
    sc.stop()
  }

}
