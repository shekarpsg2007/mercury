package com.humedica.mercury.etl.core.apps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jrachlin on 5/18/17.
  */
object RecordCounts {


  def main(args: Array[String]): Unit = {

    // Check command line, read arguments
    if (args.length != 1) {
      println("USAGE: RecordCounts <input-path>")
      System.exit(1)
    }

    val in = args(0)

    // Initialize context
    val conf = new SparkConf().setAppName("FilterByFileID")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    // Get list of input files
    val fs = FileSystem.get(new Configuration())
    val filelist = fs.listStatus(new Path(in)).map(_.getPath)

    filelist.foreach(f => {
      val df = sqlContext.read.parquet(f.toString+"/*.parquet")
      println(f.toString +":\t"+df.count)
    })
  }

}
