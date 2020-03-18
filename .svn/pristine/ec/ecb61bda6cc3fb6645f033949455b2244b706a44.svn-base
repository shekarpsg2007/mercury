package com.humedica.mercury.etl.core.apps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jrachlin on 5/18/17.
  */
object DumpSchema {


  def main(args: Array[String]): Unit = {

    // Check command line, read arguments
    if (args.length != 3) {
      println("USAGE: DumpSchema <input-path>")
      System.exit(1)
    }

    val (in,files,out) = (args(0), args(1), args(2))

    // Initialize context
    val conf = new SparkConf().setAppName("FilterByFileID")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Read fileids and cache
    val fileids = sqlContext.read.parquet(files).cache()

    // Get list of input files
    val fs = FileSystem.get(new Configuration())
    val filelist = fs.listStatus(new Path(in)).map(_.getPath)

    filelist.foreach(f => {
      val df = sqlContext.read.parquet(f.toString+"/*.parquet")
      println("\n\n-----------------------")
      println(f.toString)
      df.printSchema()
    })
  }

}
