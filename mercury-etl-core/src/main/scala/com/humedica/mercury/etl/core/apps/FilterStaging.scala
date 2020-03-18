package com.humedica.mercury.etl.core.apps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by jrachlin on 5/18/17.
  */
object FilterStaging {


  def main(args: Array[String]): Unit = {

    // Check command line, read arguments
    if (args.length != 3 && args.length != 4) {
      println("USAGE: FilterStaging <input-path> <fileid-path> <output-path> [<join-column>]")
      System.exit(1)
    }

    val (in,files,out) = (args(0), args(1), args(2))
    val joinColumn = if (args.length == 4) args(3) else "FILEID"

    // Initialize context
    val conf = new SparkConf().setAppName("FilterByFileID")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    // Read fileids and cache
    val fileids = sqlContext.read.parquet(files).cache()

    // Get list of input files
    val fs = FileSystem.get(new Configuration())
    val filelist = fs.listStatus(new Path(in)).map(_.getPath)

    filelist.foreach(f => {
      print("Filtering: "+f.getName()+":\t")
      val df = sqlContext.read.parquet(f.toString+"/*")
      print(df.count+" -> ")
      val filtered = if (f.getName.toUpperCase.startsWith("ZH")) df else df.join(fileids, joinColumn)
      filtered.write.parquet(out+"/"+f.getName)
      println(filtered.count)
    })
  }

}
