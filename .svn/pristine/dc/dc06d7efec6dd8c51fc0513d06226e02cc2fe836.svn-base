package com.humedica.mercury.etl.core.apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
//import org.apache.spark.sql.hive.HiveContext
import com.humedica.mercury.etl.core.engine.Engine
import com.humedica.mercury.etl.core.engine.Constants._
import java.util.Properties
import java.io._
import scala.collection.JavaConversions._


/**
  * Created by jrachlin on 5/18/17.
  */
object BuildSpec {

  def main(args: Array[String]): Unit = {

    if (args.length != 2 && args.length != 3) {
      println("USAGE: BuildSpec <entity> <source> [<etl.cfg>]")
      System.exit(1)
    }

    // Extract command line arguments
    val entity = args(0).toLowerCase.capitalize
    val src = args(1).toLowerCase.capitalize

    // Read engine settings
    val cfgName = if (args.length == 3) args(2) else "etl.cfg"

    val props = new Properties()
    try {
      val fi = new FileInputStream(cfgName)
      val f = new File(cfgName)
      props.load(new FileInputStream(f)); // new FileInputStream(cfgName))
    } catch {
      case ex: FileNotFoundException => println("Can't find configration file: "+cfgName); System.exit(1)
    }

    val cfg = props.toMap + ("ENTITY" -> entity) + ("ENTITYSOURCE" -> src)


    val spark = SparkSession
      .builder()
      .appName("BuildSpec")
      .config("spark.sql.caseSensitive", "false")
      .enableHiveSupport()
      .getOrCreate()


    Engine.setSession(spark)
    //Engine.build(cfg)

    //Engine.stop()

    spark.stop()

  }
}
