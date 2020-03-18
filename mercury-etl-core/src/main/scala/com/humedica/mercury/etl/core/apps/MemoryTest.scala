package com.humedica.mercury.etl.core.apps

import java.io._
import java.util.Properties

import com.humedica.mercury.etl.core.engine.Engine
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._


/**
  * Created by jrachlin on 5/18/17.
  */
object MemoryTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MemoryTest")
      .config("spark.sql.caseSensitive", "false")
      .enableHiveSupport()
      .getOrCreate()

    val mc = spark.read.parquet("/user/eleanasd/MEDICAL_CLAIM/*/MEDICAL_CLAIM")
    val ndc = spark.read.parquet("/user/eleanasd/nhi/NDC")
    val j = mc.join(ndc, Seq("NDC_KEY"), "left_outer")
    println("Count: "+j.count)

    spark.stop()

  }
}
