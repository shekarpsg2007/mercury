package com.humedica.mercury.etl.core.apps

import org.apache.spark.sql.SparkSession

/**
  * Created by bhenriksen on 12/12/17.
  */
object FdrReadWrite extends App {

  val spark = makeSession()
  val readPath=args(0)
  val writePath=args(1)

  spark.read.parquet(readPath).repartition(1)
    .write.option("delimiter","|").option("codec","gzip")
    .option("dateFormat","YYYY/MM/DD hh24:mm:ss")
    .csv(writePath)

  def makeSession(): SparkSession = SparkSession.builder()
    .appName(getClass.getCanonicalName).getOrCreate()
}