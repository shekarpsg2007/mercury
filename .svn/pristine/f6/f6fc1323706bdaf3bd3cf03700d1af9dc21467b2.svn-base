package com.humedica.mercury.etl.core.util

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object LocalSparkSession {
  def get(test: Class[_]): SparkSession = {
    LogManager.getLogger("com.humedica").setLevel(Level.DEBUG)
    SparkSession.builder()
      .appName(test.getCanonicalName)
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.port", 52305)
      .config("spark.driver.memory", "6g")
      .config("spark.cores.max", 6)
      .getOrCreate
  }
}
