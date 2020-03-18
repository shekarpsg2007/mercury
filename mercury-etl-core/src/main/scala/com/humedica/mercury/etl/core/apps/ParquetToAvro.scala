package com.humedica.mercury.etl.core.apps


import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by cdivakaran.
  */
object ParquetToAvro {

  val spark = makeSession()
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    try
    {
      val readPath=args(0)
      val writePath=args(1)
      val df = spark.read.parquet(readPath).coalesce(10)
      df.write.avro(writePath)
      val df1 = spark.read.avro(writePath)
      log.info(s"input count ${df.count}")
      log.info(s"output count ${df1.count}")
      if (df1.count.equals(df.count))
      //if (df1.count.equals(1000))
      {
        log.info("Avro Conversion good")
      }
      else throw new IllegalStateException("Counts did not match")
    }
    catch {
      case ex: Exception  => {
        log.error(ex.getMessage)
        throw ex
      }
    }
  }



  def makeSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("ParquetToAvro")
      .getOrCreate()
  }
}






