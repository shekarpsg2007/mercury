package integration

import com.humedica.mercury.etl.core.util.LocalSparkSession
import com.humedica.mercury.toolkit.impl.SketchDiff
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}
import org.scalatest.junit.JUnitSuite

class SketchDiffSpec extends JUnitSuite {

  var spark: SparkSession = _
  var fs: FileSystem = _

  @Before def initialize(): Unit = {
    spark = LocalSparkSession.get(this.getClass)
    fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  }

  @Test def sketchDiffTest(): Unit = {
    val df0Src = "/Users/jzazueta/Desktop/PATIENT_SUMMARY"
    val df1Src = "/Users/jzazueta/Desktop/CDR_201711_H704847_PATIENT_SUMMARY"
    val df0 = spark.read.parquet(df0Src)
    val df1 = spark.read.parquet(df1Src)
    SketchDiff.matchPct(df0, df1, spark)
  }

}
