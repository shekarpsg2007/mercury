package com.humedica.mercury.etl.core.apps

import com.humedica.mercury.etl.core.schema._
import com.humedica.mercury.etl.core.util._
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.slf4j._
import java.util.Objects._

object ParquetJdbcExport {

  private val log = LoggerFactory.getLogger(getClass)

  def apply(spark: SparkSession, fs: FileSystem, inputDir: Path,
            tableNameSuffix: String, jdbcParams: OracleJdbcParameters): Unit = {
    requireNonNull(fs)
    requireNonNull(inputDir)
    requireNonNull(jdbcParams)
    require(fs.isDirectory(inputDir))

    val dirs = RemoteIteratorToList.apply(fs.listFiles(inputDir, true))
      .map(p0 => p0.getPath.getParent)
      .filter(p0 => fs.isDirectory(p0))
      .filter(p0 => p0.getName != inputDir.getName).toSet

    JdbcDialects.registerDialect(new OracleDialect)

    dirs.foreach(p0 => {
      var tableName = p0.getName
      if (tableNameSuffix != null) tableName = String.format("%s%s", tableName, tableNameSuffix)
      log.info(s"Writing data from [$p0]")
      log.info(s"Target table name: [$tableName]")
      val data = spark.read.parquet(p0.toString)
      log.info(data.schema.toString)
      data.write.mode(SaveMode.Overwrite)
        .jdbc(jdbcParams.getJdbcUrl, tableName, jdbcParams.getConnectionProperties)
    })
  }

  def main(farts: Array[String]): Unit = {
    val spark = SparkSession.builder.appName(getClass.getCanonicalName).getOrCreate
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val inputPath = System.getProperty("pqe.inputDir")
    val tableNameSuffix = System.getProperty("pqe.tableNameSuffix")
    apply(spark, fs, new Path(inputPath), tableNameSuffix, OracleJdbcParametersIn.read("pqe"))
  }
}
