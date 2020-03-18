package com.humedica.mercury.toolkit

import com.humedica.mercury.etl.core.engine.LogSupport
import com.humedica.mercury.etl.core.schema.{OracleJdbcParameters, OracleJdbcParametersIn}
import com.humedica.mercury.toolkit.impl.{DiffReport, MetadataEntityToTableMappings}
import com.humedica.mercury.toolkit.schema._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object MetadataRawCompareDriver {

  val log: Logger = LoggerFactory.getLogger(getClass)

  def apply(params: RawCompareParameters, metadataParams: OracleJdbcParameters, sess: SparkSession, fs: FileSystem): Unit = {
    require(params != null)
    require(sess != null)
    require(fs != null)
    params.validate(fs)
    val entityToTables = MetadataEntityToTableMappings.retrieve(metadataParams, sess)
    entityToTables.map {
      case (entity, cdrTable) =>
        val specPath = new Path(params.etlBuildPath, entity.toUpperCase())
        val cdrPath = new Path(params.cdrRawDataPath, cdrTable.toUpperCase)
        log.info(s"Trying [$specPath] -- [$cdrPath]")
        specPath -> cdrPath
    }.filter { case (p0, p1) => fs.exists(p0) && fs.exists(p1) }
      .foreach {
        case (p0, p1) =>
          if (params.cdrTables.isEmpty || params.cdrTables.contains(p1.getName)) {
            val fileName = s"${params.runId}_${p0.getName}-${p1.getName}.txt"
            LogSupport.traceTo(new Path(params.reportsPath, fileName), fs, () => {
              try {
                log.info(s"Attempting to diff raw data [$p0] - [$p1]")
                val specData = sess.read.parquet(p0.toString)
                val cdrData = sess.read.parquet(p1.toString)
                DiffReport.apply(params.reportTypes, cdrData, specData)
              } catch {
                case e: Throwable =>
                  log.error("Unable to complete daw data diff.", e)
                  e.printStackTrace()
              }
            })
          } else {
            log.info(s"Skipping table comparison for CDR table ${p1.getName}")
          }
    }
  }

  def main(args: Array[String]): Unit = {
    val params = RawCompareParametersIn.read()
    val jdbcParams = OracleJdbcParametersIn.read
    val sess = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .config("spark.sql.broadcastTimeout",  36000).getOrCreate()
    val fs = FileSystem.get(sess.sparkContext.hadoopConfiguration)
    apply(params, jdbcParams("metadata"), sess, fs)
  }
}
