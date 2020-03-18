package com.humedica.mercury.toolkit.schema

import org.apache.hadoop.fs.{FileSystem, Path}

case class RawCompareParameters(etlBuildPath: Path, cdrRawDataPath: Path,
                                reportsPath: Path, runId: String,
                                cdrTables: Seq[String], reportTypes: Seq[String]) {
  def validate(fs: FileSystem): Unit = {
    require(fs.exists(etlBuildPath), "Missing etl build path")
    require(fs.exists(cdrRawDataPath), "Missing raw CDR data path")
    require(fs.exists(reportsPath), "Missing reports path")
  }
}

object RawCompareParametersIn {
  def read(): RawCompareParameters = RawCompareParameters(
    new Path(System.getProperty("mtk.rawcomp.etlBuildPath")),
    new Path(System.getProperty("mtk.rawcomp.cdrRawDataPath")),
    new Path(System.getProperty("mtk.rawcomp.reportsPath")),
    System.getProperty("mtk.rawcomp.runId"),
    if (System.getProperty("mtk.rawcomp.cdrTables") != null)
      System.getProperty("mtk.rawcomp.cdrTables").split(",")
    else Seq(),
    if (System.getProperty("mtk.rawcomp.reportTypes") != null)
      System.getProperty("mtk.rawcomp.reportTypes").split(",")
    else Seq(ReportTypes.fullComp, ReportTypes.autoDiff)
  )
}

object ReportTypes {
  val fullComp = "fullComp"
  val autoDiff = "autoDiff"
}
