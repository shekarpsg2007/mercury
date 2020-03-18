package com.humedica.mercury.toolkit.impl

import com.humedica.mercury.etl.core.engine.Metrics
import com.humedica.mercury.toolkit.schema.ReportTypes
import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import com.humedica.mercury.etl.core.engine.ShowDf._

object DiffReport {

  val log: Logger = LoggerFactory.getLogger(getClass)

  def apply(reportTypes: Seq[String], cdrDf: DataFrame, sparkDf: DataFrame): Unit = {
    log.info("Comparison parameters.")
    log.info(ToStringBuilder.reflectionToString(reportTypes, ToStringStyle.MULTI_LINE_STYLE))
    log.info("")

    if (reportTypes.contains(ReportTypes.fullComp)) {
      val r0 = Metrics.report(cdrDf, sparkDf)
      log.info(show(r0, false))
    }

    if (reportTypes.contains(ReportTypes.autoDiff)) {
      Diff.autodiff(cdrDf, sparkDf, silent = false)
    }

    log.info("Schema structure - Spark ETL")
    log.info(sparkDf.schema.toString())
    log.info(show(sparkDf, 10, false))

    log.info("Schema structure - Oracle data")
    log.info(cdrDf.schema.toString())
    log.info(show(cdrDf, 10, false))
  }
}
