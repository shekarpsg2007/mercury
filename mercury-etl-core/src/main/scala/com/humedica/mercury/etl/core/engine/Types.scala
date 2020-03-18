package com.humedica.mercury.etl.core.engine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
  * Created by jrachlin on 12/8/16.
  */
object Types {

  type JoinFunction = (Map[String,DataFrame]) => DataFrame

  type ProvenanceFunction = (String, DataFrame) => DataFrame

  type DataFunction = (DataFrame) => DataFrame

  val nullColumn = lit(null).cast(StringType)
}
