package com.humedica.mercury.etl.crossix.membercoveragepartd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource


/**
  * Created by eleanasd on 3/15/17.
  */
class MembercoveragepartdMembercoveragepartd(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("member_coverage_partd")

  columns = List("D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
    "STATE_CODE", "YEAR_OF_BIRTH")

  columnSelect = Map(
    "member_coverage_partd" -> List("D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "LSRD_INDIVIDUAL_ID", "LSRD_MEMBER_SYSTEM_ID",
      "STATE_CODE", "YEAR_OF_BIRTH")
  )
  map = Map(
    "LSRD_MEMBER_SYSTEM_ID" -> ((col: String, df: DataFrame) => df.withColumn(col, sha2(df("LSRD_MEMBER_SYSTEM_ID"), 256)))
  )

}

//  build(new MembercoveragepartdMembercoveragepartd(cfg), allColumns=true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get+"/MEMBER_COVERAGE_PARTD_201710")