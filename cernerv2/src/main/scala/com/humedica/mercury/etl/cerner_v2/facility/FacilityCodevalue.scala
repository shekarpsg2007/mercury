package com.humedica.mercury.etl.cerner_v2.facility

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame


/**
 * Auto-generated on 08/09/2018
 */


class FacilityCodevalue(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_code_value")

  columnSelect = Map(
    "zh_code_value" -> List("CODE_VALUE", "DESCRIPTION", "CDF_MEANING")
  )

  beforeJoin = Map(
    "zh_code_value" -> ((df: DataFrame) => {
      df.filter("code_set = 220")
    })
  )
  
  afterJoin = (df: DataFrame) => {
    df.filter("cdf_meaning = 'FACILITY'")
  }
  
  afterJoinExceptions = Map(
    "H416989_CR2" -> ((df: DataFrame) => {
      df.filter("cdf_meaning = 'BUILDING'")
    })
  )
  
  map = Map(
    "FACILITYID" -> mapFrom("CODE_VALUE"),
    "FACILITYNAME" -> mapFrom("DESCRIPTION")
  )

}