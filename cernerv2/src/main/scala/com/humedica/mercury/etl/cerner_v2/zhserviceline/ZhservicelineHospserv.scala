package com.humedica.mercury.etl.cerner_v2.zhserviceline

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame

/**
 * Auto-generated on 08/09/2018
 */


class ZhservicelineHospserv(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_code_value")

  columnSelect = Map(
    "zh_code_value" -> List("CODE_VALUE", "DISPLAY", "CODE_SET")
  )

  afterJoin = (df: DataFrame) => {
    df.filter("code_set = '34' and display is not null")
      .distinct()
  }

  map = Map(
    "LOCALSERVICECODE" -> mapFrom("CODE_VALUE", prefix = config(CLIENT_DS_ID) + "."),
    "SERVICECODEDESC" -> mapFrom("DISPLAY")
  )

}