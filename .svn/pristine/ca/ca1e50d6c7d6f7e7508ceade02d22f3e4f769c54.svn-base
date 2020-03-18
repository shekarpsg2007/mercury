package com.humedica.mercury.etl.epic_v2.zhserviceline


import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 02/01/2017
 */


class ZhservicelineService(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_pat_service")


  columnSelect = Map(
    "zh_pat_service" -> List("HOSP_SERV_C", "NAME")
    )

  beforeJoin = Map(
    "zh_pat_service" -> includeIf("HOSP_SERV_C is not null and NAME is not null")
  )


  beforeJoinExceptions =
    Map(
      "H135535_EP2" -> Map(
        "zh_pat_service" -> ((df: DataFrame) => {
          df.filter("HOSP_SERV_C is not null and NAME is not null and NAME rlike '^(?![A-Z]{3}-).*'")
        })
      ),
      "H262866_EP2" -> Map(
        "zh_pat_service" -> ((df: DataFrame) => {
          df.filter("HOSP_SERV_C is not null and NAME is not null and NAME rlike '^(?![A-Z]{3}-).*'")
        })
      ),
      "H557454_EP2" -> Map(
        "zh_pat_service" -> ((df: DataFrame) => {
          df.filter("HOSP_SERV_C is not null and NAME is not null and NAME rlike '^(?![A-Z]{3}-).*'")
        })
      ),
      "H827927_EP2" -> Map(
        "zh_pat_service" -> ((df: DataFrame) => {
          df.filter("HOSP_SERV_C is not null and NAME is not null and NAME rlike '^(?![A-Z]{3}-).*'")
        })
      ),
      "H846629_EP2" -> Map(
        "zh_pat_service" -> ((df: DataFrame) => {
          df.filter("HOSP_SERV_C is not null and NAME is not null and NAME rlike '^(?![A-Z]{3}-).*'")
        })
      )
    )


  join = noJoin()


  map = Map(
    //   "DATASRC" -> literal("service"),
    "LOCALSERVICECODE" -> mapFrom("HOSP_SERV_C", prefix=config(CLIENT_DS_ID)+"."),
    "SERVICECODEDESC" -> mapFrom("NAME")
  )

}