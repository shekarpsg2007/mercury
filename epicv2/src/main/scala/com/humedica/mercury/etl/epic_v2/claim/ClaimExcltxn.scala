package com.humedica.mercury.etl.epic_v2.claim

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by cdivakaran on 5/19/17.
  */
class ClaimExcltxn(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List("inptbilling_txn")

  cacheMe = true

  columns = List("ORIG_REV_TX_ID")

  beforeJoin = Map(
    "inptbilling_txn" -> ((df: DataFrame) => {
      df.filter("ORIG_REV_TX_ID <> '-1' and ORIG_REV_TX_ID is not null").select("ORIG_REV_TX_ID").distinct()
    })

  )

}
