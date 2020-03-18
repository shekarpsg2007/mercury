package com.humedica.mercury.etl.cerner_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by mschlomka on 8/14/2018.
  */

class ObservationOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("orders", "order_action", "cdr.zcm_obstype_code")

  columnSelect = Map(
    "orders" -> List("ORDER_ID", "PERSON_ID", "CATALOG_CD", "UPDT_DT_TM", "ORDER_STATUS_CD", "ENCNTR_ID", "ORDER_MNEMONIC"),
    "order_action" -> List("ORDER_ID", "UPDT_DT_TM", "ACTION_DT_TM", "CURRENT_START_DT_TM", "ACTION_TYPE_CD",
      "ORDER_STATUS_CD", "ORDER_DT_TM"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS")
  )

  beforeJoin = Map(
    "orders" -> ((df: DataFrame) => {
      val df2 = df.filter("person_id is not null")
      val groups = Window.partitionBy(df2("ORDER_ID"), df2("PERSON_ID"), df2("CATALOG_CD"))
        .orderBy(df2("UPDT_DT_TM").desc_nulls_last)
      df2.withColumn("rn", row_number.over(groups))
        .withColumn("LOCALCODE", concat_ws(".", lit(config(CLIENT_DS_ID)), df("CATALOG_CD")))
        .filter("rn = 1 and catalog_cd is not null")
        .drop("rn", "UPDT_DT_TM")
    }),
    "order_action" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ORDER_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1 and (action_Type_cd = '2529' or order_status_cd = '2543')")
        .drop("rn", "UPDT_DT_TM", "ORDER_STATUS_CD")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and lower(datasrc) = 'orders' and obstype is not null")
        .drop("GROUPID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("orders")
      .join(dfs("order_action"), Seq("ORDER_ID"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), dfs("orders")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("orders"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "OBSDATE" -> cascadeFrom(Seq("ORDER_DT_TM", "ACTION_DT_TM", "CURRENT_START_DT_TM")),
    "LOCALRESULT" -> mapFrom("ORDER_MNEMONIC"),
    "STATUSCODE" -> mapFrom("ORDER_STATUS_CD"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

}