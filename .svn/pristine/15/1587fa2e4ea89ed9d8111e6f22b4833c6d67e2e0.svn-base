package com.humedica.mercury.etl.cerner_v2.rxordersandprescriptions

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class RxordersandprescriptionsTempmed(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  tables = List("zh_order_catalog_item_r", "zh_med_identifier", "cdr.map_predicate_values")
  
  columns = List("CATALOG_CD", "LOCALGENERICDESC", "LOCALNDC")
  
  columnSelect = Map(
    "zh_order_catalog_item_r" -> List("CATALOG_CD", "ITEM_ID"),
    "zh_med_identifier" -> List("ITEM_ID", "MED_IDENTIFIER_TYPE_CD", "VALUE", "VALUE_KEY", "ACTIVE_IND")
  )

  beforeJoin = Map(
    "zh_med_identifier" -> ((df: DataFrame) => {
      val list_localdrugdesc = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "LOCALDRUGDESC", "TEMP_MEDS_"+config(GROUP), "ZH_MED_IDENTIFIER", "MED_IDENTIFIER_TYPE_CD")
      val list_localndc = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "LOCALNDC", "TEMP_MEDS_"+config(GROUP), "ZH_MED_IDENTIFIER", "MED_IDENTIFIER_TYPE_CD")
      df.filter("active_ind = 1 and med_identifier_type_cd in (" + list_localdrugdesc + "," + list_localndc +")")
    })
  )
  
  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_order_catalog_item_r")
      .join(dfs("zh_med_identifier"), Seq("ITEM_ID"), "inner")
  }
  
  afterJoin = (df: DataFrame) => {
    val groups_rn = Window.partitionBy(df("CATALOG_CD"), df("MED_IDENTIFIER_TYPE_CD"))
      .orderBy(length(df("VALUE")).asc_nulls_last, df("ITEM_ID").asc_nulls_last)
    val groups_cnt = Window.partitionBy(df("CATALOG_CD"), df("MED_IDENTIFIER_TYPE_CD"))
    
    val df2 = df.withColumn("rn", row_number.over(groups_rn))
      .withColumn("cnt_ndc", size(collect_set(df("VALUE_KEY")).over(groups_cnt)))
    
    val list_localdrugdesc = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "LOCALDRUGDESC", "TEMP_MEDS_"+config(GROUP), "ZH_MED_IDENTIFIER", "MED_IDENTIFIER_TYPE_CD")
    val list_localndc = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "LOCALNDC", "TEMP_MEDS_"+config(GROUP), "ZH_MED_IDENTIFIER", "MED_IDENTIFIER_TYPE_CD")
    
    df2.groupBy("CATALOG_CD")
      .agg(max(when(df2("MED_IDENTIFIER_TYPE_CD").isin(list_localdrugdesc: _*) && df2("rn") === lit("1"), df2("VALUE"))).as("LOCALGENERICDESC"),
        max(when(df2("MED_IDENTIFIER_TYPE_CD").isin(list_localndc: _*) && df2("cnt_ndc") === lit("1"), df2("VALUE_KEY"))).as("LOCALNDC"))
  }

}