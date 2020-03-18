package com.humedica.mercury.etl.epic_v2.provideridentifier

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by bhenriksen on 1/25/17.
 */
class ProvideridentifierZhproviderattr(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  tables = List("zh_providerattr",
    "zh_identity_ser_id",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "zh_providerattr" -> List("DEA_NUMBER", "PROV_ID","prov_name","SSN","UPIN","LICENSE_NUM","DEA_NUMBER"),
    "zh_identity_ser_id" -> List("PROV_ID","IDENTITY_TYPE_ID")
  )


  beforeJoin = Map(
    "zh_providerattr" -> ((df: DataFrame) => {
      val fil = df.filter("prov_name <> '*'").withColumnRenamed("PROV_ID","PROV_ID_att")
      val fpiv = unpivot(
        Seq("SSN", "UPIN", "LICENSE_NUM", "DEA_NUMBER"),
        Seq("SSN", "UPIN", "license_num", "DEA"), typeColumnName = "ID_TYPE")
      fpiv("ID", fil)
    }),
    "cdr.map_predicate_values" -> ((df: DataFrame) => {
      val fil =df.filter("GROUPID = '" + config(GROUP) + "' AND DATA_SRC = 'ZH_IDENTITY_SER_ID' AND ENTITY = 'ZH_PROVIDER' AND TABLE_NAME = 'ZH_IDENTITY_SER_ID' AND COLUMN_NAME = 'IDENTITY_TYPE_ID' AND CLIENT_DS_ID = '" + config(CLIENT_DS_ID) + "'")
      fil.select("COLUMN_VALUE")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_providerattr")
      .join(dfs("zh_identity_ser_id")
        .join(dfs("cdr.map_predicate_values"), dfs("zh_identity_ser_id")("IDENTITY_TYPE_ID") === dfs("cdr.map_predicate_values")("COLUMN_VALUE"),"inner")
        ,dfs("zh_providerattr")("PROV_ID_att") === dfs("zh_identity_ser_id")("PROV_ID"),"inner" )
  }



  map = Map(
    //"DATASRC" -> literal("zh_providerattr"),
    // "ID_TYPE" -> populate based on value of ID_VALUE
    "PROVIDER_ID" -> mapFrom("PROV_ID_att"),
    "ID_VALUE" -> mapFrom("ID")
  )


  afterMap = (df: DataFrame) => {
    df.dropDuplicates("PROVIDER_ID", "ID_TYPE", "ID_VALUE")
  }
}
