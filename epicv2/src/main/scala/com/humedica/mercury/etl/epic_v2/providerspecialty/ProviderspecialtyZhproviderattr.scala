package com.humedica.mercury.etl.epic_v2.providerspecialty

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by bhenriksen on 1/25/17.
 */
class ProviderspecialtyZhproviderattr(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  //no incl. crit.

  tables = List("zh_providerattr")

  join = noJoin()

  beforeJoin = Map(
    "zh_providerattr" -> ((df: DataFrame) => {
      val fil = df.filter("PROV_ID <> '-1' and PROV_ID <> '*'")
      val groups = Window.partitionBy(fil("PROV_ID")).orderBy(fil("EXTRACT_DATE").desc)
      fil.withColumn("rn", row_number.over(groups)).select("PROV_ID", "FILEID", "rn")
    })
  )


  afterJoin = (df: DataFrame) => {
    val dfzp = readTable("zh_providerattr",config)
    val dfzp2 = dfzp.filter("PROV_ID <> '*' and PROV_ID <> '-1'")
    val dfzp3 = dfzp2.withColumnRenamed("PROV_ID","PROV_ID_jn").withColumnRenamed("FILEID","FILEID_jn")
    df.join(dfzp3, df("PROV_ID") === dfzp3("PROV_ID_jn") && df("FILEID") === dfzp3("FILEID_jn") && df("rn") === 1,"inner")
  }


  map = Map(
    "LOCALSPECIALTYCODE" -> unpivot(Seq("PROV_SPECIALTY1", "PROV_SPECIALTY2", "PROV_SPECIALTY3","PROV_SPECIALTY4", "PROV_SPECIALTY5", "PROV_TYPE"),
      types=Seq("1", "2","3","4","5","6"), typeColumnName = "LOCAL_CODE_ORDER" ),
    "LOCALCODESOURCE" -> literal("zh_provider"),
    "LOCALPROVIDERID" -> mapFrom("PROV_ID")
  )

  afterMap = (df: DataFrame ) => {
    df.dropDuplicates("PROV_ID", "LOCALSPECIALTYCODE", "LOCALCODESOURCE", "LOCAL_CODE_ORDER", "FILEID")
  }

  mapExceptions = Map(
    //("H406239_EP2", "LOCALSPECIALTYCODE") -> pivot("PROV_SPECIALTY1")
  )
}

// val p = new ProviderspecialtyZhproviderattr(cfg) ; val ps = build(p); ps.show ; ps.count
