package com.humedica.mercury.etl.fdr.provider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class ProviderProviders(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {


  tables=List("Provider:"+config("EMR")+"@Provider", "Providerspecialty:"+config("EMR")+"@Providerspecialty")

  columnSelect = Map(
    "Providerspecialty" -> List("LOCALPROVIDERID", "LOCALSPECIALTYCODE")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("Provider")
      .join(dfs("Providerspecialty"), Seq("LOCALPROVIDERID"), "left_outer")
  }


  map = Map(
    "NPI" -> mapFrom("NPI"),
    "DISPLAY_NAME" -> mapFrom("PROVIDERNAME"),
    "PRIMARY_SPECIALTY" -> mapFrom("LOCALSPECIALTYCODE"),
    "CREDENTIALS" -> mapFrom("CREDENTIALS"),
    "PHONE" -> mapFrom("WORK_PHONE")
  )

}

