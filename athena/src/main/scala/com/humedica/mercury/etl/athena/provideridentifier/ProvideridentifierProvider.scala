package com.humedica.mercury.etl.athena.provideridentifier

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ProvideridentifierProvider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("provider")

      columnSelect = Map(
                   "provider" -> List("PROVIDER_NPI_NUMBER", "PROVIDER_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //DO NOT EXCLUDE records where deleted_datetime is null


      map = Map(
        "ID_VALUE" -> todo("PROVIDER_NPI_NUMBER", "PROVIDER_NDC_TAT_NUMBER"),      //TODO - to be coded
        "PROVIDER_ID" -> mapFrom("PROVIDER_ID")
      )

 }