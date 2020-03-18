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


class ProvideridentifierReferringprovider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("referringprovider")

      columnSelect = Map(
                   "referringprovider" -> List("NPI_NUMBER", "REFERRING_PROVIDER_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //DO NOT EXCLUDE records where deleted_datetime is null


      map = Map(
        "ID_TYPE" -> todo(""),      //TODO - to be coded
        "ID_VALUE" -> mapFrom("NPI_NUMBER"),
        "PROVIDER_ID" -> mapFrom("REFERRING_PROVIDER_ID")
      )

 }