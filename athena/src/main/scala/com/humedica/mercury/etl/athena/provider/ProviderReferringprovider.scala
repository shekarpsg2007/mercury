package com.humedica.mercury.etl.athena.provider

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ProviderReferringprovider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("referringprovider")

      columnSelect = Map(
                   "referringprovider" -> List("REFERRING_PROVIDER_ID", "REFERRING_PROVIDER_NAME", "REFERRING_PROVIDER_NAME", "NPI_NUMBER", "REFERRING_PROVIDER_NAME")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //DO NOT EXCLUDE records where deleted_datetime is null


      map = Map(
        "LOCALPROVIDERID" -> todo("REFERRING_PROVIDER_ID", "NPI_NUMBER"),      //TODO - to be coded
        "CREDENTIALS" -> todo("REFERRING_PROVIDER_NAME"),      //TODO - to be coded
        "PROVIDERNAME" -> mapFrom("REFERRING_PROVIDER_NAME"),
        "NPI" -> mapFrom("NPI_NUMBER"),
        "SUFFIX" -> mapFrom("REFERRING_PROVIDER_NAME")
      )

 }