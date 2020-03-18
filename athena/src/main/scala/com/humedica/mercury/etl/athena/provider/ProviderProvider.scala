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


class ProviderProvider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("provider")

      columnSelect = Map(
                   "provider" -> List("PROVIDER_ID", "PROVIDER_TYPE", "PROVIDER_FIRST_NAME", "PROVIDER_LAST_NAME", "PROVIDER_NPI_NUMBER",
                               "BILLED_NAME")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //DO NOT EXCLUDE records where deleted_datetime is null


      map = Map(
        "LOCALPROVIDERID" -> todo("PROVIDER_ID", "PROVIDER_NPI_NUMBER"),      //TODO - to be coded
        "CREDENTIALS" -> mapFrom("PROVIDER_TYPE"),
        "FIRST_NAME" -> mapFrom("PROVIDER_FIRST_NAME"),
        "LAST_NAME" -> mapFrom("PROVIDER_LAST_NAME"),
        "NPI" -> mapFrom("PROVIDER_NPI_NUMBER"),
        "SUFFIX" -> mapFrom("BILLED_NAME")
      )

 }