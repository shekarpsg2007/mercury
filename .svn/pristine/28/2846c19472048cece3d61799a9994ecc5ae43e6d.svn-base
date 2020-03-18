package com.humedica.mercury.etl.athena.providerspecialty

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 09/21/2018
 */


class ProviderspecialtyProvider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("provider")

      columnSelect = Map(
                   "provider" -> List("PROVIDER_ID", "SPECIALTY_CODE")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //DO NOT EXCLUDE records where deleted_datetime is null


      map = Map(
        "LOCALPROVIDERID" -> mapFrom("PROVIDER_ID"),
        "LOCALSPECIALTYCODE" -> cascadeFrom(Seq("SPECIALTY_CODE", "SPECIALTY"))
      )

 }