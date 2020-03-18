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


class ProvideridentifierClinicalprovider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("clinicalprovider")

      columnSelect = Map(
                   "clinicalprovider" -> List("NPI", "CLINICAL_PROVIDER_ID")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //DO NOT EXCLUDE records where deleted_datetime is null


      map = Map(
        "ID_TYPE" -> todo(""),      //TODO - to be coded
        "ID_VALUE" -> mapFrom("NPI"),
        "PROVIDER_ID" -> mapFrom("CLINICAL_PROVIDER_ID")
      )

 }