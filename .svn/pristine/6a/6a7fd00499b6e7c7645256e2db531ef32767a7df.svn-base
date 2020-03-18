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


class ProviderClinicalprovider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("clinicalprovider")

      columnSelect = Map(
                   "clinicalprovider" -> List("CLINICAL_PROVIDER_ID", "NAME", "FIRST_NAME", "NAME", "GENDER",
                               "LAST_NAME", "MIDDLE_NAME", "NPI", "NAME")
      )

      //TODO - Create join

      //TODO - Inclusion Criteria:
      //DO NOT EXCLUDE records where deleted_datetime is null


      map = Map(
        "LOCALPROVIDERID" -> todo("CLINICAL_PROVIDER_ID", "NPI"),      //TODO - to be coded
        "CREDENTIALS" -> mapFrom("NAME"),
        "FIRST_NAME" -> mapFrom("FIRST_NAME"),
        "PROVIDERNAME" -> mapFrom("NAME"),
        "GENDER" -> mapFrom("GENDER"),
        "LAST_NAME" -> mapFrom("LAST_NAME"),
        "MIDDLE_NAME" -> mapFrom("MIDDLE_NAME"),
        "NPI" -> mapFrom("NPI"),
        "SUFFIX" -> mapFrom("NAME")
      )

 }