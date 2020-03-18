package com.humedica.mercury.etl.asent.patientaddress

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class PatientaddressPatdem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_patdem")

  columnSelect = Map(
    "as_patdem" -> List("LAST_UPDATED_DATE", "PATIENT_MRN", "PATIENT_STATE", "PATIENT_ZIP", "PATIENT_ADDRESS1",
      "PATIENT_ADDRESS2", "ADDRESSTYPE", "PATIENT_CITY")
  )

  beforeJoin = Map(
    "as_patdem" -> ((df: DataFrame) => {
      df.withColumn("STATE", upper(regexp_replace(df("PATIENT_STATE"), "[[\\W]0-9 ]", "")))
        .filter("PATIENT_STATE is null or length(STATE) = 2")
        .filter("PATIENT_MRN != '0' and (not((upper(patient_last_name) = 'TEST') or (Patient_Last_Name like '*%' and patient_first_name is null) or " +
          "upper(PATIENT_LAST_NAME) rlike '(^TEST |TEST$|ZZTEST)' or upper(PATIENT_FIRST_NAME) rlike 'TEST' ))")
        .filter("PATIENT_MRN is not null and (PATIENT_ADDRESS1 is not null or PATIENT_ADDRESS2 is not null or PATIENT_CITY is not null or PATIENT_STATE is not null or PATIENT_ZIP is not null)")
    })
  )

  map = Map(
    "DATASRC" -> literal("patdem"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "ADDRESS_DATE" -> mapFrom("LAST_UPDATED_DATE"),
    "ADDRESS_TYPE" -> mapFrom("ADDRESSTYPE"),
    "ADDRESS_LINE1" -> mapFrom("PATIENT_ADDRESS1"),
    "ADDRESS_LINE2" -> mapFrom("PATIENT_ADDRESS2"),
    "CITY" -> mapFrom("PATIENT_CITY"),
    "STATE" -> mapFrom("STATE"),
    "ZIPCODE" -> mapFrom("PATIENT_ZIP")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ADDRESS_TYPE"), df("ADDRESS_LINE1"), df("ADDRESS_LINE2"), df("CITY"), df("PATIENT_STATE"), df("ZIPCODE")).orderBy(df("ADDRESS_DATE").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw=1")
  }
}
