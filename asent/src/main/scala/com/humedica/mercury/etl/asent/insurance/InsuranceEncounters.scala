package com.humedica.mercury.etl.asent.insurance

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class InsuranceEncounters(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_encounters")

  columnSelect = Map(
    "as_encounters" -> List("ENCOUNTER_DATE_TIME", "PATIENT_MRN", "APPOINTMENT_LOCATION_ID", "PRIMARY_INS_GROUP_NUMBER", "SECONDARY_INS_GROUP_NUMBER",
      "TERTIARY_INS_GROUP_NUMBER", "PRIMARY_INS_SUBSCRIBER_NUM", "SECONDARY_INS_SUBSCRIBER_NUM", "TERTIARY_INS_SUBSCRIBER_NUM", "ENCOUNTER_ID",
      "PRIMARY_INS_ID", "SECONDARY_INS_ID", "TERTIARY_INS_ID", "PRIMARY_INS_NAME", "SECONDARY_INS_NAME", "TERTIARY_INS_NAME", "LAST_UPDATED_DATE", "FILEID")
  )

  //only one table
  join = noJoin()

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("ENCOUNTER_ID")).orderBy(df("LAST_UPDATED_DATE").desc, df("FILEID").desc)
    val df2 = df.withColumn("rn", row_number.over(groups)).filter("rn = '1'").drop("rn")
    df2.select(df2("ENCOUNTER_DATE_TIME"), df2("PATIENT_MRN"), df2("APPOINTMENT_LOCATION_ID"), df2("ENCOUNTER_ID"),
      expr("stack(3, PRIMARY_INS_ID, PRIMARY_INS_NAME, PRIMARY_INS_GROUP_NUMBER,PRIMARY_INS_SUBSCRIBER_NUM, '1'," +
        "SECONDARY_INS_ID, SECONDARY_INS_NAME, SECONDARY_INS_GROUP_NUMBER, SECONDARY_INS_SUBSCRIBER_NUM, '2'," +
        "TERTIARY_INS_ID, TERTIARY_INS_NAME, TERTIARY_INS_GROUP_NUMBER, TERTIARY_INS_SUBSCRIBER_NUM, '3') as " +
        "(PLANCODE, PLANNAME, GROUPNBR, POLICYNBR, INS_ORDER)"))
  }

  map = Map(
    "DATASRC" -> literal("encounters"),
    "INS_TIMESTAMP" -> mapFrom("ENCOUNTER_DATE_TIME"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "FACILITYID" -> mapFrom("APPOINTMENT_LOCATION_ID"),
    "GROUPNBR" -> mapFrom("GROUPNBR"),
    "POLICYNUMBER" -> mapFrom("POLICYNBR"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PAYORCODE" -> mapFrom("PLANCODE"),
    "PAYORNAME" -> mapFrom("PLANNAME"),
    "PLANCODE" -> mapFrom("PLANCODE"),
    "PLANNAME" -> mapFrom("PLANNAME"),
    "INSURANCEORDER" -> mapFrom("INS_ORDER")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENT_MRN"), df("ENCOUNTER_ID"), df("ENCOUNTER_DATE_TIME"), df("PLANCODE")).orderBy(df("INSURANCEORDER").asc_nulls_last)
    df.withColumn("rn", row_number.over(groups)).filter("PLANCODE is not null AND PLANCODE != '0' AND rn = '1'")
  }
}
