package com.humedica.mercury.etl.asent.immunization


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class ImmunizationImmunizations(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_medication_de",
    "as_immunizations")

  columnSelect = Map(
    "as_zc_medication_de" -> List("GPI_TC3", "ID", "NDC"),
    "as_immunizations" -> List("MRN", "DATE_TO_ADMINISTER", "WHO_ADMINISTERED", "ROUTE_OF_ADMIN",
      "LAST_UPDATED_DATE", "MEDICATION_ID", "IMMUNIZATION_DESCRIPTION", "MEDICATION_NDC", "IMMUNIZATION_ID", "IMMUNIZATION_STATUS")
  )

  beforeJoin = Map(
    "as_immunizations" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("IMMUNIZATION_ID"), df("MRN"), df("MEDICATION_ID")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 AND IMMUNIZATION_STATUS != '5'").drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_immunizations")
      .join(dfs("as_zc_medication_de"), dfs("as_zc_medication_de")("id") === dfs("as_immunizations")("medication_id"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("immunizations"),
    "PATIENTID" -> mapFrom("MRN"),
    "LOCALDEFERREDREASON" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(lower(df("DATE_TO_ADMINISTER")).like("%decline%"), lit("D"))
        .when(lower(df("DATE_TO_ADMINISTER")).like("%refuse%"), lit("D"))
        .when(lower(df("DATE_TO_ADMINISTER")).like("%immune%"), lit("I"))
        .when(lower(df("DATE_TO_ADMINISTER")).like("%disease%"), lit("I"))
        .when(lower(df("DATE_TO_ADMINISTER")).like("%hospital%"), lit("M"))
        .otherwise(null))
    }),
    "LOCALGPI" -> mapFrom("GPI_TC3"),
    "LOCALPATREPORTEDFLG" -> ((col, df) => df.withColumn(col, when(df("WHO_ADMINISTERED").notEqual(0), lit("Y")).otherwise(lit("N")))),
    "LOCALROUTE" -> mapFrom("ROUTE_OF_ADMIN"),
    "ADMINDATE" -> ((col: String, df: DataFrame) => {
      val df1 = df.withColumn("TMP_DATE", when(expr("substr(DATE_TO_ADMINISTER,1,4) rlike '^[A-Za-z]{3}'"), from_unixtime(unix_timestamp(df("DATE_TO_ADMINISTER"), "MMM dd yyyy")))
        .when(expr("SUBSTR(date_to_administer,1,9) rlike '^[0-9]{2}[A-Za-z]{3}[0-9]{4}'"), from_unixtime(unix_timestamp(df("DATE_TO_ADMINISTER"), "ddMMMyyyy")))
        .when(expr("SUBSTR(date_to_administer,1,9) rlike '^[0-9]{1,2}[/][0-9]{1,2}[/][0-9]{2,4}'"), from_unixtime(unix_timestamp(df("DATE_TO_ADMINISTER"), "MM/dd/yy")))
        .when(expr("date_to_administer rlike '^[0-9]{8}'"), from_unixtime(unix_timestamp(df("DATE_TO_ADMINISTER"), "MMddyyyy")))
        .when(expr("SUBSTR(date_to_administer,1,9) rlike '^[0-9 ]{3}'"), from_unixtime(unix_timestamp(df("DATE_TO_ADMINISTER"), "dd MMM yyyy"))))
      df1.withColumn(col, when(df1("TMP_DATE").rlike("^[0-9]{4}\\-[0-9]{2}\\-[0-9]{2} [0-9]{2}[:]{1}[0-9]{2}[:]{1}[0-9]{2}$"), df1("TMP_DATE")).otherwise(lit(null)))
    }),
    "DOCUMENTEDDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, from_unixtime(unix_timestamp(df("LAST_UPDATED_DATE"))))
    }),
    "LOCALIMMUNIZATIONCD" -> mapFrom("MEDICATION_ID"),
    "LOCALIMMUNIZATIONDESC" -> mapFrom("IMMUNIZATION_DESCRIPTION"),
    "LOCALNDC" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(when(length(regexp_replace(df("MEDICATION_NDC"), "0", "")) === 0, null).otherwise(df("MEDICATION_NDC")),
        when(length(regexp_replace(df("NDC"), "0", "")) === 0, null).otherwise(df("NDC"))))
    })
  )
}