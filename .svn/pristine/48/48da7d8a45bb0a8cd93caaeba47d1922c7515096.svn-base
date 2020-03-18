package com.humedica.mercury.etl.asent.providerpatientrelation

//CUSTOM SOURCE FOR H053 ONLY//

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ProviderpatientrelationRegistration2(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_patdem")

  columnSelect = Map(
    "as_patdem" -> List("HOMECHARTLOCATIONDE", "PATIENT_MRN", "LAST_UPDATED_DATE")
  )

  beforeJoin = Map(
    "as_patdem" -> ((df: DataFrame) => {
      df.filter("HOMECHARTLOCATIONDE is not null and HOMECHARTLOCATIONDE <> '0'")
    })
  )

  join = noJoin()

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENT_MRN")).orderBy(df("LAST_UPDATED_DATE").asc_nulls_last)
    val df1 = df.withColumn("PREV_PCPID", lag(df("HOMECHARTLOCATIONDE"), 1).over(groups))
      .withColumn("NEXT_PCPID", lead(df("HOMECHARTLOCATIONDE"), 1).over(groups))
      .withColumn("PREV_PCP_DT", lag(df("LAST_UPDATED_DATE"), 1).over(groups))
      .withColumn("NEXT_PCP_DT", lead(df("LAST_UPDATED_DATE"), 1).over(groups))
      .withColumn("ENDDATE", concat(date_sub(lead(df("LAST_UPDATED_DATE"), 1).over(groups), 1), substring(lead(df("LAST_UPDATED_DATE"), 1).over(groups), 11, 19)))
      .withColumn("ENDOFYEAR", lit(null))
      .withColumn("STARTDATE", df("LAST_UPDATED_DATE"))
      .withColumn("BEGINDATE", lit("2005-01-01 00:00:00"))
    df1.withColumn("START_PCPID", when(coalesce(df1("HOMECHARTLOCATIONDE"), lit("NULL")) =!= coalesce(df1("PREV_PCPID"), lit("NULL")), lit("Y")).otherwise(lit("N")))
      .withColumn("END_PCPID", when(coalesce(df1("HOMECHARTLOCATIONDE"), lit("NULL")) =!= coalesce(df1("NEXT_PCPID"), lit("NULL")), lit("Y")).otherwise(lit("N")))
  }

  map = Map(
    "DATASRC" -> literal("registration2"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "PROVIDERID" -> mapFrom("HOMECHARTLOCATIONDE", prefix = "allied."),
    "LOCALRELSHIPCODE" -> literal("ALLIED"),
    "ENDDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when((df("END_PCPID") === lit("Y") && df("ENDDATE").isNull), null).otherwise(df("ENDDATE")))
    })
  )

}