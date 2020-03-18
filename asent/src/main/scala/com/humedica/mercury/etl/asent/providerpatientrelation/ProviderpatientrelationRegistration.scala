package com.humedica.mercury.etl.asent.providerpatientrelation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ProviderpatientrelationRegistration(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_patdem")

  columnSelect = Map(
    "as_patdem" -> List("PCP_ID", "PATIENT_MRN", "PCP_TYPE", "LAST_UPDATED_DATE")
  )

  beforeJoin = Map(
    "as_patdem" -> ((df: DataFrame) => {
      val fil = df.filter("(PCP_ID is not null and PCP_ID != 0) and PATIENT_MRN is not null")
      fil.withColumn("PROVIDERID", when(fil("PCP_TYPE") === lit("P"), fil("PCP_ID"))
        .otherwise(when(fil("PCP_TYPE") === lit("R"), concat(fil("PCP_TYPE"), lit("."), fil("PCP_ID"))).otherwise(null)))
    })
  )

  join = noJoin()

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENT_MRN")).orderBy(df("LAST_UPDATED_DATE").asc_nulls_last)
    val df1 = df.withColumn("PREV_PCPID", lag(df("PROVIDERID"), 1).over(groups))
      .withColumn("NEXT_PCPID", lead(df("PROVIDERID"), 1).over(groups))
      .withColumn("PREV_PCP_DT", lag(df("LAST_UPDATED_DATE"), 1).over(groups))
      .withColumn("NEXT_PCP_DT", lead(df("LAST_UPDATED_DATE"), 1).over(groups))
      .withColumn("ENDDATE", concat(date_sub(lead(df("LAST_UPDATED_DATE"), 1).over(groups), 1), substring(lead(df("LAST_UPDATED_DATE"), 1).over(groups), 11, 19)))
      .withColumn("ENDOFYEAR", concat(year(current_date), lit("-12-31 00:00:00")))
      .withColumn("STARTDATE", df("LAST_UPDATED_DATE"))
      .withColumn("BEGINDATE", lit("2005-01-01 00:00:00"))
    df1.withColumn("START_PCPID", when(coalesce(df1("PROVIDERID"), lit("NULL")) =!= coalesce(df1("PREV_PCPID"), lit("NULL")), lit("Y")).otherwise(lit("N")))
      .withColumn("END_PCPID", when(coalesce(df1("PROVIDERID"), lit("NULL")) =!= coalesce(df1("NEXT_PCPID"), lit("NULL")), lit("Y")).otherwise(lit("N")))
  }

  map = Map(
    "DATASRC" -> literal("registration"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "PROVIDERID" -> mapFrom("PROVIDERID"),
    "LOCALRELSHIPCODE" -> literal("PCP"),
    "STARTDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PREV_PCPID").isNull, df("BEGINDATE")).otherwise(df("STARTDATE")))
    }),
    "ENDDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when((df("END_PCPID") === lit("Y") && df("ENDDATE").isNull), null).otherwise(df("ENDDATE")))
    })
  )

  mapExceptions = Map(
    ("H053731_AS ENT", "STARTDATE") -> mapFrom("STARTDATE")
  )

}
