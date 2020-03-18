package com.humedica.mercury.etl.asent.patientidentifier

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class PatientidentifierPatdem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_patdem")

  columnSelect = Map(
    "as_patdem" -> List("PATIENT_SSN", "PATIENT_MRN", "PATIENT_NOTE2", "LAST_UPDATED_DATE")
  )

  beforeJoin = Map(
    "as_patdem" -> ((df: DataFrame) => {
      val unpiv = df.select(df("PATIENT_MRN"), df("LAST_UPDATED_DATE"), expr("stack(2, PATIENT_SSN, 'SSN', PATIENT_NOTE2, 'MRN') as (COLUMN_VALUE,COLUMN_NAME)"))
        .filter("length(PATIENT_MRN) > 0 and (not((upper(patient_last_name) = 'TEST') or (Patient_Last_Name like '*%' and patient_first_name is null) or " +
          "upper(PATIENT_LAST_NAME) rlike '(^TEST |TEST$|ZZTEST)' or upper(PATIENT_FIRST_NAME) rlike 'TEST' ))")
      val groups = Window.partitionBy(unpiv("PATIENT_MRN")).orderBy(unpiv("LAST_UPDATED_DATE").desc_nulls_last)
      unpiv.withColumn("RW", row_number.over(groups)).filter("RW = 1 and COLUMN_VALUE is not null")
    })
  )

  afterJoin = (df: DataFrame) => {
    df.filter("COLUMN_NAME = 'SSN'")
  }

  afterJoinExceptions = Map(
    "H285893_AS ENT" -> ((df: DataFrame) => {
      df
    })
  )

  map = Map(
    "DATASRC" -> literal("patdem"),
    "IDTYPE" -> mapFrom("COLUMN_NAME"),
    "IDVALUE" -> mapFrom("COLUMN_VALUE"),
    "IDSUBTYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("COLUMN_NAME") === lit("MRN"), lit("Cerner (MRN)")).otherwise(null))
    }),
    "PATIENTID" -> mapFrom("PATIENT_MRN")
  )

}
