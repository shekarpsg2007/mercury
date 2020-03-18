package com.humedica.mercury.etl.asent.patientdetail

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class PatientdetailZipcode(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {
  tables = List("as_patdem")

  columnSelect = Map(
    "as_patdem" -> List("PATIENT_MRN", "LAST_UPDATED_DATE", "PATIENT_ZIP", "PATIENT_FIRST_NAME", "PATIENT_LAST_NAME")
  )

  beforeJoin = Map(
    "as_patdem" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_MRN"), df("PATIENT_ZIP")).orderBy(df("LAST_UPDATED_DATE").desc)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1 AND PATIENT_ZIP IS NOT NULL")
    })
  )

  map = Map(
    "DATASRC" -> literal("patdem"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "PATDETAIL_TIMESTAMP" -> mapFrom("LAST_UPDATED_DATE"),
    "PATIENTDETAILTYPE" -> literal("ZIPCODE"),
    "LOCALVALUE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("PATIENT_ZIP"), 1, 5))
    })
  )

  afterMap = (df: DataFrame) => {
    df.filter("length(PATIENT_MRN) > 0 and (not " +
      "((PATIENT_LAST_NAME like '*%' and PATIENT_FIRST_NAME is null)" +
      "or rlike(upper(PATIENT_LAST_NAME), '(^TEST |TEST$|ZZTEST)') " +
      "or rlike(upper(PATIENT_FIRST_NAME), 'TEST')) " +
      "or upper(PATIENT_LAST_NAME) = 'TEST')")
  }
}  