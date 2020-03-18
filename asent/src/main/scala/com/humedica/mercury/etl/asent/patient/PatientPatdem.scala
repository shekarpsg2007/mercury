package com.humedica.mercury.etl.asent.patient

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class PatientPatdem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_patdem")

  columnSelect = Map(
    "as_patdem" -> List("PATIENT_MRN", "PATIENT_DATE_OF_BIRTH", "PATIENT_FIRST_NAME", "GENDER_ID", "INACTIVE_FLAG",
      "PATIENT_LAST_NAME", "LAST_UPDATED_DATE")
  )

  beforeJoin = Map(
    "as_patdem" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENT_MRN"))
        .orderBy(coalesce(df("LAST_UPDATED_DATE"), df("PATIENT_DATE_OF_BIRTH")).desc_nulls_last)
      val fil = df.filter("PATIENT_MRN != '0' and (not((upper(patient_last_name) = 'TEST') or " +
        "(Patient_Last_Name like '*%' and patient_first_name is null) or " +
        "upper(PATIENT_LAST_NAME) rlike '(^TEST |TEST$|ZZTEST)' or upper(PATIENT_FIRST_NAME) rlike 'TEST')" +
        "or upper(PATIENT_LAST_NAME) = 'TEST')")
      fil.withColumn("rn", row_number.over(groups))
        .filter("rn = 1 AND PATIENT_MRN IS NOT NULL")
    })
  )

  map = Map(
    "DATASRC" -> literal("patdem"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "MEDICALRECORDNUMBER" -> mapFrom("PATIENT_MRN"),
    "DATEOFBIRTH" -> mapFrom("PATIENT_DATE_OF_BIRTH"),
    "INACTIVE_FLAG" -> mapFrom("INACTIVE_FLAG")
  )

  mapExceptions = Map(
    ("H285893_AS ENT", "INACTIVE_FLAG") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PATIENT_MRN").rlike("^[^0]"), lit("Y")).otherwise(df("INACTIVE_FLAG")))
    })
  )
}
