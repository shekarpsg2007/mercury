package com.humedica.mercury.etl.asent.patientcontact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class PatientcontactPatdem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_patdem")

  columnSelect = Map(
    "as_patdem" -> List("LAST_UPDATED_DATE", "PATIENT_MRN", "CELL_PHONE_AREA_CODE", "CELL_PHONE_EXCHANGE",
      "CELL_PHONE_LAST_4", "HOME_PHONE_AREA_CODE", "HOME_PHONE_EXCHANGE", "HOME_PHONE_LAST_4", "PATIENT_EMAIL",
      "WORK_PHONE_AREA_CODE", "WORK_PHONE_EXCHANGE", "WORK_PHONE_LAST_4")
  )

  beforeJoin = Map(
    "as_patdem" -> ((df: DataFrame) => {
      val fil = df.filter("PATIENT_MRN is not null and LAST_UPDATED_DATE is not null and (coalesce(CELL_PHONE_AREA_CODE,HOME_PHONE_AREA_CODE,WORK_PHONE_AREA_CODE) is not null or " +
        "instr(PATIENT_EMAIL,'@') >1)")
      val groups = Window.partitionBy(fil("PATIENT_MRN")).orderBy(fil("LAST_UPDATED_DATE").desc_nulls_last)
      fil.withColumn("rw", row_number.over(groups)).filter("rw=1")
    })
  )

  map = Map(
    "DATASRC" -> literal("patdem"),
    "UPDATE_DT" -> mapFrom("LAST_UPDATED_DATE"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "WORK_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("WORK_PHONE_AREA_CODE").isNull || df("WORK_PHONE_EXCHANGE").isNull || df("WORK_PHONE_LAST_4").isNull, null)
        .otherwise(concat_ws("-", df("WORK_PHONE_AREA_CODE"), lpad(df("WORK_PHONE_EXCHANGE"), 3, "0"), lpad(df("WORK_PHONE_LAST_4"), 4, "0"))))
    }),
    "CELL_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("CELL_PHONE_AREA_CODE").isNull || df("CELL_PHONE_EXCHANGE").isNull || df("CELL_PHONE_LAST_4").isNull, null)
        .otherwise(concat_ws("-", df("CELL_PHONE_AREA_CODE"), lpad(df("CELL_PHONE_EXCHANGE"), 3, "0"), lpad(df("CELL_PHONE_LAST_4"), 4, "0"))))
    }),
    "HOME_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("HOME_PHONE_AREA_CODE").isNull || df("HOME_PHONE_EXCHANGE").isNull || df("HOME_PHONE_LAST_4").isNull, null)
        .otherwise(concat_ws("-", df("HOME_PHONE_AREA_CODE"), lpad(df("HOME_PHONE_EXCHANGE"), 3, "0"), lpad(df("HOME_PHONE_LAST_4"), 4, "0"))))
    }),
    "PERSONAL_EMAIL" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PATIENT_EMAIL").contains("@"), df("PATIENT_EMAIL")).otherwise(null))
    })
  )

}
