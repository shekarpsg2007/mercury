package com.humedica.mercury.etl.epic_v2.patientcontact

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/13/2017
 */


class PatientcontactPatreg(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patreg")


  beforeJoin =
    Map("patreg" -> ((df: DataFrame) => {
      val fil = df.filter("UPDATE_DATE is not null and PAT_ID is not null and coalesce(email_address,home_phone,work_phone) is not null")
      val groups = Window.partitionBy(fil("PAT_ID")).orderBy(fil("UPDATE_DATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1")
    })
    )

  join = noJoin()


  map = Map(
    "DATASRC" -> literal("patreg"),
    "UPDATE_DT" -> mapFrom("UPDATE_DATE"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "HOME_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(regexp_replace(df("HOME_PHONE"),"-",""),1,10))}),
    "PERSONAL_EMAIL" -> mapFrom("EMAIL_ADDRESS"),
    "WORK_PHONE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(regexp_replace(df("WORK_PHONE"),"-",""),1,10))})
  )

}