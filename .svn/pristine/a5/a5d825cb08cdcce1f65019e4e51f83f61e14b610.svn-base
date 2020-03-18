package com.humedica.mercury.etl.epic_v2.patientcontact

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._

/**
 * Auto-generated on 01/13/2017
 */


class PatientcontactOthercommunctn(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("other_communctn")

      beforeJoin = Map(
        "other_communctn" -> ((df: DataFrame) => {
          val fil = df.filter("UPDATE_DATE is not null")
          fil.groupBy("PAT_ID", "UPDATE_DATE").pivot("OTHER_COMMUNIC_C", Seq("1", "7", "8")).agg(max("OTHER_COMMUNIC_NUM"))
        })

      )

      join = noJoin()


      map = Map(
        "DATASRC" -> literal("other_communctn"),
        "UPDATE_DT" -> mapFrom("UPDATE_DATE"),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "CELL_PHONE" -> ((col: String, df: DataFrame) => {
          df.withColumn(col, substring(regexp_replace(df("1"), "-", ""), 1, 10))
        }),
        "HOME_PHONE" -> ((col: String, df: DataFrame) => {
          df.withColumn(col, substring(regexp_replace(df("7"), "-", ""), 1, 10))
        }),
        "WORK_PHONE" -> ((col: String, df: DataFrame) => {
          df.withColumn(col, substring(regexp_replace(df("8"), "-", ""), 1, 10))
        })
      )


  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID")).orderBy(df("UPDATE_DT").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1")
  }

 }