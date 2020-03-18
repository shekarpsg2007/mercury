package com.humedica.mercury.etl.epic_v2.patient

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/19/2017
 */


class PatientPatreg(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.patient.PatientTemptable", "cdr.map_predicate_values")


  columnSelect = Map(
    "temptable" -> List("IDENTITY_TYPE_ID", "PATIENTID", "UPDATE_DATE", "DATEOFBIRTH", "DATEOFDEATH", "MEDICALRECORDNUMBER",
      "IDENTITY_TYPE_ID_RNK", "ID_UPDATE_DATE")
  )


  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val list_id_type = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "IDENTITY_ID", "PATIENT", "IDENTITY_ID", "IDENTITY_TYPE_ID")
      df.filter("'NO_MPV_MATCHES' in (" + list_id_type + ") or IDENTITY_TYPE_ID is null or IDENTITY_TYPE_ID in (" + list_id_type + ")")
    })
  )

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"))
      .orderBy(df("UPDATE_DATE").desc_nulls_last)
    val groups1 = Window.partitionBy(df("PATIENTID"))
      .orderBy(when(isnull(df("DATEOFBIRTH")), 0).otherwise(1).desc_nulls_last, df("UPDATE_DATE").desc_nulls_last)
    val groups_mrn = Window.partitionBy(df("PATIENTID"))
      .orderBy(df("ID_UPDATE_DATE").desc_nulls_last)
    val addColumn = df.withColumn("rownbr", row_number.over(groups))
      .withColumn("MEDICALRECORDNUMBER", first("MEDICALRECORDNUMBER").over(groups_mrn))
      .withColumn("DATEOFDEATH", first("DATEOFDEATH").over(groups))
      .withColumn("DATEOFBIRTH", first("DATEOFBIRTH").over(groups1))
    addColumn.filter("rownbr = 1")
  }


  map = Map(
    "DATASRC" -> literal("patreg"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "DATEOFBIRTH" -> mapFrom("DATEOFBIRTH"),
    "DATEOFDEATH" -> mapFrom("DATEOFDEATH"),
    "MEDICALRECORDNUMBER" -> mapFrom("MEDICALRECORDNUMBER")
      )

  afterJoinExceptions = Map(
    "H171267_EP2" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENTID"))
        .orderBy(df("UPDATE_DATE").desc_nulls_last)
      val groups1 = Window.partitionBy(df("PATIENTID"))
        .orderBy(when(isnull(df("DATEOFBIRTH")), 0).otherwise(1).desc, df("UPDATE_DATE").desc_nulls_last)
      val groups_mrn = Window.partitionBy(df("PATIENTID"))
        .orderBy(df("ID_UPDATE_DATE").desc_nulls_last, df("IDENTITY_TYPE_ID_RNK").desc_nulls_last)
      val addColumn = df.withColumn("rownbr", row_number.over(groups))
        .withColumn("MEDICALRECORDNUMBER", first("MEDICALRECORDNUMBER").over(groups_mrn))
        .withColumn("DATEOFDEATH", first("DATEOFDEATH").over(groups))
        .withColumn("DATEOFBIRTH", first("DATEOFBIRTH").over(groups1))
      addColumn.filter("rownbr = 1")
    })
  )

}
