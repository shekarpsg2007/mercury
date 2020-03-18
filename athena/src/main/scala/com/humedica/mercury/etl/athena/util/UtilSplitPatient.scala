package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.athena.util.UtilSplitTable
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._

class UtilSplitPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true
  columns = List("PATIENT_ID", "PROVIDER_GROUP_ID", "MEDICAL_GROUP_ID", "SPLIT_COLUMN")

  tables = List("patient", "providergroup", "medicalgroup")

  columnSelect = Map(
    "patient" -> List("PATIENT_ID", "PROVIDER_GROUP_ID", "FILEID"),
    "providergroup" -> List("PROVIDER_GROUP_ID", "MEDICAL_GROUP_ID", "FILEID"),
    "medicalgroup" -> List("MEDICAL_GROUP_ID", "FEDERAL_ID_NUMBER", "FILEID")
  )

  beforeJoin = Map(
    "patient" -> ((df: DataFrame) => {
      val splitColValue = new UtilSplitTable(config).columnValue
      val groups = Window.partitionBy(df("PATIENT_ID")).orderBy(df("FILEID").desc)
      val split_clause = if (splitColValue == null) " and 1=2" else " and 1=1"
      df.withColumn("pat_rw", row_number.over(groups))
        .filter("pat_rw =1 " + split_clause)
        .select("PATIENT_ID", "PROVIDER_GROUP_ID")
    }),
    "providergroup" -> ((df: DataFrame) => {
      val splitColValue = new UtilSplitTable(config).columnValue
      val groups = Window.partitionBy(df("PROVIDER_GROUP_ID")).orderBy(df("FILEID").desc)
      val split_clause = if (splitColValue == null) " and 1=2" else " and 1=1"
      df.withColumn("prov_rw", row_number.over(groups))
        .filter("prov_rw =1 " + split_clause)
        .select("PROVIDER_GROUP_ID", "MEDICAL_GROUP_ID")
    }),
    "medicalgroup" -> ((df: DataFrame) => {
      val splitColValue = new UtilSplitTable(config).columnValue
      val groups = Window.partitionBy(df("MEDICAL_GROUP_ID")).orderBy(df("FILEID").desc)
      val split_clause = if (splitColValue == null) " and 1=2" else " and 1=1"
      df.withColumn("med_rw", row_number.over(groups))
        .filter("med_rw=1 " + split_clause)
        .select("MEDICAL_GROUP_ID", "FEDERAL_ID_NUMBER")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patient")
      .join(dfs("providergroup"), Seq("PROVIDER_GROUP_ID"), "inner")
      .join(dfs("medicalgroup"), Seq("MEDICAL_GROUP_ID"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val colName = new UtilSplitTable(config).columnName
    val colValue = new UtilSplitTable(config).columnValue
    val TabName = new UtilSplitTable(config).tableName

    if (TabName == null) {
      df.filter("1=2")
    } else {
      val splitDF = readTable(TabName, config)
      val splitDFGrp = splitDF.groupBy("FEDERAL_ID_NUMBER").agg(min(colName).as("SPLIT_COLUMN"))
      df.join(splitDFGrp, Seq("FEDERAL_ID_NUMBER"), "inner")
        .filter("split_column = '" + colValue + "'")
    }

  }
}

// test
//  val a = new UtilSplitPatient(cfg); val o = build(a); o.show; o.count
