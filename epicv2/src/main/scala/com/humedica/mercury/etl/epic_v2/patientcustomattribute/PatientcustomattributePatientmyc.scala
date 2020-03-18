package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._


class PatientcustomattributePatientmyc (config: Map[String, String]) extends EntitySource(config: Map[String, String])
{

  tables = List("patient_myc","zc_mychart_status","cdr.map_predicate_values")

  columnSelect = Map (
    "patient_myc" -> List("MYCHART_STATUS_C","PAT_ID","FILEID"),
    "zc_mychart_status" -> List("MYCHART_STATUS_C","NAME")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("patient_myc")
      .join(dfs("zc_mychart_status"), Seq("MYCHART_STATUS_C"), "inner")
  }


  afterJoin = (df: DataFrame) => {
    val list_myc_include = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT_MYC", "PATIENT_ATTRIBUTE", "PATIENT_MYC", "INCLUSION")
    val groups = Window.partitionBy(df("PAT_ID")).orderBy(df("FILEID").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1 and pat_id is not null and 'Y' = " + list_myc_include + "").drop("rn")
    }

  map = Map(
    "DATASRC" -> literal("patient_myc"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ATTRIBUTE_TYPE_CUI" -> literal("CH002788"),
    "ATTRIBUTE_VALUE" -> mapFrom("NAME")
  )
}

//  test
//  NOTE: NO TEST DATA EXISTS FOR H704
//  val x = new PatientcustomattributePatientmyc(cfg) ; val t = build(x) ; t.show
