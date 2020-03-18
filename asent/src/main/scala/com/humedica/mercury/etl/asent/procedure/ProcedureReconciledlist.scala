package com.humedica.mercury.etl.asent.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class ProcedureReconciledlist(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_reconciled_list", "as_patdem", "cdr.map_custom_proc")

  columnSelect = Map(
    "as_reconciled_list" -> List("RECORDEDDTTM", "ENCOUNTERID", "PATIENTID", "FILEID", "ITEMTYPE"),
    "as_patdem" -> List("PATIENT_MRN", "MASTER_PATIENT_ID", "FILEID"),
    "cdr.map_custom_proc" -> List("MAPPEDVALUE", "LOCALCODE", "DATASRC", "GROUPID")
  )

  beforeJoin = Map(
    "as_patdem" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("MASTER_PATIENT_ID"), df("PATIENT_MRN")).orderBy(df("FILEID").desc)
      val df2 = df.withColumn("rn", row_number.over(groups))
      df2.filter("rn = '1'").drop("rn").filter("PATIENT_MRN IS NOT NULL")
    }),
    "as_reconciled_list" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("RECORDEDDTTM")).orderBy(df("FILEID").desc)
      val fil = df.filter("ITEMTYPE = 'ME' and RECORDEDDTTM is not null")
      val df2 = fil.withColumn("rn", row_number.over(groups))
      df2.filter("rn = '1'").drop("rn").withColumn("LC", lit("MEDREC"))
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_reconciled_list")
      .join(dfs("as_patdem"), dfs("as_patdem")("MASTER_PATIENT_ID") === dfs("as_reconciled_list")("PATIENTID"), "inner")
      .join(dfs("cdr.map_custom_proc"), dfs("cdr.map_custom_proc")("LOCALCODE") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_reconciled_list")("LC")) &&
        dfs("cdr.map_custom_proc")("groupid") === lit(config(GROUP)) && dfs("cdr.map_custom_proc")("datasrc") === lit("reconciled_list"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("reconciled_list"),
    "LOCALCODE" -> literal(config(CLIENT_DS_ID) + "." + "MEDREC"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "PROCEDUREDATE" -> mapFromDate("RECORDEDDTTM"),
    "ACTUALPROCDATE" -> mapFromDate("RECORDEDDTTM"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE")
  )
}