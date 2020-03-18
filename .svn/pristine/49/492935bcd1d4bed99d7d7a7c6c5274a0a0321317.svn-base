package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by swallis on 6/6/2018
 */


class ProcedurePatencrsn(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("pat_enc_rsn", "cdr.map_custom_proc")

  columnSelect = Map(
        "pat_enc_rsn" -> List("ENC_REASON_ID", "PAT_ENC_CSN_ID", "CONTACT_DATE", "ENC_REASON_NAME", "PAT_ID", "UPDATE_DATE"),
        "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID","MAPPEDVALUE")
  )

  beforeJoin = Map(
    "pat_enc_rsn" -> ((df: DataFrame) => {
      val fil = df.filter("PAT_ID is not null and ENC_REASON_ID is not null and CONTACT_DATE is not null")
      val groups = Window.partitionBy(fil("PAT_ID"), fil("PAT_ENC_CSN_ID"), fil("ENC_REASON_ID")).orderBy(fil("UPDATE_DATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn=1").drop("rn")
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'pat_enc_rsn'").drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("pat_enc_rsn")
      .join(dfs("cdr.map_custom_proc"),
        dfs("cdr.map_custom_proc")("LOCALCODE") === concat(lit(config(CLIENT_DS_ID) + "."), dfs("pat_enc_rsn")("ENC_REASON_ID")), "inner")
  }

  map = Map(
    "DATASRC" -> literal("pat_enc_rsn"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("CONTACT_DATE"),
    "ACTUALPROCDATE" -> mapFrom("CONTACT_DATE"),
    "LOCALCODE" -> mapFrom("ENC_REASON_ID", prefix = config(CLIENT_DS_ID) + "."),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "LOCALNAME" -> mapFrom("ENC_REASON_NAME")
  )


}