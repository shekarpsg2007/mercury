package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by abendiganavale on 8/14/18.
  */
class ProcedurePatencconcept(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("pat_enc_concept", "cdr.map_custom_proc","cdr.map_predicate_values")

  columnSelect = Map(
    "pat_enc_concept" -> List("PAT_ID", "CONCEPT_ID", "CONCEPT_VALUE", "CUR_VALUE_DATETIME", "LINE", "PAT_ENC_CSN_ID"),
    "cdr.map_custom_proc" -> List("LOCALCODE", "DATASRC", "GROUPID","MAPPEDVALUE")
  )

  beforeJoin = Map(
    "pat_enc_concept" -> ((df: DataFrame) => {
      df.filter("PAT_ID is not null and CONCEPT_ID is not null and CUR_VALUE_DATETIME is not null")
    }),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'pat_enc_concept'")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("pat_enc_concept")
      .join(dfs("cdr.map_custom_proc"),
        dfs("cdr.map_custom_proc")("LOCALCODE") === concat(lit(config(CLIENT_DS_ID) + "."), dfs("pat_enc_concept")("CONCEPT_ID")), "inner")
  }

  map = Map(
    "DATASRC" -> literal("pat_enc_concept"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("CUR_VALUE_DATETIME"),
    "LOCALCODE" -> ((col: String, df: DataFrame) => {
      val list_concept_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "LOCALCODE", "PROCEDUREDO", "PAT_ENC_CONCEPT", "CONCEPT_ID")
      df.withColumn(col, concat_ws("", lit(config(CLIENT_DS_ID) + "."),
                                       when(df("CONCEPT_ID").isin(list_concept_id), concat_ws("",df("CONCEPT_ID"), lit("_"), df("CONCEPT_VALUE")))
                                      .otherwise(df("CONCEPT_ID"))))
    }),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "SEQUENCE" -> mapFrom("LINE")
  )

  afterMap = (df:DataFrame) => {
    df.distinct()
  }

}
