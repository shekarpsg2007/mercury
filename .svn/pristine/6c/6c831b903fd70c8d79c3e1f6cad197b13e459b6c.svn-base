package com.humedica.mercury.etl.epic_v2.observation


import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cdivakaran on 7/21/17.
  */
class ObservationDocinformation(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables=List("doc_information", "zh_doc_info_type", "cdr.zcm_obstype_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "doc_information" -> List("DOC_STAT_C", "RECORD_STATE_C", "DOC_PT_ID", "DOC_INFO_ID", "DOC_EFF_TIME", "DOC_RECV_TIME", "FILEID", "DOC_INFO_TYPE_C", "DOC_CSN"),
    "zh_doc_info_type" -> List("DOC_INFO_TYPE_C", "EXTRACT_DATE", "FILEID"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "OBSCODE")
  )


  beforeJoin = Map(
    "doc_information" -> ((df : DataFrame) => {
      val rec_state_excl = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EXCLUSION", "OBSERVATION", "DOC_INFORMATION", "RECORD_STATE_C")
      val doc_sta_excl = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EXCLUSION", "OBSERVATION", "DOC_INFORMATION", "DOC_STAT_C")
      val doc_sta_incl = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "INCLUSION", "OBSERVATION", "DOC_INFORMATION", "DOC_STAT_C")

      val groups = Window.partitionBy(df("DOC_PT_ID"), df("DOC_INFO_ID")).orderBy(df("DOC_EFF_TIME").desc_nulls_last, df("DOC_RECV_TIME").desc_nulls_last, df("FILEID").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val fil = addColumn.filter("rn = 1 AND DOC_PT_ID is not null").drop("rn").drop("FILEID")
      //see DATA-11185 for requirements
      val tbl1 = fil.filter("'NO_MPV_MATCHES' in (" + doc_sta_incl + ") AND ((DOC_STAT_C not in (" + doc_sta_excl + ") OR DOC_STAT_C is null) OR (RECORD_STATE_C not in (" + rec_state_excl + ") OR RECORD_STATE_C is null))")
      val tbl2 = fil.filter("doc_stat_c in (" + doc_sta_incl + ")")

      tbl1.union(tbl2)
    }),
    "zh_doc_info_type" -> ((df: DataFrame) => {
      val groups1 = Window.partitionBy(df("DOC_INFO_TYPE_C")).orderBy(df("EXTRACT_DATE").desc, df("FILEID").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups1))
      addColumn.filter("rn = 1").drop("rn").drop("FILEID")
    }),
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'doc_information' and obstype <> 'LABRESULT'")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("doc_information")
      .join(dfs("cdr.zcm_obstype_code"), concat(lit(config(CLIENT_DS_ID)+"."), dfs("doc_information")("DOC_INFO_TYPE_C")) === dfs("cdr.zcm_obstype_code")("obscode"), "inner")
      .join(dfs("zh_doc_info_type"), Seq("DOC_INFO_TYPE_C"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("doc_information"),
    "ENCOUNTERID" -> mapFrom("DOC_CSN"),
    "PATIENTID" -> mapFrom("DOC_PT_ID"),
    "OBSDATE" -> cascadeFrom(Seq("DOC_EFF_TIME", "DOC_RECV_TIME")),
    "LOCALCODE" -> mapFrom("DOC_INFO_TYPE_C", nullIf=Seq(null), prefix=config(CLIENT_DS_ID) + "."),
    "LOCALRESULT" -> literal("Document"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS"),
    "STATUSCODE" -> mapFrom("DOC_STAT_C")
  )

  afterMap = (df: DataFrame) => {
      val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"), df("OBSDATE"), df("OBSTYPE")).orderBy(df("DOC_EFF_TIME").desc, df("DOC_RECV_TIME").desc)
      val addColumn = df.withColumn("obs_rn", row_number.over(groups))
      addColumn.filter("obs_rn =1 AND OBSDATE IS NOT NULL AND PATIENTID IS NOT NULL AND LOCALCODE IS NOT NULL")
    }

}
