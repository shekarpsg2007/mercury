package com.humedica.mercury.etl.epic_v2.encounterprovider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 03/30/2017
 */


class EncounterproviderHsptrmtteam(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hsp_trmt_team",
    "encountervisit",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "hsp_trmt_team" -> List("PROV_ID", "PAT_ID", "PAT_ENC_CSN_ID", "TRTMNT_TEAM_REL_C", "TRTMNT_TM_BEGIN_DT",
      "FILEID"),
    "encountervisit" -> List("PAT_ENC_CSN_ID", "ED_ARRIVAL_TIME", "HOSP_ADMSN_TIME", "EFFECTIVE_DATE_DT",
      "CONTACT_DATE","CHECKIN_TIME","UPDATE_DATE")
  )

  beforeJoin = Map(
    "encountervisit" -> ((df: DataFrame) => {
      val fil = df.filter("PAT_ENC_CSN_ID is not null").repartition(1000)
      val groups = Window.partitionBy(fil("PAT_ENC_CSN_ID")).orderBy(fil("UPDATE_DATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    }
      ),
    "hsp_trmt_team" -> ((df: DataFrame) => {
      val list_trtmnt_team_rel_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "HSP_TRMT_TEAM", "ENCOUNTERPROVIDER", "HSP_TRMT_TEAM", "TRTMNT_TEAM_REL_C")
      val fil = df.filter("TRTMNT_TEAM_REL_C in (" + list_trtmnt_team_rel_c + ")").repartition(1000)
      val groups = Window.partitionBy(fil("PROV_ID"), fil("PAT_ENC_CSN_ID"), fil("TRTMNT_TEAM_REL_C")).orderBy(fil("FILEID").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hsp_trmt_team")
      .join(dfs("encountervisit"), Seq("PAT_ENC_CSN_ID"))}


  map = Map(
    "DATASRC" -> literal("hsp_trmt_team"),
    "ENCOUNTERTIME" -> cascadeFrom(Seq("CHECKIN_TIME", "ED_ARRIVAL_TIME", "HOSP_ADMSN_TIME", "EFFECTIVE_DATE_DT", "CONTACT_DATE",
      "TRTMNT_TM_BEGIN_DT")),
    "PROVIDERID" -> mapFrom("PROV_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PROVIDERROLE" -> mapFrom("TRTMNT_TEAM_REL_C", prefix=config(CLIENT_DS_ID)+".")
  )

}