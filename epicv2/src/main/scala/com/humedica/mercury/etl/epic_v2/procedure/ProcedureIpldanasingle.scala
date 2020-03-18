package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.{EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Created by bhenriksen on 2/16/17.
 */

//***DO NOT IMPLEMENT -- Not yet implemented in prod***

class ProcedureIpldanasingle(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_ip_flo_gp_data",
    "ip_lda_nasingle")

  //TODO - Create join

  //TODO - Inclusion Criteria:
  //include where Removal_Dttm is not null


  map = Map(
    "DATASRC" -> literal("ip_lda_nasingle"),
    "LOCALCODE" -> mapFrom("FLO_MEAS_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("REMOVAL_DTTM"),
    "LOCALNAME" -> mapFrom("FLO_MEAS_NAME"),
    "ACTUALPROCDATE" -> mapFrom("REMOVAL_INSTANT"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "HOSP_PX_FLAG" -> todo("")      //TODO - to be coded
  )

}