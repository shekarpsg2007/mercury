package com.humedica.mercury.etl.epic_v2.patientidentifier

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 01/27/2017
 */


class PatientidentifierIdentityidhx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("identity_id_hx")
  
  columnSelect = Map(
    "identity_id_hx" -> List("PAT_ID","OLD_PAT_ID")
  )

  beforeJoin = Map(
    "identity_id_hx" -> ((df: DataFrame) => {
      val fil = includeIf("pat_id != old_pat_id and pat_id is not null and old_pat_id is not null")(df)
      val fpiv = unpivot(Seq("PAT_ID", "OLD_PAT_ID"), Seq("PAT_ID", "OLD_PAT_ID"), typeColumnName = "DATA")
      fpiv("PATID", fil)
    }
      ))


  join = noJoin()


  afterJoin = (df: DataFrame) => {
    df.filter("PATID is not null")
  }




  map = Map(
    "DATASRC" -> literal("identity_id_hx"),
    "IDTYPE" -> literal("EMPI"),
    "IDVALUE" -> mapFrom("OLD_PAT_ID"),
    "PATIENTID" -> mapFrom("PATID")
  )
  
  afterMap = (df: DataFrame) => {
    df.drop("PAT_ID","OLD_PAT_ID","DATA").distinct
  }



}