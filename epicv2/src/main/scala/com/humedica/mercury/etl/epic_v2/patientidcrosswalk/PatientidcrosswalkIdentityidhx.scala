package com.humedica.mercury.etl.epic_v2.patientidcrosswalk

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 02/01/2017
 */


class PatientidcrosswalkIdentityidhx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("identity_id_hx", "cdr.map_predicate_values")
  
  columnSelect = Map(
    "identity_id_hx" -> List("IDENTITY_NEW_ID", "ID_HX", "ID_CHG_DATE", "ID_TYPE_HX")
  )

  beforeJoin = Map(
    "identity_id_hx" -> ((df: DataFrame) => {
      val mpv = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "IDENTITY_ID_HX", "PAT_ID_XWALK", "IDENTITY_ID_HX", "ID_TYPE_HX")
      df.filter("identity_new_id is not null and id_hx is not null and id_chg_date is not null " +
        "and id_type_hx in (" + mpv + ")")
      })
  )

  join = noJoin()

  map = Map(
    "NEW_ID" -> mapFrom("IDENTITY_NEW_ID"),
    "NEW_ID_TYPE" -> literal("MRN"),
    "OLD_ID" -> mapFrom("ID_HX"),
    "OLD_ID_TYPE" -> literal("MRN"),
    "RECORD_DATE" -> mapFrom("ID_CHG_DATE")
  )
  
  afterMap = (df: DataFrame) => {
    df.distinct
  }

}