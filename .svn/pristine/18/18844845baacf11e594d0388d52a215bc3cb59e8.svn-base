package com.humedica.mercury.etl.epic_v2.allergies

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**

  * NOTE:  THIS IS AN EXPERIMENTAL SPECIFICATION FOR TESTING SPEC OVERRIDES
 */
class AllergiesAlertH999(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("alert", "cdr.map_predicate_values")

  //de dupe on patientid,localAllergenCode,Encoutnerid,update_date taking record with latest update_dat


  join = noJoin()

  map = Map(

    "DATASRC" -> literal("alert_H999"),  //  <=====   THIS IS AN EXPERIMENT
    "ONSETDATE" -> mapFrom("UPDATE_DATE"),
    "LOCALALLERGENDESC" -> mapFrom("ALERT_DESC"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_CSN"),

    "LOCALALLERGENCD" -> ((col:String, df:DataFrame) => df.withColumn(col, substring(df("ALERT_DESC"),0,100))),

    "LOCALALLERGENTYPE" -> mapFrom("MED_ALERT_TYPE_C",prefix=config(CLIENT_DS_ID)+".a.", nullIf=Seq(null))   // prefix with '[Client_DS_ID].a.'
  )


  afterMap = (df: DataFrame) => {
    val med_alert_type_c_col = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "DISCH_DISP_C")
    val fil = df.filter("(MED_ALERT_TYPE_C = 2 or med_alert_type_c in " + med_alert_type_c_col+") " +
      "and ALERT_DESC is not null and PAT_ID is not null and UPDATE_DATE is not null")
    bestRowPerGroup(List("PAT_ID", "ALERT_DESC", "PAT_CSN"), "UPDATE_DATE")(fil).drop("GROUPID,CLIENT_DS_ID")
  }
}
