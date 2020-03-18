package com.humedica.mercury.etl.hl7_v2.allergies

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * Created by bhenriksen on 1/19/17.
 */
class AllergiesAlert(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("alert", "cdr.map_predicate_values")

  //de dupe on patientid,localAllergenCode,Encoutnerid,update_date taking record with latest update_dat


  join = noJoin()

  map = Map(
    "DATASRC" -> literal("alert"),
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


  //TODO -- exception may be completley handled my map_predicate_valus
  /*
  beforeJoinExceptions = Map(("H477171_EP2_53") -> Map(
     "alert" ->((df: DataFrame) => {
    //var filtered = df.filter("MED_ALERT_TYPE_C == 2")
    val groups = Window.partitionBy(df("PATIENTID"), df("LOCALALLERGENCODE"), df("ENCOUNTERID")).orderBy(to_date(df("UPDATE_DATE")).desc)
    var deduped = df.withColumn("rn", row_number.over(groups)).where(df("rn") === 1).drop("rn")
    includeIf("MED_ALERT_TYPE_C = 13")(deduped)
  })))
  */

}
