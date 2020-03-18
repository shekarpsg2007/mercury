package com.humedica.mercury.etl.epic_v2.appointmentex

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/4/18.
  */
class AppointmentexEncountervisit(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("encountervisit", "cdr.map_predicate_values")

  beforeJoin = Map(
    "encountervisit" -> ((df: DataFrame) => {
      val list_enc_type_c_incl = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "ENC_TYPE_C_INCL")
      val list_appt_prc_id_excl = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "APPT_PRC_ID_EXCL")
      val list_enc_type_c_excl = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "ENC_TYPE_C_EXCL")
      val list_appt_prc_id_incl = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "APPT_PRC_ID_INCL")
      //val fil = includeIf("pat_enc_csn_id is not null and pat_id is not null and appt_time is not null and department_id is not null")(df)
      val fil = df.filter("PAT_ENC_CSN_ID is not null and PAT_ID is not null and APPT_TIME is not null and DEPARTMENT_ID is not null")
      val addColumn = fil.withColumn("nullColumn",lit("'NO_MPV_MATCHES'"))
      val df2 = addColumn.withColumn("incude_row", when(addColumn("nullColumn").isin(list_enc_type_c_incl:_*),"y")
        .when((coalesce(fil("ENC_TYPE_C"),lit("X")).isin(list_enc_type_c_incl:_*))
          && (coalesce(fil("APPT_PRC_ID"),lit("X")).isin(list_appt_prc_id_incl:_*))
          && ((!(coalesce(fil("ENC_TYPE_C"),lit("X")).isin(list_enc_type_c_excl:_*))) || (!(fil("APPT_PRC_ID").isin(list_appt_prc_id_excl:_*))))
          , "y").otherwise("n"))
      val groups = Window.partitionBy(df2("PAT_ENC_CSN_ID")).orderBy(df2("UPDATE_DATE").desc)
      val addColumn2 = df2.withColumn("rn", row_number.over(groups))
      addColumn2.filter("rn = 1 and (coalesce(APPT_STATUS_C, 'X') <> '1' or incude_row = 'n')").drop("rn")
    }))

  join = noJoin()



  map = Map(
    "DATASRC" -> literal("encountervisit"),
    "APPOINTMENTDATE" -> mapFrom("Appt_Time"),
    "APPOINTMENTID" -> mapFrom("Pat_Enc_Csn_Id"),
    "LOCATIONID" -> mapFrom("Department_Id"),
    "PATIENTID" -> mapFrom("Pat_Id"),
    "PROVIDERID" -> mapFrom("Visit_Prov_Id")

  )

}
