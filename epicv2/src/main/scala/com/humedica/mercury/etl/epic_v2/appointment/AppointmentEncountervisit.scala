package com.humedica.mercury.etl.epic_v2.appointment

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._


class AppointmentEncountervisit (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


    tables = List("encountervisit", "cdr.map_predicate_values")

    beforeJoin = Map(
        "encountervisit" -> ((df: DataFrame) => {
            val list_enc_type_c_incl = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "ENC_TYPE_C_incl")
            val list_appt_prc_id_excl = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "APPT_PRC_ID_excl")
            val list_enc_type_c_excl = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "ENC_TYPE_C_excl")
            val list_appt_prc_id_incl = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "APPOINTMENT", "ENCOUNTERVISIT", "APPT_PRC_ID_incl")
            val fil = includeIf("pat_enc_csn_id is not null and pat_id is not null and appt_time is not null and department_id is not null")(df)
            val addColumn = fil.withColumn("nullColumn",lit("'NO_MPV_MATCHES'"))
            addColumn.withColumn("incude_row", when(addColumn("nullColumn") isin(list_enc_type_c_incl:_*),"y")
              .when((coalesce(fil("ENC_TYPE_C"),lit("X")) isin(list_enc_type_c_incl:_*))
              && (coalesce(fil("APPT_PRC_ID"),lit("X")) isin(list_appt_prc_id_incl:_*))
              && ((!(coalesce(fil("ENC_TYPE_C"),lit("X")) isin(list_enc_type_c_excl:_*))) || (!(coalesce(fil("APPT_PRC_ID"),lit("X")) isin(list_appt_prc_id_excl:_*))))
                , "y"))
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


    afterMap = (df: DataFrame) => {
        val groups = Window.partitionBy(df("APPOINTMENTID")).orderBy(df("UPDATE_DATE").desc)
        val addColumn = df.withColumn("rn", row_number.over(groups))
        addColumn.filter("rn = 1 and APPT_STATUS_C = '1' and incude_row = 'y'").drop("rn")
    }
}