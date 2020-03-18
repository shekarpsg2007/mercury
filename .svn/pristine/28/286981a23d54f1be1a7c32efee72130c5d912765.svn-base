package com.humedica.mercury.etl.epic_v2.tempClinicalencounter

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 03/30/2017
  */


class TempClinicalencounterEncountervisit(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("encountervisit")


  //cacheMe = true


  columns = List("pat_enc_csn_id","pat_id","hosp_admsn_time","hosp_disch_time","admit_source_c","hsp_account_id"
    ,"disch_disp_c","checkin_time","ed_arrival_time","contact_date","department_id","adt_pat_class_c","enc_type_c"
    ,"admission_prov_id","attnd_prov_id","discharge_prov_id","visit_prov_id","referral_source_id","update_date"
    ,"serv_area_id","lmp_date","bp_diastolic","bp_systolic","height","pulse","temperature","weight","respirations","H430_excep_col"
    ,"arrivaltime","encounter_primary_location","effective_dept_id","Appt_Status_C","external_visit_id","row_num_enc_lvl"
    ,"row_num")




  map = Map(
    "department_id" -> mapFrom("department_id", nullIf=Seq("-1")),
    "H430_excep_col" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when((df("enc_type_c") === "50") &&
        ((df("appt_status_c") !== "2") || isnull(df("appt_status_c"))), "1").otherwise("0"))
    }),
    "arrivaltime" -> cascadeFrom(Seq("checkin_time","ed_arrival_time","hosp_admsn_time","Effective_Date_Dt","contact_date"))
  )

  //afterMap = (df: DataFrame) => {
  //val groups = Window.partitionBy(df("pat_enc_csn_id")).orderBy(df("update_date"), df("fileid").desc)
  //val groups1 = Window.partitionBy(df("pat_enc_csn_id"), df("attnd_prov_id")).orderBy(df("update_date"), df("fileid").desc)
  //val fil = df.withColumn("row_num_enc_lvl", row_number.over(groups)).withColumn("row_num", row_number.over(groups1))
  //  includeIf("(appt_status_c is null or appt_status_c not in ('3','4','5')) and (enc_type_c is null or enc_type_c not in ('5')) and (pat_enc_csn_id is not null and pat_enc_csn_id <> '-1') and (pat_id is not null and pat_id <> '-1')")(df)
  // tables.write.parquet("/mapr/optum-dev/user/cdivakaran/delta_testing/TEMPCLINICALENCOUNTER/")
  //}

}