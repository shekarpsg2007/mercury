package com.humedica.mercury.etl.crossix.epicrxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Auto-generated on 03/30/2017
  */


class ClinicalencounterTemptable(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    tables = List("encountervisit","shelf_manifest_entry_wclsid")

    //cacheMe = true


    columns = List("PAT_ENC_CSN_ID","PAT_ID","HOSP_ADMSN_TIME","HOSP_DISCH_TIME","ADMIT_SOURCE_C","HSP_ACCOUNT_ID","DISCH_DISP_C","CHECKIN_TIME"
        ,"ED_ARRIVAL_TIME","CONTACT_DATE","DEPARTMENT_ID","ADT_PAT_CLASS_C","ENC_TYPE_C","ADMISSION_PROV_ID","ATTND_PROV_ID"
        ,"DISCHARGE_PROV_ID","VISIT_PROV_ID","REFERRAL_SOURCE_ID","UPDATE_DATE","SERV_AREA_ID","LMP_DATE"
        ,"BP_DIASTOLIC","BP_SYSTOLIC","HEIGHT","PULSE","TEMPERATURE","WEIGHT","RESPIRATIONS","H430_EXCEP_COL","ARRIVALTIME","ENCOUNTER_PRIMARY_LOCATION"
        ,"EFFECTIVE_DEPT_ID","APPT_STATUS_C","EXTERNAL_VISIT_ID","ROW_NUM_ENC_LVL","ROW_NUM", "APPT_PRC_ID", "GRPID1", "CLSID1")


    join = (dfs: Map[String, DataFrame]) => {
        var sme = dfs("shelf_manifest_entry_wclsid").withColumnRenamed("GROUPID","GRPID1").withColumnRenamed("CLIENT_DATA_SRC_ID","CLSID1")
        dfs("encountervisit").join(sme, Seq("FILE_ID"), "inner")
    }

    map = Map(
        "DEPARTMENT_ID" -> mapFrom("department_id", nullIf=Seq("-1")),
        "H430_EXCEP_COL" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when((df("enc_type_c") === "50") &&
                    ((df("appt_status_c") !== "2") || isnull(df("appt_status_c"))), "1").otherwise("0"))
        }),
        "ARRIVALTIME" -> cascadeFrom(Seq("checkin_time","ed_arrival_time","hosp_admsn_time","Effective_Date_Dt","contact_date")),
    "GRPID1" -> mapFrom("GRPID1"),
    "CLSID1" -> mapFrom("CLSID1")
    )

    afterMap = (df: DataFrame) => {
        val groups = Window.partitionBy(df("pat_enc_csn_id")).orderBy(df("update_date").desc, df("fileid").desc)
        val groups1 = Window.partitionBy(df("pat_enc_csn_id"), df("attnd_prov_id")).orderBy(df("update_date").desc, df("fileid").desc)
        val fil = df.withColumn("row_num_enc_lvl", row_number.over(groups)).withColumn("row_num", row_number.over(groups1))
        includeIf("row_num = 1 and (appt_status_c is null or appt_status_c not in ('3','4','5')) and (enc_type_c is null or enc_type_c not in ('5')) and (pat_enc_csn_id is not null and pat_enc_csn_id <> '-1') and (pat_id is not null and pat_id <> '-1')")(fil)
    }

}



//   build(new ClinicalencounterTemptable(cfg), allColumns=true).distinct.write.parquet(cfg("EMR_DATA_ROOT")+"/CLINICALENCOUNTERTEMPTABLE")
