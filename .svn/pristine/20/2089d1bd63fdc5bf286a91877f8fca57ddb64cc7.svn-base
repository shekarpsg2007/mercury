package com.humedica.mercury.etl.crossix.epicrxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 02/01/2017
  */

class ClinicalencounterEncountervisit(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("ENCOUNTERID","DATASRC","FACILITYID","PATIENTID","ADMITTINGPHYSICIAN","ATTENDINGPHYSICIAN","REFERPROVIDERID"
        ,"ADMITTIME","ARRIVALTIME","DISCHARGETIME","LOCALPATIENTTYPE","LOCALACCOUNTSTATUS","LOCALDISCHARGEDISPOSITION","INPATIENTLOCATION"
        ,"PCPID","LOCALFINANCIALCLASS","VISITID","LOCALADMITSOURCE","LOCALDRG","LOCALMDC"
        ,"LOCALDRGTYPE","LOCALENCOUNTERTYPE","HGPID","GRP_MPI","APRDRG_CD","APRDRG_SOI","APRDRG_ROM","TOTALCOST","TOTALCHARGE"
        ,"WASPLANNEDFLG","ELOS","ALT_ENCOUNTERID","INFERRED_PAT_TYPE","ROW_SOURCE", "MODIFIED_DATE", "GRPID1", "CLSID1")

    //cacheMe = true

    tables = List("clinicalencountertemptable",
        "pat_enc_hsp", "inptbilling_acct", "hsp_acct_mult_drgs", "shelf_manifest_entry_wclsid",
        "zh_claritydrg","zh_claritydept","zh_beneplan","hh_pat_enc","medadminrec","hsp_atnd_prov","hsp_acct_pat_csn","cdr.map_predicate_values")



    columnSelect = Map(
        "clinicalencountertemptable" -> List("ARRIVALTIME", "DISCH_DISP_C", "PAT_ID", "HOSP_ADMSN_TIME", "HOSP_DISCH_TIME", "PAT_ENC_CSN_ID", "ATTND_PROV_ID", "UPDATE_DATE", "ENC_TYPE_C","HSP_ACCOUNT_ID",
            "ADT_PAT_CLASS_C", "ADMIT_SOURCE_C","APPT_STATUS_C", "DEPARTMENT_ID", "EFFECTIVE_DEPT_ID", "ENCOUNTER_PRIMARY_LOCATION", "SERV_AREA_ID", "GRPID1", "CLSID1"),
        "hh_pat_enc" ->List("HH_TYPE_OF_SVC_C", "HH_CONTACT_TYPE_ID", "PAT_ENC_CSN_ID","FILE_ID"),
        "pat_enc_hsp" -> List("PAT_ENC_CSN_ID", "INP_ADM_DATE", "ED_DISPOSITION_C", "ADMIT_CONF_STAT_C", "DISCH_CONF_STAT_C", "HSP_ACCOUNT_ID", "DEPARTMENT_ID", "EXP_LEN_OF_STAY","FILE_ID"),
        "hsp_acct_pat_csn" -> List("PAT_ENC_CSN_ID", "HSP_ACCOUNT_ID", "LINE", "PAT_ENC_DATE", "FILEID","FILE_ID"),
        "inptbilling_acct" -> List("HSP_ACCOUNT_ID", "HSP_ACCOUNT_NAME", "CODING_STATUS_C", "FILEID", "ACCT_BASECLS_HA_C", "FINAL_DRG_ID", "PRIMARY_PLAN_ID", "GUAR_NAME", "PRIM_ENC_CSN_ID", "FILE_ID"),
        "medadminrec" -> List("MAR_ENC_CSN", "PAT_ID", "MAR_BILLING_PROV_ID","FILE_ID"),
        "zh_claritydrg" -> List("DRG_ID", "DRG_NUMBER","FILEID"),
        "hsp_atnd_prov" -> List("PAT_ENC_CSN_ID","FILE_ID"),
        "zh_claritydept" -> List("DEPARTMENT_ID","FILEID"),
        "hsp_acct_mult_drgs" -> List("HSP_ACCOUNT_ID", "DRG_MPI_CODE", "DRG_ROM", "DRG_ID_TYPE_ID", "LINE", "INST_OF_UPDATE", "FILEID", "FILE_ID"),
        "zh_beneplan" -> List("BENEFIT_PLAN_ID", "BENEFIT_PLAN_NAME","FILEID"),
        "shelf_manifest_entry_wclsid" -> List("GROUPID", "CLIENT_DATA_SRC_ID", "FILE_ID")

    )


    def mpv(mpvTable: DataFrame, dataSrc:String, entity:String, tableName: String, columnName:String): DataFrame = {
        mpvTable.filter("DATA_SRC='"+dataSrc+"' AND ENTITY='"+entity+"' AND TABLE_NAME='"+tableName+"' AND COLUMN_NAME='"+columnName+"'")
                .select("GROUPID","CLIENT_DS_ID", "COLUMN_VALUE") }

    beforeJoin = Map(
        "clinicalencountertemptable" -> ((df: DataFrame) => {
            val list_disch_disp_c = mpv(table("cdr.map_predicate_values"), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "DISCH_DISP_C")
                    .withColumnRenamed("COLUMN_VALUE", "list_disch_disp_c_val")
                    .withColumnRenamed("GROUPID", "GROUPID_list_disch_disp_c")
                    .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_disch_disp_c")
            val list_enc_type_c = mpv(table("cdr.map_predicate_values"),  "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "ENC_TYPE_C")
                    .withColumnRenamed("COLUMN_VALUE", "list_enc_type_c_val")
                    .withColumnRenamed("GROUPID", "GROUPID_list_enc_type_c")
                    .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_enc_type_c")
            var j1 = df.join(list_disch_disp_c, (df("DISCH_DISP_C")===list_disch_disp_c("list_disch_disp_c_val"))
                                                        .and(df("GRPID1")===list_disch_disp_c("GROUPID_list_disch_disp_c"))
                                                        .and(df("CLSID1")===list_disch_disp_c("CLIENT_DS_ID_list_disch_disp_c")), "left_outer" )
                                                        .filter("arrivaltime IS NOT NULL and (DISCH_DISP_C is null or list_disch_disp_c_val is null)")
                        .join(list_enc_type_c, (df("ENC_TYPE_C")===list_enc_type_c("list_enc_type_c_val"))
                                .and(df("GRPID1")===list_enc_type_c("GROUPID_list_enc_type_c"))
                                .and(df("CLSID1")===list_enc_type_c("CLIENT_DS_ID_list_enc_type_c")), "left_outer" )
                                .filter("arrivaltime IS NOT NULL and (ENC_TYPE_C is null or list_enc_type_c_val is  null)")
            j1.withColumnRenamed("PAT_ID", "PAT_ID_EV").withColumnRenamed("DEPARTMENT_ID", "DEPARTMENT_ID_EV")
        }),
        "pat_enc_hsp" -> ((df: DataFrame) => {
            df.withColumnRenamed("DEPARTMENT_ID", "DEPARTMENT_ID_PEH").withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_PEH")
        }),
        "hsp_acct_pat_csn" -> ((df: DataFrame) => {
            val groups = Window.partitionBy(df("PAT_ENC_CSN_ID")).orderBy(df("HSP_ACCOUNT_ID"), df("FILEID").desc)
            df.withColumn("hapc_rn", row_number.over(groups))
                    .withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_hapc")
                    .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_hapc")
        }),
        "inptbilling_acct" -> ((df: DataFrame) => {
            val fil = df.filter("lower(HSP_ACCOUNT_NAME) NOT LIKE 'zzz%'")
            val groups = Window.partitionBy(fil("HSP_ACCOUNT_ID")).orderBy(fil("FILEID").desc)
            fil.withColumn("acct_rownumber", row_number.over(groups))
                    .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_IA")
                    .withColumnRenamed("FILEID", "FILEID_ia")
        }),
        "medadminrec" -> ((df: DataFrame) => {
            df.dropDuplicates(Seq("MAR_ENC_CSN", "PAT_ID", "MAR_BILLING_PROV_ID")).drop("PAT_ID")
        }),
        "hsp_acct_mult_drgs" -> ((df: DataFrame) => {
            var sme = table("shelf_manifest_entry_wclsid").withColumnRenamed("GROUPID","GRPID_sme").withColumnRenamed("CLIENT_DATA_SRC_ID","CLSID_sme")
            var df0 = df.join(sme, Seq("FILE_ID"), "inner")

            val list_drg_id_type_id = mpv(table("cdr.map_predicate_values"), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "DRG_ID_TYPE_ID")
                    .withColumnRenamed("COLUMN_VALUE", "list_drg_id_type_id_val1")
                    .withColumnRenamed("GROUPID", "GROUPID_list_drg_id_type_id")
                    .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_drg_id_type_id")

            var df1 = df0.join(list_drg_id_type_id,
                (df0("DRG_ID_TYPE_ID")===list_drg_id_type_id("list_drg_id_type_id_val1").cast("String").cast("String"))
                            .and(df0("GRPID_sme")===list_drg_id_type_id("GROUPID_list_drg_id_type_id").cast("String"))
                            .and(df0("CLSID_sme")===list_drg_id_type_id("CLIENT_DS_ID_list_drg_id_type_id").cast("String")), "left_outer" ).drop("GRPID_sme").drop("CLSID_sme")

            val fil = df1.filter("list_drg_id_type_id_val1 is not null")
            val groups = Window.partitionBy(fil("HSP_ACCOUNT_ID")).orderBy(fil("LINE"),fil("INST_OF_UPDATE").desc, fil("FILEID").desc)
            fil.withColumn("ranking", row_number.over(groups)).drop("LINE")
                    .drop("FILEID")
                    .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_hamd")
        })

    )

    join = (dfs: Map[String, DataFrame]) => {
        var sme = dfs("shelf_manifest_entry_wclsid").withColumnRenamed("GROUPID","GRPID1").withColumnRenamed("CLIENT_DATA_SRC_ID","CLSID1")

        var clinicalencountertemptable = dfs("clinicalencountertemptable")
        var clsidcol = clinicalencountertemptable("CLSID1")

        var pat_enc_hsp = dfs("pat_enc_hsp").join(sme, Seq("FILE_ID"), "inner").drop("FILE_ID")
        var hh_pat_enc = dfs("hh_pat_enc").join(sme, Seq("FILE_ID"), "inner").drop("FILE_ID")
        var hsp_acct_pat_csn = dfs("hsp_acct_pat_csn").join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GRPID1", "GRPID1_hapc").withColumnRenamed("CLSID1","CLSID1_hapc").drop("FILE_ID")
        var hsp_atnd_prov = dfs("hsp_atnd_prov").join(sme, Seq("FILE_ID"), "inner").drop("FILE_ID")
        var hsp_acct_mult_drgs = dfs("hsp_acct_mult_drgs").join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GRPID1", "GRPID1_hsmd").withColumnRenamed("CLSID1","CLSID1_hsmd").drop("FILE_ID")
        var medadminrec = dfs("medadminrec").join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GRPID1", "GRPID1_mdr").withColumnRenamed("CLSID1","CLSID1_mdr").drop("FILE_ID")
        var zh_beneplan = dfs("zh_beneplan").withColumn("FILE_ID", dfs("zh_beneplan")("FILEID").cast("String")).join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GRPID1", "GRPID1_zbp").withColumnRenamed("CLSID1","CLSID1_zbp").drop("FILE_ID").drop("FILEID")
        var inptbilling_acct = dfs("inptbilling_acct").join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GRPID1", "GRPID1_ia").withColumnRenamed("CLSID1","CLSID1_ia").drop("FILE_ID")
        var zh_claritydept = dfs("zh_claritydept").withColumn("FILE_ID", dfs("zh_claritydept")("FILEID").cast("String")).join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GRPID1", "GRPID1_zcd").withColumnRenamed("CLSID1","CLSID1_zcd").drop("FILE_ID").drop("FILEID")
        var zh_claritydrg = dfs("zh_claritydrg").withColumn("FILE_ID", dfs("zh_claritydrg")("FILEID").cast("String")).join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GRPID1", "GRPID1_zdg").withColumnRenamed("CLSID1","CLSID1_zdg").drop("FILE_ID").drop("FILEID")


        clinicalencountertemptable
                .join(pat_enc_hsp, Seq("PAT_ENC_CSN_ID", "GRPID1", "CLSID1"), "left_outer")
                .join(hh_pat_enc, Seq("PAT_ENC_CSN_ID", "GRPID1", "CLSID1"), "left_outer")
                .join(hsp_acct_pat_csn, clinicalencountertemptable("GRPID1") === hsp_acct_pat_csn("GRPID1_hapc") and clinicalencountertemptable("CLSID1") === hsp_acct_pat_csn("CLSID1_hapc") and clinicalencountertemptable("PAT_ENC_CSN_ID") === hsp_acct_pat_csn("PAT_ENC_CSN_ID_hapc") && (hsp_acct_pat_csn("hapc_rn") === "1"), "left_outer")
                .join(hsp_atnd_prov, Seq("PAT_ENC_CSN_ID", "GRPID1", "CLSID1"), "left_outer")
                .join(hsp_acct_mult_drgs,
                    clinicalencountertemptable("GRPID1") === hsp_acct_mult_drgs("GRPID1_hsmd") and clinicalencountertemptable("CLSID1") === hsp_acct_mult_drgs("CLSID1_hsmd") and clinicalencountertemptable("HSP_ACCOUNT_ID") === hsp_acct_mult_drgs("HSP_ACCOUNT_ID_hamd") && (hsp_acct_mult_drgs("ranking") === "1"), "left_outer")
                .join(medadminrec,
                    clinicalencountertemptable("GRPID1") === medadminrec("GRPID1_mdr") and clinicalencountertemptable("CLSID1") === medadminrec("CLSID1_mdr") and clinicalencountertemptable("PAT_ENC_CSN_ID") === medadminrec("MAR_ENC_CSN"), "left_outer")
                .join(zh_claritydept,
                    clinicalencountertemptable("GRPID1") === zh_claritydept("GRPID1_zcd") and clinicalencountertemptable("CLSID1") === zh_claritydept("CLSID1_zcd") and
                            coalesce(when(clsidcol === "4665" && clinicalencountertemptable("DEPARTMENT_ID_EV") === "0", null).otherwise(clinicalencountertemptable("DEPARTMENT_ID_EV")),
                                pat_enc_hsp("DEPARTMENT_ID_PEH"), clinicalencountertemptable("EFFECTIVE_DEPT_ID")) === zh_claritydept("DEPARTMENT_ID"), "left_outer")
                .join(inptbilling_acct
                        .join(zh_claritydrg,
                            inptbilling_acct("GRPID1_ia") === zh_claritydrg("GRPID1_zdg") and inptbilling_acct("CLSID1_ia") === zh_claritydrg("CLSID1_zdg") and inptbilling_acct("FINAL_DRG_ID") === zh_claritydrg("DRG_ID"), "left_outer")
                        .join(zh_beneplan,
                            inptbilling_acct("GRPID1_ia") === zh_beneplan("GRPID1_zbp") and inptbilling_acct("CLSID1_ia") === zh_beneplan("CLSID1_zbp") and inptbilling_acct("primary_plan_id") === zh_beneplan("benefit_plan_id"),"left_outer")
                    ,coalesce(when(hsp_acct_pat_csn("HSP_ACCOUNT_ID_hapc") === "-1", null).otherwise(hsp_acct_pat_csn("HSP_ACCOUNT_ID_HAPC"))
                        ,when(clinicalencountertemptable("HSP_ACCOUNT_ID") === "-1", null).otherwise(clinicalencountertemptable("HSP_ACCOUNT_ID"))
                        ,when(pat_enc_hsp("HSP_ACCOUNT_ID_PEH") === "-1", null).otherwise(pat_enc_hsp("HSP_ACCOUNT_ID_PEH"))) === inptbilling_acct("HSP_ACCOUNT_ID_IA")
                            && (inptbilling_acct("acct_rownumber") === "1"), "left_outer")

    }


    afterJoin = (df: DataFrame) => {
        val t_serv_area = mpv(table("cdr.map_predicate_values"), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "SERV_AREA_ID")
                .select("COLUMN_VALUE","GROUPID","CLIENT_DS_ID").withColumnRenamed("COLUMN_VALUE", "COLUMN_VALUE_tsa").withColumnRenamed("GROUPID", "GROUPID_t_serv_area").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_t_serv_area")
        val df1 = df.join(t_serv_area, df("GRPID1") === t_serv_area("GROUPID_t_serv_area") and df("CLSID1") === t_serv_area("CLIENT_DS_ID_t_serv_area") and df("SERV_AREA_ID") === t_serv_area("COLUMN_VALUE_tsa"), "left_outer")
        val list_hh_type_of_svc_c = mpv(table("cdr.map_predicate_values"), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HH_PAT_ENC", "HH_TYPE_OF_SVC_C")
                .withColumnRenamed("COLUMN_VALUE", "list_hh_type_of_svc_c_val")
                .withColumnRenamed("GROUPID", "GROUPID_list_hh_type_of_svc_c")
                .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_hh_type_of_svc_c")
        val list_hh_contact_type_id = mpv(table("cdr.map_predicate_values"),  "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HH_PAT_ENC", "HH_CONTACT_TYPE_ID")
                .withColumnRenamed("COLUMN_VALUE", "list_hh_contact_type_id_val")
                .withColumnRenamed("GROUPID", "GROUPID_list_hh_contact_type_id")
                .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_hh_contact_type_id")
        val list_coding_status_c = mpv(table("cdr.map_predicate_values"), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
                .withColumnRenamed("COLUMN_VALUE", "list_coding_status_c_val")
                .withColumnRenamed("GROUPID", "GROUPID_list_coding_status_c")
                .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_coding_status_c")
        val list_ed_disposition_c = mpv(table("cdr.map_predicate_values"),  "ENCOUNTERVISIT", "CLINICALENCOUNTER", "PAT_ENC_HSP", "ED_DISPOSITION_C")
                .withColumnRenamed("COLUMN_VALUE", "list_ed_disposition_c_val")
                .withColumnRenamed("GROUPID", "GROUPID_list_ed_disposition_c")
                .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_ed_disposition_c")

        var j1 = df1.join(list_hh_type_of_svc_c,
                            (df("HH_TYPE_OF_SVC_C")===list_hh_type_of_svc_c("list_hh_type_of_svc_c_val"))
                             .and(df("GRPID1")===list_hh_type_of_svc_c("GROUPID_list_hh_type_of_svc_c"))
                             .and(df("CLSID1")===list_hh_type_of_svc_c("CLIENT_DS_ID_list_hh_type_of_svc_c")), "left_outer")
                .join(list_hh_contact_type_id,
                    (df("HH_CONTACT_TYPE_ID")===list_hh_contact_type_id("list_hh_contact_type_id_val"))
                            .and(df("GRPID1")===list_hh_contact_type_id("GROUPID_list_hh_contact_type_id"))
                            .and(df("CLSID1")===list_hh_contact_type_id("CLIENT_DS_ID_list_hh_contact_type_id")), "left_outer")
                .join(list_coding_status_c,
                        (coalesce(df("CODING_STATUS_C"),lit("X"))===list_coding_status_c("list_coding_status_c_val"))
                                .and(df("GRPID1")===list_coding_status_c("GROUPID_list_coding_status_c"))
                                .and(df("CLSID1")===list_coding_status_c("CLIENT_DS_ID_list_coding_status_c")), "left_outer")
                .join(list_ed_disposition_c,
                            (df("ED_DISPOSITION_C") ===list_ed_disposition_c("list_ed_disposition_c_val"))
                                    .and(df("GRPID1")===list_ed_disposition_c("GROUPID_list_ed_disposition_c"))
                                    .and(df("CLSID1")===list_ed_disposition_c("CLIENT_DS_ID_list_ed_disposition_c")), "left_outer")

        j1.filter("(HH_TYPE_OF_SVC_C is null OR list_hh_type_of_svc_c_val is null) AND " +
                "(HH_CONTACT_TYPE_ID is null OR list_hh_contact_type_id_val is null) AND " +
                "(list_coding_status_c_val is null) AND " +
                "(ED_DISPOSITION_C is null or list_ed_disposition_c_val is null) AND " +
                "(ADMIT_CONF_STAT_C is null or ADMIT_CONF_STAT_C <> '3') and (DISCH_CONF_STAT_C is null or DISCH_CONF_STAT_C <> '3') AND " +
                "(COLUMN_VALUE_tsa is null or (COLUMN_VALUE_tsa = SERV_AREA_ID))")
    }


    map = Map(
        "DATASRC" -> literal("encountervisit"),
        "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
        "PATIENTID" -> mapFrom("PAT_ID_EV"),
        "DISCHARGETIME" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(concat(lit(""), hour(df("HOSP_DISCH_TIME")), minute(df("HOSP_DISCH_TIME")), second(df("HOSP_DISCH_TIME"))) === "23590", null).otherwise(df("HOSP_DISCH_TIME")))
        }),
        "ADMITTIME" -> ((col: String, df: DataFrame) => {
            df.withColumn(col,when(df("GRPID1").isin ("H430416", "H557454"),df("HOSP_ADMSN_TIME"))
                    .when(df("ACCT_BASECLS_HA_C") === "1",coalesce(df("INP_ADM_DATE"),df("HOSP_ADMSN_TIME"))).otherwise(df("HOSP_ADMSN_TIME")))
        }),
        "LOCALADMITSOURCE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("ADMIT_SOURCE_C").isNotNull && (df("ADMIT_SOURCE_C") !== "-1"),concat(df("CLSID1")+".",df("ADMIT_SOURCE_C")))
                    .otherwise(null))
        }),
        "LOCALDISCHARGEDISPOSITION" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("DISCH_DISP_C").isNotNull,concat(df("CLSID1")+".",df("DISCH_DISP_C")))
                    .otherwise(null))
        }),
        "FACILITYID" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, coalesce(df("DEPARTMENT_ID"), concat(lit(""),concat(df("CLSID1")+"loc.", df("ENCOUNTER_PRIMARY_LOCATION")))))
        }),
        "ALT_ENCOUNTERID" ->  ((col: String, df: DataFrame) => {
            df.withColumn(col, coalesce(when(df("HSP_ACCOUNT_ID_hapc") === "-1", null).otherwise(df("HSP_ACCOUNT_ID_hapc")),
                when(df("HSP_ACCOUNT_ID") === "-1", null).otherwise(df("HSP_ACCOUNT_ID")),
                when(df("HSP_ACCOUNT_ID_PEH") === "-1", null).otherwise(df("HSP_ACCOUNT_ID_PEH"))))
        }),
        "ARRIVALTIME" -> mapFrom("ARRIVALTIME"),
        "LOCALENCOUNTERTYPE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col,  when(df("ADT_PAT_CLASS_C").isNotNull, concat(lit("c."), df("ADT_PAT_CLASS_C")))
                    .otherwise(when(df("ENC_TYPE_C").isNotNull, concat(lit("e."), df("ENC_TYPE_C"))).otherwise(null))) } ),
        "LOCALDRG" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when((df("FINAL_DRG_ID") !== "-1") && (df("DRG_NUMBER") like "MS%"),
                substring(df("DRG_NUMBER"),3,9999)))
        }),
        "LOCALDRGTYPE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when((df("FINAL_DRG_ID") !== "-1") && (df("DRG_NUMBER") like "MS%"), lit("MS")))
            df.withColumn(col, when((df("FINAL_DRG_ID") !== "-1") && (df("DRG_NUMBER") like "MS%"), lit("MS")))
        }),
        "APRDRG_CD" -> ((col: String, df: DataFrame) => {
            val list_drg_id_type_id = mpv(table("cdr.map_predicate_values"),  "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "DRG_ID_TYPE_ID")
                    .withColumnRenamed("COLUMN_VALUE", "list_drg_id_type_id_val2")
                    .withColumnRenamed("GROUPID", "GROUPID_list_drg_id_type_id")
                    .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_drg_id_type_id")

            var j1 = df.join(list_drg_id_type_id, (df("DRG_ID_TYPE_ID")===list_drg_id_type_id("list_drg_id_type_id_val2"))
                    .and(df("GRPID1")===list_drg_id_type_id("GROUPID_list_drg_id_type_id"))
                    .and(df("CLSID1")===list_drg_id_type_id("CLIENT_DS_ID_list_drg_id_type_id")), "left_outer")

            j1.withColumn(col,
                when( j1("list_drg_id_type_id_val2").isNull,
                    when((j1("FINAL_DRG_ID") !== "-1") && (j1("DRG_NUMBER").startsWith("APR")), substring(j1("DRG_NUMBER"), 4, 9999)))
                        .when(j1("list_drg_id_type_id_val2").isNotNull, j1("DRG_MPI_CODE"))
                        .otherwise(null))
        }),
        "APRDRG_ROM" -> ((col: String, df: DataFrame) => {
            val list_drg_id_type_id = mpv(table("cdr.map_predicate_values"),  "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "DRG_ID_TYPE_ID").withColumnRenamed("COLUMN_VALUE", "list_drg_id_type_id_val")
            var j1 = df.join(list_drg_id_type_id, df("DRG_ID_TYPE_ID")===list_drg_id_type_id("list_drg_id_type_id_val"), "left_outer")
            j1.withColumn(col,
                when(j1("list_drg_id_type_id_val").isNull, null)
                        .otherwise(when(j1("list_drg_id_type_id_val").isNotNull, j1("DRG_ROM")).otherwise(null)))
        }),
        "ELOS" -> mapFrom("EXP_LEN_OF_STAY")
    )





    afterMap = (df: DataFrame) => {


        var mpvtbl = table("cdr.map_predicate_values")
        var sme = table("shelf_manifest_entry_wclsid").withColumnRenamed("GROUPID","GRPID_sme").withColumnRenamed("CLIENT_DATA_SRC_ID","CLSID_sme")

        val df_for_join = df.drop("ACCT_BASECLS_HA_C").drop("GUAR_NAME").drop("CODING_STATUS_C").drop("PRIM_ENC_CSN_ID").drop("acct_rownumber")
        var acct_prim = table("inptbilling_acct")
        acct_prim = acct_prim.join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GRPID_sme","GRPID1_ap").withColumnRenamed("CLSID_sme","CLSID1_ap")


        val list_coding_status_c = mpv(mpvtbl, "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
                .withColumnRenamed("COLUMN_VALUE", "list_coding_status_c_val")
                .withColumnRenamed("GROUPID", "GROUPID_list_coding_status_c")
                .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_coding_status_c")

                acct_prim = acct_prim.join(list_coding_status_c,
                    (coalesce(acct_prim("CODING_STATUS_C"),lit("X"))===list_coding_status_c("list_coding_status_c_val")) and
                            acct_prim("GRPID1_ap") === list_coding_status_c("GROUPID_list_coding_status_c") and
                            acct_prim("CLSID1_ap") === list_coding_status_c("CLIENT_DS_ID_list_coding_status_c"), "left_outer")

                        var acct_prim_select = acct_prim.select("GRPID1_ap","CLSID1_ap", "GUAR_NAME", "ACCT_BASECLS_HA_C", "PRIM_ENC_CSN_ID", "acct_rownumber", "CODING_STATUS_C")
                                .filter("list_coding_status_c_val is not null")

                        val list_locPatType_guar = mpv(mpvtbl,"ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "GUAR_NAME")
                                .withColumnRenamed("COLUMN_VALUE", "list_locPatType_guar_val")
                                .withColumnRenamed("GROUPID", "GROUPID_list_locPatType_guar")
                                .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_locPatType_guar")

                            acct_prim_select = acct_prim_select.join(list_locPatType_guar,
                                (acct_prim_select("GUAR_NAME").contains(list_locPatType_guar("list_locPatType_guar_val"))) and
                                        acct_prim_select("GRPID1_ap") === list_locPatType_guar("GROUPID_list_locPatType_guar") and
                                        acct_prim_select("CLSID1_ap") === list_locPatType_guar("CLIENT_DS_ID_list_locPatType_guar"), "left_outer")

                                   val list_locPatType_plan = mpv(mpvtbl, "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ZH_BENEPLAN", "BENEFIT_PLAN_NAME")
                                           .withColumnRenamed("COLUMN_VALUE", "list_locPatType_plan_val")
                                           .withColumnRenamed("GROUPID", "GROUPID_list_locPatType_plan")
                                           .withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_list_locPatType_plan")


                                   var joined = df_for_join.join(acct_prim_select,
                                       df_for_join("ENCOUNTERID") === acct_prim_select("PRIM_ENC_CSN_ID") && (acct_prim_select("acct_rownumber") === "1") and
                                               df_for_join("GRPID1") === acct_prim_select("GRPID1_ap") and
                                               df_for_join("CLSID1") === acct_prim_select("CLSID1_ap"), "left_outer")

                                   joined = joined.join(list_locPatType_plan,
                                       (joined("BENEFIT_PLAN_NAME").contains(list_locPatType_plan("list_locPatType_plan_val")) and
                                               joined("GRPID1_ap") === list_locPatType_plan("GROUPID_list_locPatType_plan") and
                                               joined("CLSID1_ap") === list_locPatType_plan("CLIENT_DS_ID_list_locPatType_plan")), "left_outer")

                                         val out = joined.withColumn("LOCALPATIENTTYPE",
                                             when(joined("list_locPatType_guar_val").isNotNull, "HOSPICE_GUAR")
                                                     .when(joined("list_locPatType_plan_val").isNotNull, "HOSPICE_PLAN")
                                                     .when(((joined("ACCT_BASECLS_HA_C") !== "-1") && joined("ACCT_BASECLS_HA_C").isNotNull) || ((joined("ADT_PAT_CLASS_C") !== "-1") && joined("ADT_PAT_CLASS_C").isNotNull),
                                                         concat(joined("CLSID1")+".", coalesce(joined("ACCT_BASECLS_HA_C"), lit("0")), lit("."), coalesce(joined("ADT_PAT_CLASS_C"), lit("0"))))
                                                     .when((joined("ENC_TYPE_C") !== "-1") && joined("ENC_TYPE_C").isNotNull, concat(lit("e"), joined("CLSID1")+".", joined("ENC_TYPE_C"))).otherwise(null))
                                         val groups = Window.partitionBy(out("PAT_ENC_CSN_ID")).orderBy(out("UPDATE_DATE").desc, out("FILEID").desc, out("FILEID_ia").desc)
                                         val addcolumn = out.withColumn("rn", row_number.over(groups))
                                         addcolumn.filter("rn = '1'")
    }






    //TODO Leaving alt_enctrid_col null for now.

    mapExceptions = Map(
        ("H262866_EP2", "FACILITYID") -> ((col: String, df: DataFrame) => {
            df.withColumn(col, coalesce(df("DEPARTMENT_ID"), concat(lit(""), df("CLSID1"), lit("loc."), df("ENCOUNTER_PRIMARY_LOCATION"))))
        }),
        ("H303173_EPIC_DH", "ALT_ENCOUNTERID") -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(coalesce(df("checkin_time"), df("ed_arrival_time"), df("hosp_admsn_time"), df("Effective_Date_Dt"), df("contact_date")) lt to_date(lit("10/01/2015")), df("bill_num"))
                    .otherwise(coalesce(df("hsp_acct_pat_csn.hsp_accoun_id"), df("encountervisit.hsp_acct_pat_csn"))))
        }),
        ("H328218_EP2_MERCY", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
        ("H340651_EP2", "FACILITYID") -> cascadeFrom(Seq("REV_LOC_ID", "DEPARTMENT_ID")),
        ("H406239_EP2", "FACILITYID") -> mapFrom("DEPARTMENT_ID", nullIf=Seq("-1")),      //TODO - to be coded
        ("H406239_EP2", "LOCALPATIENTTYPE") -> todo("ACCT_BASECLS_HA_C"),      //TODO - to be coded
        ("H406239_EP2", "LOCALPATIENTTYPE") -> todo("ADT_PAT_CLASS_C"),      //TODO - to be coded
        ("H406239_EP2", "LOCALPATIENTTYPE") -> todo("ENC_TYPE_C"),      //TODO - to be coded
        ("H430416_EP2", "ARRIVALTIME") -> cascadeFrom(Seq("CHECKIN_TIME", "HOSP_ADMSN_TIME", "CONTACT_DATE")),
        ("H430416_EP2", "ADMITTIME") -> mapFrom("HOSP_ADMSN_TIME"),
        ("H477171_EP2_53", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
        ("H477171_EP2_53", "LOCALADMITSOURCE") -> mapFrom("ADMIT_SOURCE_C", prefix="2281."),
        ("H477171_EP2_53", "LOCALDISCHARGEDISPOSITION") -> mapFrom("DISCH_DISP_C", prefix="2281."),
        ("H477171_EP2_63", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
        ("H477171_EP2_63", "LOCALADMITSOURCE") -> mapFrom("ADMIT_SOURCE_C", prefix="2201."),
        ("H477171_EP2_63", "LOCALDISCHARGEDISPOSITION") -> mapFrom("DISCH_DISP_C", prefix="2201."),
        ("H477171_EP2_73", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
        ("H477171_EP2_73", "LOCALADMITSOURCE") -> mapFrom("ADMIT_SOURCE_C", prefix="2741."),
        ("H477171_EP2_73", "LOCALDISCHARGEDISPOSITION") -> mapFrom("DISCH_DISP_C", prefix="2741."),
        ("H477171_EP2_73", "LOCALPATIENTTYPE") -> cascadeFrom(Seq("ADT_PAT_CLASS_C", "ENC_TYPE_C")),
        ("H557454_EP2", "ADMITTIME") -> mapFrom("HOSP_ADMSN_TIME"),
        ("H557454_EP2", "ALT_ENCOUNTERID") -> mapFrom("EXTERNAL_VISIT_ID"),
        ("H557454_EP2", "FACILITYID") -> mapFrom("EXTERNAL_VISIT_ID"),
        ("H704847_EP2", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
        ("H704847_EP2", "ALT_ENCOUNTERID") -> nullValue(),
        ("H827927_UPH_EP2", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
        ("H984197_EP2_V1", "ALT_ENCOUNTERID") -> mapFrom("BILL_NUM")
    )

}



//  build(new ClinicalencounterEncountervisit(cfg), allColumns=true).distinct.write.parquet(cfg("EMR_DATA_ROOT")+"/CLINICALENCOUNTERVISIT")
