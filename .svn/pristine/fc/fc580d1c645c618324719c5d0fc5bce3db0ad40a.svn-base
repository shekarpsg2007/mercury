package com.humedica.mercury.etl.epic_v2.encounterprovider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class EncounterproviderTemptable(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  tables = List("temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable",
    "pat_enc_hsp", "inptbilling_acct", "hsp_acct_mult_drgs",
    "zh_claritydrg","zh_claritydept","zh_beneplan","hh_pat_enc","medadminrec",//"hsp_atnd_prov",
    "hsp_acct_pat_csn","cdr.map_predicate_values")

  columns = List("DATASRC", "ENCOUNTERID", "PATIENTID", "FACILITYID", "ENCOUNTERTIME", "ADMISSION_PROV_ID",
    "ATTND_PROV_ID", "DISCHARGE_PROV_ID", "VISIT_PROV_ID", "REFERRAL_SOURCE_ID", "MED_MAR_BILLING_PROV_ID", "MED_PAT_ID", "MED_MAR_ENC_CSN",
    "RN", "EV_ATT_RN")

  columnSelect = Map(
    "temptable" -> List("ARRIVALTIME", "DISCH_DISP_C", "PAT_ID", "HOSP_ADMSN_TIME", "HOSP_DISCH_TIME", "PAT_ENC_CSN_ID", "ATTND_PROV_ID", "UPDATE_DATE", "ENC_TYPE_C","HSP_ACCOUNT_ID",
      "ADT_PAT_CLASS_C", "ADMIT_SOURCE_C", "APPT_STATUS_C", "DEPARTMENT_ID", "EFFECTIVE_DEPT_ID", "ENCOUNTER_PRIMARY_LOCATION", "SERV_AREA_ID", "ADMISSION_PROV_ID", "DISCHARGE_PROV_ID",
      "VISIT_PROV_ID", "REFERRAL_SOURCE_ID"),
    "hh_pat_enc" ->List("HH_TYPE_OF_SVC_C", "HH_CONTACT_TYPE_ID", "PAT_ENC_CSN_ID"),
    "pat_enc_hsp" -> List("PAT_ENC_CSN_ID", "INP_ADM_DATE", "ED_DISPOSITION_C", "ADMIT_CONF_STAT_C", "DISCH_CONF_STAT_C", "HSP_ACCOUNT_ID", "DEPARTMENT_ID", "EXP_LEN_OF_STAY", "UPDATE_DATE"),
    "hsp_acct_pat_csn" -> List("PAT_ENC_CSN_ID", "HSP_ACCOUNT_ID", "LINE", "PAT_ENC_DATE", "FILEID"),
    "inptbilling_acct" -> List("HSP_ACCOUNT_ID", "HSP_ACCOUNT_NAME", "CODING_STATUS_C", "FILEID", "ACCT_BASECLS_HA_C", "FINAL_DRG_ID", "PRIMARY_PLAN_ID", "GUAR_NAME", "PRIM_ENC_CSN_ID"),
    "medadminrec" -> List("MAR_ENC_CSN", "PAT_ID", "MAR_BILLING_PROV_ID"),
    "zh_claritydrg" -> List("DRG_ID", "DRG_NUMBER"),
    //"hsp_atnd_prov" -> List("PAT_ENC_CSN_ID"),
    "zh_claritydept" -> List("DEPARTMENT_ID"),
    "hsp_acct_mult_drgs" -> List("HSP_ACCOUNT_ID", "DRG_MPI_CODE", "DRG_ROM", "DRG_ID_TYPE_ID", "LINE", "INST_OF_UPDATE", "FILEID"),
    "zh_beneplan" -> List("BENEFIT_PLAN_ID", "BENEFIT_PLAN_NAME")
  )

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val list_disch_disp_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),"ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "DISCH_DISP_C")
      val list_enc_type_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "ENC_TYPE_C")
      val fil = df.filter("arrivaltime IS NOT NULL and (DISCH_DISP_C is null or DISCH_DISP_C not in (" + list_disch_disp_c + ")) and " +
        "(ENC_TYPE_C not in (" + list_enc_type_c + ") or ENC_TYPE_C is null)")
      val fil1 = fil.withColumnRenamed("PAT_ID", "PAT_ID_EV").withColumnRenamed("DEPARTMENT_ID", "DEPARTMENT_ID_EV")
      fil1.repartition(1000)
    }),
    "pat_enc_hsp" -> ((df: DataFrame) => {
      val df1 = df.withColumnRenamed("DEPARTMENT_ID", "DEPARTMENT_ID_PEH").withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_PEH").withColumnRenamed("UPDATE_DATE", "UPDATE_DATE_PEH")
      val groups = Window.partitionBy(df1("PAT_ENC_CSN_ID")).orderBy(df1("HSP_ACCOUNT_ID_PEH"), df1("UPDATE_DATE_PEH").desc)
      val df2 = df1.withColumn("hsp_rn", row_number.over(groups))
      df2.filter("hsp_rn = 1").repartition(1000)
    }),
    "hsp_acct_pat_csn" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("PAT_ENC_CSN_ID")).orderBy(df1("HSP_ACCOUNT_ID"), df1("FILEID").desc)
      df1.withColumn("hapc_rn", row_number.over(groups))
        .withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_hapc")
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_hapc")
    }),
    "inptbilling_acct" -> ((df: DataFrame) => {
      val fil = df.filter("lower(HSP_ACCOUNT_NAME) NOT LIKE 'zzz%'").repartition(1000)
      val groups = Window.partitionBy(fil("HSP_ACCOUNT_ID")).orderBy(fil("FILEID").desc)
      fil.withColumn("acct_rownumber", row_number.over(groups))
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_IA")
        .withColumnRenamed("FILEID", "FILEID_ia")
    }),
    "medadminrec" -> ((df: DataFrame) => {
      df.dropDuplicates(Seq("MAR_ENC_CSN", "PAT_ID", "MAR_BILLING_PROV_ID")).repartition(1000)
    }),
    "hsp_acct_mult_drgs" -> ((df: DataFrame) => {
      val list_drg_id_type_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "DRG_ID_TYPE_ID")
      val fil = df.filter("DRG_ID_TYPE_ID in (" + list_drg_id_type_id + ") or 'NO_MPV_MATCHES' in (" + list_drg_id_type_id + ")").repartition(1000)
      val groups = Window.partitionBy(fil("HSP_ACCOUNT_ID")).orderBy(fil("LINE"),fil("INST_OF_UPDATE").desc, fil("FILEID").desc)
      fil.withColumn("ranking", row_number.over(groups)).drop("LINE")
        .drop("FILEID")
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_hamd")
    }),
    //"hsp_atnd_prov" -> ((df: DataFrame) => {
    //  df.repartition(1000)
    //}),
    "hh_pat_enc" -> ((df: DataFrame) => {
      df.repartition(1000)
    })

  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("pat_enc_hsp"), Seq("PAT_ENC_CSN_ID"), "left_outer")
      .join(dfs("hh_pat_enc"), Seq("PAT_ENC_CSN_ID"), "left_outer")
      .join(dfs("hsp_acct_pat_csn"), dfs("temptable")("PAT_ENC_CSN_ID") === dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hapc") && (dfs("hsp_acct_pat_csn")("hapc_rn") === "1"), "left_outer")
      //.join(dfs("hsp_atnd_prov"), Seq("PAT_ENC_CSN_ID"), "left_outer")
      .join(dfs("hsp_acct_mult_drgs"),
        dfs("temptable")("HSP_ACCOUNT_ID") === dfs("hsp_acct_mult_drgs")("HSP_ACCOUNT_ID_hamd") && (dfs("hsp_acct_mult_drgs")("ranking") === "1"), "left_outer")
      .join(dfs("medadminrec"),
        dfs("temptable")("PAT_ENC_CSN_ID") === dfs("medadminrec")("MAR_ENC_CSN"), "left_outer")
      .join(dfs("zh_claritydept"),
        coalesce(when(lit(config(CLIENT_DS_ID)) === "4665" && dfs("temptable")("DEPARTMENT_ID_EV") === "0", null).otherwise(dfs("temptable")("DEPARTMENT_ID_EV")),
          dfs("pat_enc_hsp")("DEPARTMENT_ID_PEH"), dfs("temptable")("EFFECTIVE_DEPT_ID")) === dfs("zh_claritydept")("DEPARTMENT_ID"), "left_outer")
      .join(dfs("inptbilling_acct")
        .join(dfs("zh_claritydrg"),
          dfs("inptbilling_acct")("FINAL_DRG_ID") === dfs("zh_claritydrg")("DRG_ID"), "left_outer")
        .join(dfs("zh_beneplan"),
          dfs("inptbilling_acct")("primary_plan_id") === dfs("zh_beneplan")("benefit_plan_id"),"left_outer")
        ,coalesce(when(dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_hapc") === "-1", null).otherwise(dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_HAPC"))
          ,when(dfs("temptable")("HSP_ACCOUNT_ID") === "-1", null).otherwise(dfs("temptable")("HSP_ACCOUNT_ID"))
          ,when(dfs("pat_enc_hsp")("HSP_ACCOUNT_ID_PEH") === "-1", null).otherwise(dfs("pat_enc_hsp")("HSP_ACCOUNT_ID_PEH"))) === dfs("inptbilling_acct")("HSP_ACCOUNT_ID_IA")
          && (dfs("inptbilling_acct")("acct_rownumber") === "1"), "left_outer")

  }

  afterJoin = (df: DataFrame) => {
    val t_serv_area = mpv(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "SERV_AREA_ID")
      .select("COLUMN_VALUE").withColumnRenamed("COLUMN_VALUE", "COLUMN_VALUE_tsa")
    val df1 = df.join(t_serv_area, when(df("SERV_AREA_ID").isNull, "'NO_MPV_MATCHES'") === t_serv_area("COLUMN_VALUE_tsa"), "left_outer")
    val list_hh_type_of_svc_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HH_PAT_ENC", "HH_TYPE_OF_SVC_C")
    val list_hh_contact_type_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HH_PAT_ENC", "HH_CONTACT_TYPE_ID")
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
    val list_ed_disposition_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "PAT_ENC_HSP", "ED_DISPOSITION_C")
    val df2 = df1.filter("(HH_TYPE_OF_SVC_C is null OR HH_TYPE_OF_SVC_C NOT IN (" + list_hh_type_of_svc_c + ")) AND " +
      "(HH_CONTACT_TYPE_ID is null OR HH_CONTACT_TYPE_ID not in (" + list_hh_contact_type_id + ")) AND " +
      "(coalesce(CODING_STATUS_C,'X') in (" + list_coding_status_c + ") or 'NO_MPV_MATCHES' in (" + list_coding_status_c + ")) AND " +
      "(ED_DISPOSITION_C is null or ED_DISPOSITION_C not in (" + list_ed_disposition_c + ")) AND " +
      "(ADMIT_CONF_STAT_C is null or ADMIT_CONF_STAT_C <> '3') and (DISCH_CONF_STAT_C is null or DISCH_CONF_STAT_C <> '3') AND " +
      "(COLUMN_VALUE_tsa is null or (COLUMN_VALUE_tsa = SERV_AREA_ID))")
    val groups = Window.partitionBy(df("PAT_ENC_CSN_ID")).orderBy(df("UPDATE_DATE").desc, df("FILEID").desc, df("FILEID_ia").desc, df("HSP_ACCOUNT_ID"))
    val ev_att_groups = Window.partitionBy(df("PAT_ENC_CSN_ID"), df("ATTND_PROV_ID")).orderBy(df("UPDATE_DATE").desc)
    val df3 = df2.withColumn("RN", row_number.over(groups)).withColumn("EV_ATT_RN", row_number.over(ev_att_groups))
    df3.filter("RN = 1 or EV_ATT_RN = 1")
  }

  map = Map(
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID_EV"),
    "FACILITYID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("DEPARTMENT_ID"), concat_ws("",lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))))
    }),
    "ENCOUNTERTIME" -> mapFrom("ARRIVALTIME"),
    "ADMISSION_PROV_ID" -> mapFrom("ADMISSION_PROV_ID"),
    "ATTND_PROV_ID" -> mapFrom("ATTND_PROV_ID"),
    "DISCHARGE_PROV_ID" -> mapFrom("DISCHARGE_PROV_ID"),
    "VISIT_PROV_ID" -> mapFrom("VISIT_PROV_ID"),
    "REFERRAL_SOURCE_ID" -> mapFrom("REFERRAL_SOURCE_ID"),
    "MED_MAR_BILLING_PROV_ID" -> mapFrom("MAR_BILLING_PROV_ID", nullIf = Seq("-1")),
    "MED_PAT_ID" -> mapFrom("PAT_ID", nullIf = Seq("-1")),
    "MED_MAR_ENC_CSN" -> mapFrom("MAR_ENC_CSN", nullIf = Seq("-1"))
  )

  //afterMap = (df: DataFrame) => {
    //val df_for_join = df.drop("ACCT_BASECLS_HA_C").drop("GUAR_NAME").drop("PRIM_ENC_CSN_ID").repartition(1000)
    //val acct_prim = table("inptbilling_acct").repartition(1000)
    //val acct_prim_select = acct_prim.select("GUAR_NAME", "ACCT_BASECLS_HA_C", "PRIM_ENC_CSN_ID")
    //val list_locPatType_guar = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "GUAR_NAME")
    //val list_locPatType_plan = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ZH_BENEPLAN", "BENEFIT_PLAN_NAME")
    //val joined = df_for_join.join(acct_prim_select, df_for_join("ENCOUNTERID") === acct_prim_select("PRIM_ENC_CSN_ID"), "left_outer")
    //val df1 = joined.withColumn("LOCALPATIENTTYPE",
    //  when(joined("GUAR_NAME") like list_locPatType_guar, "HOSPICE_GUAR")
    //    .when(joined("BENEFIT_PLAN_NAME") like list_locPatType_plan, "HOSPICE_PLAN")
    //    .when(((joined("ACCT_BASECLS_HA_C") =!= "-1") && joined("ACCT_BASECLS_HA_C").isNotNull) || ((joined("ADT_PAT_CLASS_C") =!= "-1") && joined("ADT_PAT_CLASS_C").isNotNull),
    //      concat(lit(config(CLIENT_DS_ID)+"."), coalesce(joined("ACCT_BASECLS_HA_C"), lit("0"))+".", coalesce(joined("ADT_PAT_CLASS_C"), lit("0")))).
    //   when((joined("ENC_TYPE_C") =!= "-1") && joined("ENC_TYPE_C").isNotNull, concat(lit("e"), lit(config(CLIENT_DS_ID)), joined("ENC_TYPE_C"))).otherwise(null))
    //val groups = Window.partitionBy(df("PAT_ENC_CSN_ID")).orderBy(df("UPDATE_DATE").desc, df("FILEID").desc, df("FILEID_ia").desc, df("HSP_ACCOUNT_ID"))
    //val ev_att_groups = Window.partitionBy(df("PAT_ENC_CSN_ID"), df("ATTND_PROV_ID")).orderBy(df("UPDATE_DATE").desc)
    //val df2 = df.withColumn("RN", row_number.over(groups)).withColumn("EV_ATT_RN", row_number.over(ev_att_groups))
    //df2.filter("rn = 1 or ev_att_rn = 1")
  //}

  mapExceptions = Map(
    ("H262866_EP2", "FACILITYID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("DEPARTMENT_ID"), concat_ws("",lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))))
    }),
    ("H303173_EPIC_DH", "ALT_ENCOUNTERID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(coalesce(df("checkin_time"), df("ed_arrival_time"), df("hosp_admsn_time"), df("Effective_Date_Dt"), df("contact_date")) lt to_date(lit("10/01/2015")), df("bill_num"))
        .otherwise(coalesce(df("hsp_acct_pat_csn.hsp_accoun_id"), df("encountervisit.hsp_acct_pat_csn"))))
    }),
    ("H328218_EP2_MERCY", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
    ("H340651_EP2", "FACILITYID") -> cascadeFrom(Seq("REV_LOC_ID", "DEPARTMENT_ID")),
    ("H406239_EP2", "FACILITYID") -> todo("DEPARTMENT_ID"),      //TODO - to be coded
    ("H406239_EP2", "LOCALPATIENTTYPE") -> todo("ACCT_BASECLS_HA_C"),      //TODO - to be coded
    ("H406239_EP2", "LOCALPATIENTTYPE") -> todo("ADT_PAT_CLASS_C"),      //TODO - to be coded
    ("H406239_EP2", "LOCALPATIENTTYPE") -> todo("ENC_TYPE_C"),      //TODO - to be coded
    ("H430416_EP2", "ARRIVALTIME") -> cascadeFrom(Seq("CHECKIN_TIME", "HOSP_ADMSN_TIME", "CONTACT_DATE")),
    ("H430416_EP2", "ADMITTIME") -> mapFrom("HOSP_ADMSN_TIME"),
    ("H557454_EP2", "ADMITTIME") -> mapFrom("HOSP_ADMSN_TIME"),
    ("H557454_EP2", "ALT_ENCOUNTERID") -> mapFrom("EXTERNAL_VISIT_ID"),
    ("H557454_EP2", "FACILITYID") -> todo("DEPARTMENT_ID"),      //TODO - to be coded
    ("H557454_EP2", "FACILITYID") -> todo("ENCOUNTER_PRIMARY_LOCATION"),      //TODO - to be coded
    ("H704847_EP2", "APRDRG_CD") -> todo("DRG_MPI_CODE"),      //TODO - to be coded
    ("H704847_EP2", "APRDRG_ROM") -> todo("DRG_ROM"),      //TODO - to be coded
    ("H704847_EP2", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
    ("H704847_EP2", "LOCALPATIENTTYPE") -> cascadeFrom(Seq("ACCT_BASECLS_HA_C", "ADT_PAT_CLASS_C", "ENC_TYPE_C")),
    ("H717614_EP2", "APRDRG_CD") -> todo("DRG_MPI_CODE"),      //TODO - to be coded
    ("H717614_EP2", "APRDRG_ROM") -> todo("DRG_ROM"),      //TODO - to be coded
    ("H827927_UPH_EP2", "FACILITYID") -> mapFrom("DEPARTMENT_ID"),
    ("H984197_EP2_V1", "ALT_ENCOUNTERID") -> mapFrom("BILL_NUM")
  )
}

//test
// val tt = new EncounterproviderTemptable(cfg) ; val ett = build(tt) ; ett.show ; ett.count ; ett.filter("rn = '1' and admission_prov_id is not null and admission_prov_id <> '-1'").count

