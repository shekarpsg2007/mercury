package com.humedica.mercury.etl.epic_v2.clinicalencounter

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._


/**
  * Auto-generated on 02/01/2017
  */


class ClinicalencounterEncountervisit(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  tables = List("temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable",
    "pat_enc_hsp", "inptbilling_acct", "hsp_acct_mult_drgs", "hsp_acct_sbo",
    "zh_claritydrg","zh_claritydept","zh_beneplan","hh_pat_enc","medadminrec","hsp_atnd_prov","hsp_acct_pat_csn","cdr.map_predicate_values")

  columnSelect = Map(
    "temptable" -> List("ARRIVALTIME", "DISCH_DISP_C", "PAT_ID", "HOSP_ADMSN_TIME", "HOSP_DISCH_TIME", "PAT_ENC_CSN_ID", "ATTND_PROV_ID", "UPDATE_DATE", "ENC_TYPE_C","HSP_ACCOUNT_ID",
      "ADT_PAT_CLASS_C", "ADMIT_SOURCE_C","APPT_STATUS_C", "DEPARTMENT_ID", "EFFECTIVE_DEPT_ID", "ENCOUNTER_PRIMARY_LOCATION", "SERV_AREA_ID", "EXTERNAL_VISIT_ID","CHECKIN_TIME","CONTACT_DATE"),
    "hh_pat_enc" ->List("HH_TYPE_OF_SVC_C", "HH_CONTACT_TYPE_ID", "PAT_ENC_CSN_ID"),
    "pat_enc_hsp" -> List("PAT_ENC_CSN_ID", "INP_ADM_DATE", "ED_DISPOSITION_C", "ADMIT_CONF_STAT_C", "DISCH_CONF_STAT_C", "HSP_ACCOUNT_ID", "DEPARTMENT_ID", "EXP_LEN_OF_STAY", "UPDATE_DATE","BILL_NUM"),
    "hsp_acct_pat_csn" -> List("PAT_ENC_CSN_ID", "HSP_ACCOUNT_ID", "LINE", "PAT_ENC_DATE", "FILEID"),
    "inptbilling_acct" -> List("HSP_ACCOUNT_ID", "HSP_ACCOUNT_NAME", "CODING_STATUS_C", "FILEID", "ACCT_BASECLS_HA_C", "FINAL_DRG_ID", "PRIMARY_PLAN_ID", "GUAR_NAME", "PRIM_ENC_CSN_ID"),
    "medadminrec" -> List("MAR_ENC_CSN", "PAT_ID", "MAR_BILLING_PROV_ID"),
    "zh_claritydrg" -> List("DRG_ID", "DRG_NUMBER"),
    "hsp_atnd_prov" -> List("PAT_ENC_CSN_ID"),
    "zh_claritydept" -> List("DEPARTMENT_ID","REV_LOC_ID"),
    "hsp_acct_mult_drgs" -> List("HSP_ACCOUNT_ID", "DRG_MPI_CODE", "DRG_ROM", "DRG_ID_TYPE_ID", "LINE", "INST_OF_UPDATE", "FILEID"),
    "zh_beneplan" -> List("BENEFIT_PLAN_ID", "BENEFIT_PLAN_NAME"),
    "hsp_acct_sbo" -> List("HSP_ACCOUNT_ID", "SBO_HAR_TYPE_C")
  )

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val list_disch_disp_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),"ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "DISCH_DISP_C")
      val list_enc_type_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "ENC_TYPE_C")
      val fil = df.filter("arrivaltime IS NOT NULL and (DISCH_DISP_C is null or DISCH_DISP_C not in (" + list_disch_disp_c + ")) and " +
        "(ENC_TYPE_C not in (" + list_enc_type_c + ") or ENC_TYPE_C is null)")
      fil.withColumnRenamed("PAT_ID", "PAT_ID_EV").withColumnRenamed("DEPARTMENT_ID", "DEPARTMENT_ID_EV")
           .withColumnRenamed("UPDATE_DATE","UPDATE_DATE_ev")
    }),
    "pat_enc_hsp" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val ev_peh_join  = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "EV_PEH_JOIN", "USE_HSP_ACCT_ID")
      val groups = Window.partitionBy(df1("pat_enc_csn_id")).orderBy(df1("UPDATE_DATE").desc)
      val groups2 = Window.partitionBy(df1("pat_enc_csn_id"), df1("hsp_account_id")).orderBy(df1("UPDATE_DATE").desc) 
      val addColumn = df1.withColumn("rn", when(lit("'N'") === lit(ev_peh_join),row_number.over(groups)).otherwise(row_number.over(groups2)))
      addColumn.filter("rn = 1").withColumnRenamed("DEPARTMENT_ID", "DEPARTMENT_ID_PEH")
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_PEH")
        .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_hsp")
        .drop("rn")
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
      df.dropDuplicates(Seq("MAR_ENC_CSN", "PAT_ID", "MAR_BILLING_PROV_ID")).drop("PAT_ID")
    }),
    "hsp_acct_mult_drgs" -> ((df: DataFrame) => {
      val list_drg_id_type_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "DRG_ID_TYPE_ID")
      val fil = df.filter("DRG_ID_TYPE_ID in (" + list_drg_id_type_id + ")").repartition(1000)
      val groups = Window.partitionBy(fil("HSP_ACCOUNT_ID")).orderBy(fil("LINE"),fil("INST_OF_UPDATE").desc, fil("FILEID").desc)
      fil.withColumn("ranking", row_number.over(groups)).drop("LINE")
        .drop("FILEID")
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_hamd")
    }),
    "hsp_acct_sbo" -> ((df: DataFrame) => {
      val list_har_type_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HSP_ACCOUNT_SBO", "SBO_HAR_TYPE_C")
      df.filter("sbo_har_type_c in (" + list_har_type_c + ")")
        .withColumnRenamed("HSP_ACCOUNT_ID", "HSP_ACCOUNT_ID_SBO")
    })

  )

  join = (dfs: Map[String, DataFrame]) => {
    val ev_peh_join  = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "EV_PEH_JOIN", "USE_HSP_ACCT_ID")
    dfs("temptable")
      .join(dfs("pat_enc_hsp"), when(lit(ev_peh_join) === lit("'N'"), dfs("temptable")("PAT_ENC_CSN_ID") === dfs("pat_enc_hsp")("PAT_ENC_CSN_ID_hsp"))
          .otherwise(dfs("temptable")("PAT_ENC_CSN_ID") === dfs("pat_enc_hsp")("PAT_ENC_CSN_ID_hsp")&&
           dfs("temptable")("HSP_ACCOUNT_ID") === dfs("pat_enc_hsp")("HSP_ACCOUNT_ID_peh")), "left_outer")
      .join(dfs("hh_pat_enc"), Seq("PAT_ENC_CSN_ID"), "left_outer")
      .join(dfs("hsp_acct_pat_csn"), dfs("temptable")("HSP_ACCOUNT_ID") === dfs("hsp_acct_pat_csn")("HSP_ACCOUNT_ID_hapc") &&
                                     dfs("temptable")("PAT_ENC_CSN_ID") === dfs("hsp_acct_pat_csn")("PAT_ENC_CSN_ID_hapc") &&
                                     (dfs("hsp_acct_pat_csn")("hapc_rn") === "1"), "left_outer")
      .join(dfs("hsp_atnd_prov"), Seq("PAT_ENC_CSN_ID"), "left_outer")
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
    val df1 = df.join(t_serv_area, df("SERV_AREA_ID") === t_serv_area("COLUMN_VALUE_tsa"), "left_outer")
    val list_hh_type_of_svc_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HH_PAT_ENC", "HH_TYPE_OF_SVC_C")
    val list_hh_contact_type_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HH_PAT_ENC", "HH_CONTACT_TYPE_ID")
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
    val list_ed_disposition_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "PAT_ENC_HSP", "ED_DISPOSITION_C")
    df1.filter("(HH_TYPE_OF_SVC_C is null OR HH_TYPE_OF_SVC_C NOT IN (" + list_hh_type_of_svc_c + ")) AND " +
      "(HH_CONTACT_TYPE_ID is null OR HH_CONTACT_TYPE_ID not in (" + list_hh_contact_type_id + ")) AND " +
      "(coalesce(CODING_STATUS_C,'X') in (" + list_coding_status_c + ") or 'NO_MPV_MATCHES' in (" + list_coding_status_c + ")) AND " +
      "(ED_DISPOSITION_C is null or ED_DISPOSITION_C not in (" + list_ed_disposition_c +")) AND " +
      "(ADMIT_CONF_STAT_C is null or ADMIT_CONF_STAT_C <> '3') and (DISCH_CONF_STAT_C is null or DISCH_CONF_STAT_C <> '3') AND " +
      "(COLUMN_VALUE_tsa is null or (COLUMN_VALUE_tsa = SERV_AREA_ID))")

  }

  map = Map(
    "DATASRC" -> literal("encountervisit"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PATIENTID" -> mapFrom("PAT_ID_EV"),
    "DISCHARGETIME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(concat_ws("", hour(df("HOSP_DISCH_TIME")), minute(df("HOSP_DISCH_TIME")), second(df("HOSP_DISCH_TIME"))) === "23590", null).otherwise(df("HOSP_DISCH_TIME")))
    }),
    "ADMITTIME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(lit(config(GROUP)) isin ("H430416", "H557454"),df("HOSP_ADMSN_TIME"))
        .when(df("ACCT_BASECLS_HA_C") === "1",coalesce(df("INP_ADM_DATE"),df("HOSP_ADMSN_TIME"))).otherwise(df("HOSP_ADMSN_TIME")))
    }),
    "LOCALADMITSOURCE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ADMIT_SOURCE_C").isNotNull && (df("ADMIT_SOURCE_C") =!= "-1"),concat(lit(config(CLIENT_DS_ID)+"."),df("ADMIT_SOURCE_C")))
        .otherwise(null))
    }),
    "LOCALDISCHARGEDISPOSITION" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DISCH_DISP_C").isNotNull,concat(lit(config(CLIENT_DS_ID)+"."),df("DISCH_DISP_C")))
        .otherwise(null))
    }),
    /*"FACILITYID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(df("DEPARTMENT_ID"), concat_ws("",lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))))
    }),*/
    "FACILITYID" -> ((col: String, df: DataFrame) => {
      val facility_col = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "FACILITYID")
      df.withColumn(col,
        when(lit("'Dept'") === facility_col, df("DEPARTMENT_ID"))
          .when(lit("'Loc'") === facility_col, df("REV_LOC_ID"))
          .when(lit("'coalesceDeptPrimary'") === facility_col, coalesce(df("DEPARTMENT_ID"),
              when(df("ENCOUNTER_PRIMARY_LOCATION").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))).otherwise(null)))
          .when(lit("'coalesceLocDept'") === facility_col, coalesce(df("REV_LOC_ID"),df("DEPARTMENT_ID")))
          .otherwise(coalesce(df("DEPARTMENT_ID"),
            when(df("ENCOUNTER_PRIMARY_LOCATION").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))).otherwise(null))))
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
      df.withColumn(col, when((df("FINAL_DRG_ID") =!= "-1") && (df("DRG_NUMBER") like "MS%"),
        expr("substr(DRG_NUMBER,3,length(DRG_NUMBER))")))
    }),
    "LOCALDRGTYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when((df("FINAL_DRG_ID") =!= "-1") && (df("DRG_NUMBER") like "MS%"), lit("MS")))
      df.withColumn(col, when((df("FINAL_DRG_ID") =!= "-1") && (df("DRG_NUMBER") like "MS%"), lit("MS")))
    }),
    "APRDRG_CD" -> ((col: String, df: DataFrame) => {
      val list_drg_id_type_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "DRG_ID_TYPE_ID")
      df.withColumn(col,
        when(lit("'NO_MPV_MATCHES'").isin(list_drg_id_type_id:_*),
          when((df("FINAL_DRG_ID") =!= "-1") && df("DRG_NUMBER").startsWith("APR"), expr("substr(DRG_NUMBER,4,length(DRG_NUMBER))")).otherwise(null))
        .otherwise(when(df("DRG_ID_TYPE_ID").isin(list_drg_id_type_id: _*), df("DRG_MPI_CODE"))))
    }),
    "APRDRG_ROM" -> ((col: String, df: DataFrame) => {
      val list_drg_id_type_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "DRG_ID_TYPE_ID")
      df.withColumn(col,
        when(lit("'NO_MPV_MATCHES'").isin (list_drg_id_type_id:_*), null)
          .otherwise(when(df("DRG_ID_TYPE_ID").isin(list_drg_id_type_id:_*), df("DRG_ROM")).otherwise(null)))
    }),
    "ELOS" -> mapFrom("EXP_LEN_OF_STAY")


  )

  afterMap = (df: DataFrame) => {
    val df_for_join = df.drop("ACCT_BASECLS_HA_C").drop("GUAR_NAME").drop("CODING_STATUS_C").drop("PRIM_ENC_CSN_ID").drop("acct_rownumber")
    val acct_prim = table("inptbilling_acct")
    val sbo = table("hsp_acct_sbo")
    val sbo_join_type = if (sbo.take(1).isEmpty) "left_outer" else "inner"
    val acct_prim_sbo = acct_prim.join(sbo, acct_prim("HSP_ACCOUNT_ID_IA") === sbo("HSP_ACCOUNT_ID_SBO"), sbo_join_type)
    val list_coding_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "CODING_STATUS_C")
    val acct_prim_select = acct_prim_sbo.select("GUAR_NAME", "ACCT_BASECLS_HA_C", "PRIM_ENC_CSN_ID", "acct_rownumber", "CODING_STATUS_C")
      .filter("coalesce(CODING_STATUS_C,'X') in (" + list_coding_status_c + ") or 'NO_MPV_MATCHES' in (" + list_coding_status_c + ")")
    val list_locPatType_guar = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "INPTBILLING_ACCT", "GUAR_NAME")
      .replace("'","")
    val list_locPatType_plan = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ZH_BENEPLAN", "BENEFIT_PLAN_NAME")
      .replace("'","")
    val joined = df_for_join.join(acct_prim_select, df_for_join("ENCOUNTERID") === acct_prim_select("PRIM_ENC_CSN_ID") && (acct_prim_select("acct_rownumber") === "1"), "left_outer")
    val out = joined.withColumn("LOCALPATIENTTYPE",
      when(joined("GUAR_NAME") like list_locPatType_guar, "HOSPICE_GUAR")
        .when(joined("BENEFIT_PLAN_NAME") like list_locPatType_plan, "HOSPICE_PLAN")
        .when(((joined("ACCT_BASECLS_HA_C") =!= "-1") && joined("ACCT_BASECLS_HA_C").isNotNull) || ((joined("ADT_PAT_CLASS_C") =!= "-1") && joined("ADT_PAT_CLASS_C").isNotNull),
          concat(lit(config(CLIENT_DS_ID)+"."), coalesce(joined("ACCT_BASECLS_HA_C"), lit("0")), lit("."), coalesce(joined("ADT_PAT_CLASS_C"), lit("0"))))
        .when((joined("ENC_TYPE_C") =!= "-1") && joined("ENC_TYPE_C").isNotNull, concat(lit("e"), lit(config(CLIENT_DS_ID)+"."), joined("ENC_TYPE_C"))).otherwise(null))
    val out1 = out.repartition(1000)
    val groups = Window.partitionBy(out1("PAT_ENC_CSN_ID")).orderBy(out1("UPDATE_DATE_ev").desc, out1("FILEID").desc, out1("FILEID_ia"), out1("HSP_ACCOUNT_ID_ia"))
    val addcolumn = out1.withColumn("rn", row_number.over(groups))
    addcolumn.filter("rn = '1'")
  }

  mapExceptions = Map(
    ("H303173_EPIC_DH", "ALT_ENCOUNTERID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ARRIVALTIME") lt to_date(lit("10/01/2015")), df("BILL_NUM")).otherwise(coalesce(df("HSP_ACCOUNT_ID_hapc"),df("HSP_ACCOUNT_ID"),df("HSP_ACCOUNT_ID_PEH"))))
    }),
    ("H430416_EP2", "ARRIVALTIME") -> cascadeFrom(Seq("CHECKIN_TIME", "HOSP_ADMSN_TIME", "CONTACT_DATE")),
    ("H430416_EP2", "ADMITTIME") -> mapFrom("HOSP_ADMSN_TIME"),
    ("H557454_EP2", "ADMITTIME") -> mapFrom("HOSP_ADMSN_TIME"),
    ("H101623_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H135772_EP2_BCM", "ALT_ENCOUNTERID") -> nullValue(),
    ("H135772_EP2_BSL", "ALT_ENCOUNTERID") -> nullValue(),
    ("H171267_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H218562_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H302436_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H303173_EPIC_EL", "ALT_ENCOUNTERID") -> nullValue(),
    ("H328218_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H328218_EP2_MERCY", "ALT_ENCOUNTERID") -> nullValue(),
    ("H340651_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H430416_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H451171_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H557454_EP2", "ALT_ENCOUNTERID") -> mapFrom("EXTERNAL_VISIT_ID"),
    ("H671753_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H704847_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H717614_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H834852_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H969755_EP2", "ALT_ENCOUNTERID") -> nullValue(),
    ("H984197_EP2_V1", "ALT_ENCOUNTERID") -> mapFrom("BILL_NUM")
  )

}

// test
// val e = new ClinicalencounterEncountervisit(cfg) ; val enc = build(e) ; enc.show
