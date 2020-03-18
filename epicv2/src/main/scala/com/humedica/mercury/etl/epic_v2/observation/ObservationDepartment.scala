package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._


/**
 * Auto-generated on 01/27/2017
 */


class ObservationDepartment(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable", "zh_claritydept",
    "cdr.zcm_obstype_code", "hh_pat_enc", "hsp_acct_pat_csn", "inptbilling_acct", "cdr.map_predicate_values")

  columnSelect = Map(
    "temptable" -> List("DEPARTMENT_ID", "EFFECTIVE_DEPT_ID", "PAT_ENC_CSN_ID", "ARRIVALTIME", "ENC_TYPE_C", "PAT_ID", "APPT_STATUS_C", "UPDATE_DATE"
        ,"ENCOUNTER_PRIMARY_LOCATION"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "GROUPID"),
    "zh_claritydept" -> List("DEPARTMENT_ID", "REV_LOC_ID"),
    "hh_pat_enc" -> List("PAT_ENC_CSN_ID", "HH_TYPE_OF_SVC_C", "HH_CONTACT_TYPE_ID"),
    "hsp_acct_pat_csn" -> List("PAT_ENC_CSN_ID", "FILEID", "HSP_ACCOUNT_ID"),
    "inptbilling_acct" -> List("HSP_ACCOUNT_ID", "FILEID", "HSP_ACCOUNT_NAME")
  )



  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val list_enc_type_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),"ENCOUNTERVISIT", "OBSERVATION", "ENCOUNTERVISIT", "ENC_TYPE_C")
      df.filter("(ENC_TYPE_C not in (" + list_enc_type_c + ") OR ENC_TYPE_C is null) and PAT_ID is not null and ARRIVALTIME is not null")
    }),
    "inptbilling_acct" -> ((df: DataFrame) => {
      //val fil = df.filter("lower(HSP_ACCOUNT_NAME) not like 'zzz%'")
      //bestRowPerGroup(List("HSP_ACCOUNT_ID"), "FILEID")(fil).drop("FILEID")

      val df1 = df.repartition(1000)
      val fil = df1.filter("lower(HSP_ACCOUNT_NAME) not like 'zzz%'")
      val groups =  Window.partitionBy(fil("HSP_ACCOUNT_ID")).orderBy(fil("FILEID").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 and lower(HSP_ACCOUNT_ID) not like 'zzz%'")
        .drop("rn")
        .withColumnRenamed("PAT_ID","PAT_ID_inpt")
        .withColumnRenamed("FILEID", "FILEID_inpt")
    }),
    "hh_pat_enc" -> ((df: DataFrame) => {
      val list_hh_type_of_svc_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HH_PAT_ENC", "HH_TYPE_OF_SVC_C")
      val list_hh_contact_type_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "HH_PAT_ENC", "HH_CONTACT_TYPE_ID")
      df.filter("(hh_type_of_svc_c is null or hh_type_of_svc_c not in (" + list_hh_type_of_svc_c + ") ) and " +
        "(hh_contact_type_id is null or hh_contact_type_id not in (" + list_hh_contact_type_id + "))")
    }),
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'department' and GROUPID='"+config(GROUP)+"'"),
    "zh_claritydept" -> ((df: DataFrame) => {
      df.withColumnRenamed("DEPARTMENT_ID", "DEPARTMENT_ID_ZH")
    }),
    "hsp_acct_pat_csn" -> ((df: DataFrame) => {
      df.withColumnRenamed("FILEID","FILEID_hspacct")
    })
  )



  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("OBSCODE") === concat(lit(config(CLIENT_DS_ID)+"."), coalesce(dfs("temptable")("DEPARTMENT_ID"), dfs("temptable")("EFFECTIVE_DEPT_ID"))), "inner")
      .join(dfs("zh_claritydept"), dfs("zh_claritydept")("DEPARTMENT_ID_ZH") === coalesce(dfs("temptable")("DEPARTMENT_ID"), dfs("temptable")("EFFECTIVE_DEPT_ID")), "left_outer")
      .join(dfs("hh_pat_enc"), Seq("PAT_ENC_CSN_ID"), "left_outer")
      .join(dfs("hsp_acct_pat_csn")
          .join(dfs("inptbilling_acct"), Seq("HSP_ACCOUNT_ID"), "left_outer")
        ,Seq("PAT_ENC_CSN_ID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PAT_ENC_CSN_ID")).orderBy(df("UPDATE_DATE").desc, df("FILEID_hspacct").desc, df("FILEID_inpt").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }


  map = Map(
    "DATASRC" -> literal("department"),
    "LOCALCODE" -> cascadeFrom(Seq("DEPARTMENT_ID", "EFFECTIVE_DEPT_ID"), prefix = config(CLIENT_DS_ID) + "."),
    "OBSDATE" -> mapFrom("ARRIVALTIME"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "FACILITYID" -> ((col: String, df: DataFrame) => {
      val facility_col = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "FACILITYID")
      df.withColumn(col,
        when(lit("'Dept'") === facility_col, df("DEPARTMENT_ID_ZH"))
          .when(lit("'Loc'") === facility_col, df("REV_LOC_ID"))
          .when(lit("'coalesceDeptPrimary'") === facility_col, coalesce(df("DEPARTMENT_ID_ZH"),
            when(df("ENCOUNTER_PRIMARY_LOCATION").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))).otherwise(null)))
          .when(lit("'coalesceLocDept'") === facility_col, coalesce(df("REV_LOC_ID"),df("DEPARTMENT_ID_ZH")))
          .otherwise(coalesce(df("DEPARTMENT_ID_ZH"),
            when(df("ENCOUNTER_PRIMARY_LOCATION").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))).otherwise(null))))
    }),
    "STATUSCODE" -> mapFrom("APPT_STATUS_C"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALRESULT" -> cascadeFrom(Seq("DEPARTMENT_ID", "EFFECTIVE_DEPT_ID"), prefix = config(CLIENT_DS_ID) + "."),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")

    /* commented per CDRSCALE-701
    ,
    "LOCALRESULT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DEPARTMENT_ID").isNotNull && df("EFFECTIVE_DEPT_ID").isNotNull,
        concat(lit(config(CLIENT_DS_ID) + "."),coalesce(df("DEPARTMENT_ID"), df("EFFECTIVE_DEPT_ID"))))
      )
    })
    */
  )
  
}

// test
// var o = new ObservationDepartment(cfg) ; val od = build(o) ; od.show(false) ; od.count()