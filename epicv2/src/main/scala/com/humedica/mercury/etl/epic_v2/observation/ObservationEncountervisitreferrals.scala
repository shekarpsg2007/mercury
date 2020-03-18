package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/**
 * Auto-generated on 02/01/2017
 */


class ObservationEncountervisitreferrals(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable",
                   "zh_claritydept",
                   "cdr.zcm_obstype_code",
                   "cdr.map_predicate_values")


  columnSelect = Map(
    "temptable" -> List("PAT_ID", "PAT_ENC_CSN_ID", "CHECKIN_TIME", "ED_ARRIVAL_TIME","HOSP_ADMSN_TIME", "CONTACT_DATE", "DEPARTMENT_ID", "UPDATE_DATE"
        ,"ENCOUNTER_PRIMARY_LOCATION"),
    "zh_claritydept" -> List("REV_LOC_ID", "DEPARTMENT_ID"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "GROUPID")
  )
  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'encountervisit' and GROUPID='"+config(GROUP)+"'"),
    "temptable" -> ((df: DataFrame) => {
      df.withColumn("OBSDATE", coalesce(df("CHECKIN_TIME"), df("ED_ARRIVAL_TIME"), df("HOSP_ADMSN_TIME"), df("CONTACT_DATE")))
        .withColumn("LOCALCODE", when(df("DEPARTMENT_ID") === "3500101", "BMI_FOLLOWUP").otherwise("DEPRESSION_FOLLOWUP"))
    }),
    "zh_claritydept" -> renameColumn("DEPARTMENT_ID", "DEPARTMENT_ID_zh")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("zh_claritydept"),dfs("temptable")("DEPARTMENT_ID") === dfs("zh_claritydept")("DEPARTMENT_ID_zh"),"inner")
      .join(dfs("cdr.zcm_obstype_code"),dfs("temptable")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("encountervisit"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "FACILITYID" -> ((col: String, df: DataFrame) => {
      val facility_col = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "FACILITYID")
      df.withColumn(col,
        when(lit("'Dept'") === facility_col, df("DEPARTMENT_ID_zh"))
          .when(lit("'Loc'") === facility_col, df("REV_LOC_ID"))
          .when(lit("'coalesceDeptPrimary'") === facility_col, coalesce(df("DEPARTMENT_ID_zh"),
            when(df("ENCOUNTER_PRIMARY_LOCATION").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))).otherwise(null)))
          .when(lit("'coalesceLocDept'") === facility_col, coalesce(df("REV_LOC_ID"),df("DEPARTMENT_ID_zh")))
          .otherwise(coalesce(df("DEPARTMENT_ID_zh"),
            when(df("ENCOUNTER_PRIMARY_LOCATION").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))).otherwise(null))))
    }),
    "OBSRESULT" -> mapFrom("LOCALCODE"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALRESULT" -> literal("FUTURE_VISIT")
  )

  afterMap = (df: DataFrame) => {
    val list_include_enc = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT_REFERRALS", "OBSERVATION", "ENCOUNTERVISIT", "DEPARTMENT_ID")
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"), df("OBSDATE"), df("OBSTYPE")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rownbr", row_number.over(groups))
    addColumn.filter("rownbr =1 and OBSDATE is not null and PATIENTID is not null and DEPARTMENT_ID in (" + list_include_enc + ")")
  }

 }