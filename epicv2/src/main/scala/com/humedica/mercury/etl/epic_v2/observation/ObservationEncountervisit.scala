package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._


class ObservationEncountervisit(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable", "zh_claritydept", "cdr.zcm_obstype_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "temptable" -> List("HEIGHT", "LMP_DATE", "APPT_STATUS_C", "ENC_TYPE_C", "PAT_ENC_CSN_ID", "PAT_ID", "CHECKIN_TIME", "ED_ARRIVAL_TIME", "HOSP_ADMSN_TIME", "CONTACT_DATE", "UPDATE_DATE",
      "DEPARTMENT_ID", "ENCOUNTER_PRIMARY_LOCATION", "BP_DIASTOLIC", "BP_SYSTOLIC", "PULSE", "TEMPERATURE", "WEIGHT", "RESPIRATIONS"),
    "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "LOCALUNIT", "GROUPID", "DATATYPE"),
    "zh_claritydept" -> List("DEPARTMENT_ID", "REV_LOC_ID")
  )

  beforeJoin = Map(
    "temptable" -> ((df: DataFrame) => {
      val list_enc_type_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "TEMP_ENCOUNTER", "ENC_TYPE_C")
      val fil = df.filter("ENC_TYPE_C <> '5' and (('NO_MPV_MATCHES') in (" + list_enc_type_c + ") or (('NO_MPV_MATCHES') not in (" + list_enc_type_c + ") and ENC_TYPE_C not in (" + list_enc_type_c + ")))")
      val height1 = fil.withColumn("cal_height", regexp_replace(regexp_extract(fil("HEIGHT"), "([^ ]+)", 0),"'","").multiply(12) + regexp_replace(regexp_extract(fil("HEIGHT"), "[ ]([^ ]+)", 0),"\"",""))
        .withColumnRenamed("LMP_DATE", "LMP_DT")
      val fpiv = unpivot(
        Seq("BP_DIASTOLIC", "BP_SYSTOLIC", "CAL_HEIGHT", "PULSE", "TEMPERATURE", "WEIGHT", "RESPIRATIONS", "LMP_DT"),
        Seq("BP_DIASTOLIC", "BP_SYSTOLIC", "HEIGHT", "PULSE", "TEMPERATURE", "WEIGHT", "RESPIRATIONS", "LMP_DATE"), typeColumnName = "LOCALCODE")
      fpiv("LOCALRESULT", height1)
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'encountervisit' and GROUPID = '" + config(GROUP) + "'").drop("GROUPID")
    }),
    "zh_claritydept" -> renameColumn("DEPARTMENT_ID", "DEPARTMENT_ID_zh")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("zh_claritydept"),dfs("temptable")("DEPARTMENT_ID") === dfs("zh_claritydept")("DEPARTMENT_ID_zh"),"left_outer")
      .join(dfs("cdr.zcm_obstype_code"), dfs("temptable")("localcode") === dfs("cdr.zcm_obstype_code")("obscode"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    df.withColumn("LOCALRESULT", when(df("DATATYPE") === lit("D"), from_unixtime(unix_timestamp(df("LOCALRESULT")))).otherwise(df("LOCALRESULT")))
  }
  
  map = Map(
    "DATASRC" -> literal("encountervisit"),
    "OBSDATE" -> cascadeFrom(Seq("CHECKIN_TIME", "ED_ARRIVAL_TIME", "HOSP_ADMSN_TIME", "CONTACT_DATE")),
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
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"), df("OBSDATE"), df("OBSTYPE")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rownbr", row_number.over(groups))
    addColumn.filter("rownbr =1 and OBSDATE is not null and PATIENTID is not null")
  }

}