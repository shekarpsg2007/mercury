package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ObservationPatientmyc(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("patient_myc", "cdr.zcm_obstype_code")

      beforeJoin = Map(
        "patient_myc" -> includeIf("MYCHART_STATUS_C = '1' AND PAT_ACCESS_CODE_TM is not null AND PAT_ID is not null"),
        "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'patient_myc' AND GROUPID='"+config(GROUP)+"'") )

      join = (dfs: Map[String, DataFrame]) => {
        dfs("patient_myc")
          .join(dfs("cdr.zcm_obstype_code"), lit("PATIENT_MYC") === dfs("cdr.zcm_obstype_code")("OBSCODE"))
      }

      map = Map(
        "DATASRC" -> literal("patient_myc"),
        "LOCALCODE" -> literal("PATIENT_MYC"),
        "OBSDATE" -> mapFrom("PAT_ACCESS_CODE_TM"),
        "OBSTYPE" -> mapFrom("OBSTYPE"),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "LOCALRESULT" -> literal("PATIENT_MYC"),
        "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
        "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
      )


    afterMap =   (df: DataFrame) => {
      df.filter("OBSTYPE is not null AND LOCALCODE is not null")
    }

 }