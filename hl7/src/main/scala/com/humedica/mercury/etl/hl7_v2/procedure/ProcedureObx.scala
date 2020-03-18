package com.humedica.mercury.etl.hl7_v2.procedure

import com.humedica.mercury.etl.core.engine.Constants.CLIENT_DS_ID
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 9/21/17.
  */
class ProcedureObx(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("temptable:hl7_v2.temptable.TemptableAll"
    ,"hl7_segment_obx_a")

  columnSelect = Map(
    "temptable" -> List("PATIENTID","MESSAGEID","LASTUPDATEDATE"),
    "hl7_segment_obx_a" -> List("OBX_F3_C2","OBX_F14_C1","OBX_RESULTDATA","MESSAGEID")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hl7_segment_obx_a")
      .join(dfs("temptable"), Seq("MESSAGEID"), "inner")
      .join(dfs("cdr.map_custom_proc"), concat(lit(config(CLIENT_DS_ID) + "."), dfs("hl7_segment_obx_a")("OBX_F3_C2")) === dfs("cdr.map_custom_proc")("LOCALCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("hl7_segment_obx_a"),
    "LOCALCODE" -> mapFrom("OBX_F3_C2",nullIf=Seq("\"\"")),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "PROCEDUREDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(!df("OBX_F3_C2").isin("amb_intnal_med_note_PneumoVac_date","amb_pulm_immunizations_pneumovax_when?","note_amb_proj_stay_disc_sum_pneumovax_dt",
            "amb_geri_screen_last_mammo_date", "amb_obgyn_visit_mammogramHx_date_ft3", "amb_obgyn_visit_mammogramHx_date_ft1",
            "amb_obgyn_visit_mammogramHx_date_ft5", "amb_obgyn_visit_mammogramHx_date_ft4", "amb_intnal_last_mammo_date",
            "amb_obgyn_visit_mammogramHx_date_ft6", "amb_obgyn_visit_mammogramHx_date_ft2", "COSMO_HM_mammo_dt",
            "amb_obgyn_visit_bonedensity_screen_date_ft1", "amb_obgyn_cont_last_dexa", "amb_aim_screen_last_dexa_ft",
            "amb_BJ_Fam_Med_screening_dexa", "amb_geri_screen_last_colon_date", "amb_intnal_last_colon_date",
            "Date of last colonoscopy, if known:", "amb_IM_diab_eye_last_date", "afm_ad_soap_HM_SCREEN_colon",
            "afm_ad_soap_HM_SCREEN_colon_ft", "afm_ad_soap_HM_SCREEN_Dexa Scan_ft", "afm_ad_soap_HM_SCREEN_mammo",
            "afm_ad_soap_HM_SCREEN_mammo_ft", "COSMO_HM_colon_dt", "COSMO_HM_mammo_dt",
            "afm_ad_soap_HM_SCREEN_optho_ft", "amb_diab_sm_rd_risk_eye", "amb_diabetes_Last_eye_MDvisit_txt",
            "amb_geri_meds_surg_eyeexam", "amb_IM_diab_eye_last_date", "amb_intnal_med_note_eyeexam",
            "amb_BJ_Fam_Med_screen_optho", "amb_IM_podiatry_last_date", "amb_diab_sm_rd_risk_foot",
            "amb_diabetes_Last_Pod_Mdvisit_txt", "amb_geri_meds_surg_podiatry", "amb_intnal_med_note_podiatry1")
              ,unix_timestamp(substring(df("OBX_F14_C1"),1,8), "yyyyMMdd").cast("timestamp"))
          .when(substring(df("OBX_RESULTDATA"),1,255) rlike "^[0-9]{4}", unix_timestamp(concat_ws(".", substring(df("OBX_RESULTDATA"),1,4), lit("0101")),"yyyyMMdd").cast("timestamp"))
          .when(substring(df("OBX_RESULTDATA"),1,255) rlike "^[0-9]{2}(\\-|/)[0-9]{4}", unix_timestamp(concat_ws(".", substring(df("OBX_RESULTDATA"),4,4), substring(df("OBX_RESULTDATA"),1,2) ,lit("01")),"yyyyMMdd").cast("timestamp"))
          .when(substring(df("OBX_RESULTDATA"),1,255) rlike "^[0-9]{1}(\\-|/)[0-9]{4}", unix_timestamp(concat_ws(".", substring(df("OBX_RESULTDATA"),3,4), lit("0") ,substring(df("OBX_RESULTDATA"),1,1) ,lit("01")),"yyyyMMdd").cast("timestamp"))
          .when(substring(df("OBX_RESULTDATA"),1,255) rlike "^[0-9]{2}(\\-|/)[0-9]{2}", unix_timestamp(concat_ws(".", lit("20") ,substring(df("OBX_RESULTDATA"),4,2),substring(df("OBX_RESULTDATA"),1,2) ,lit("01")),"yyyyMMdd").cast("timestamp"))
          .when(substring(df("OBX_RESULTDATA"),1,255) rlike "^[0-9]{1}(\\-|/)[0-9]{2}", unix_timestamp(concat_ws(".", lit("20") ,substring(df("OBX_RESULTDATA"),3,2), lit("0"),substring(df("OBX_RESULTDATA"),1,1) ,lit("01")),"yyyyMMdd").cast("timestamp"))
          .when(unix_timestamp(substring(df("OBX_RESULTDATA"),1,10), "MM/dd/yyyy").isNotNull, unix_timestamp(concat_ws(".", substring(df("OBX_RESULTDATA"),7,4), lit("0101")),"yyyyMMdd").cast("timestamp"))
          .when(unix_timestamp(substring(df("OBX_RESULTDATA"),1,8), "MM/dd/yy").isNotNull, unix_timestamp(concat_ws(".", lit("20") ,substring(df("OBX_RESULTDATA"),7,4), lit("0101")),"yyyyMMdd").cast("timestamp"))
          .when(unix_timestamp(substring(df("OBX_RESULTDATA"),1,10), "yyyy-mm-dd").isNotNull, unix_timestamp(substring(df("OBX_RESULTDATA"),7,4),"yyyy-mm-dd").cast("timestamp"))
          .when(unix_timestamp(substring(df("OBX_RESULTDATA"),1,8), "Mon yyyy").isNotNull, unix_timestamp(substring(df("OBX_RESULTDATA"),1,8),"Mon yyyy").cast("timestamp"))
          .when(unix_timestamp(df("OBX_RESULTDATA"), "Month dd yyyy").isNotNull, unix_timestamp(df("OBX_RESULTDATA"),"Month dd yyyy").cast("timestamp"))
          .when(unix_timestamp(df("OBX_RESULTDATA"), "Month/yyyy").isNotNull, unix_timestamp(df("OBX_RESULTDATA"),"Month/yyyy").cast("timestamp"))
          .otherwise(null)
      )
    }),
    "LOCALNAME" -> mapFrom("OBX_F3_C2",nullIf=Seq("\"\"")),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> mapFrom("CUSTOM")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("PATIENTID IS NOT NULL AND PROCEDUREDATE IS NOT NULL AND LOCALCODE IS NOT NULL")
    val groups = Window.partitionBy(fil("PATIENTID"), fil("PROCEDUREDATE"), fil("LOCALCODE")).orderBy(fil("LASTUPDATEDATE").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  mapExceptions = Map(
    ("H984787_HL7_ATH_QM_WIAPP", "PROCEDUREDATE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, unix_timestamp(substring(df("OBX_F14_C1"), 1, 8), "yyyyMMdd").cast("timestamp"))
    })
  )

}
