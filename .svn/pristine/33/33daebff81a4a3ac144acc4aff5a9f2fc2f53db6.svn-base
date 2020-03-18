package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
  * Auto-generated on 02/01/2017
  */


class ObservationReferral(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "referral",
    "cdr.zcm_obstype_code"
  )

  columnSelect = Map(
    "referral" -> List("PAT_ID", "ENTRY_DATE", "PRIM_LOC_ID", "UPDATE_DATE", "RFL_TYPE_C", "REFD_TO_DEPT_ID",
      "REFD_TO_SPEC_C", "PROV_SPEC_C", "REFERRAL_ID", "RFL_STATUS_C", "DENY_RSN_C"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE")
  )

  beforeJoin = Map(
    "referral" -> ((df: DataFrame) => {
      val ref2 = table("referral")
      val addColumn = ref2.filter("pat_id is not null")
        .withColumn("LOCALOBSCODE", when(expr("RFL_TYPE_C in ('101','111') or REFD_TO_DEPT_ID in ('3500101') " +
          "or REFD_TO_SPEC_C in ('10061','20015','600968') or PROV_SPEC_C in ('28','38','74')"), lit("BMI_FOLLOWUP"))
          .when(expr("PROV_SPEC_C in ('100202','101215','101248','101249','101251','108','113','154','16','162','163'," +
            "'164','165','166','170','171','172','173','174','175','86')"), lit("DEPRESSION_FOLLOWUP"))
          .otherwise(lit(null)))
        .select("LOCALOBSCODE", "PAT_ID", "REFERRAL_ID")
        .filter("localobscode is not null")

      val df1 = df.filter("rfl_status_c not in ('4', '5', '6', '100500') and deny_rsn_c is null")
      df1.join(addColumn, Seq("PAT_ID", "REFERRAL_ID"), "inner")
    }),
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUPID) + "' and DATASRC = 'referral'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("referral")
      .join(dfs("cdr.zcm_obstype_code"), dfs("referral")("LOCALOBSCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("referral"),
    "OBSDATE" -> mapFrom("ENTRY_DATE"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "FACILITYID" -> mapFrom("PRIM_LOC_ID"),
    "LOCALRESULT" -> literal("REFERRAL"),
    "LOCALCODE" -> mapFrom("LOCALOBSCODE"),
    "OBSRESULT" -> literal("REFERRAL")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PAT_ID"), df("LOCALCODE"), df("OBSTYPE"), df("OBSDATE")).orderBy(df("UPDATE_DATE").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

}