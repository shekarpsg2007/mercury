package com.humedica.mercury.etl.epic_v2.immunization

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ImmunizationImmunization(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("immunizations")

  beforeJoin = Map(
    "immunizations" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("Immune_id"), df("pat_id"), df("Imm_Csn"), df("Immunzatn_Id"),
        expr("case when Defer_Reason_C IS NOT NULL AND Defer_Reason_C <> -1 then null else coalesce(immunization_time, immune_date) end"))
        .orderBy(df("UPDATE_DATE").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1 and (IMMNZTN_STATUS_C not in ('2','3') OR IMMNZTN_STATUS_C is null) and PAT_ID is not null and IMMUNZATN_ID is not null").drop("rn")
    })
  )

  map = Map(
    "DATASRC" -> literal("immunization"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("IMM_CSN"),
    "LOCALDEFERREDREASON" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DEFER_REASON_C") === "-1",  null).when(isnull(df("DEFER_REASON_C")),null).otherwise(concat(lit(config(CLIENT_DS_ID)+"."), df("DEFER_REASON_C"))))
    }),
    "LOCALPATREPORTEDFLG" -> ((col: String, df: DataFrame) => df.withColumn(col, when(df("EXTERNAL_ADMIN_C") === "-1", null).when(df("EXTERNAL_ADMIN_C").isNotNull, "Y").otherwise(null))),
    "LOCALROUTE" -> mapFrom("ROUTE_C", nullIf= Seq("-1") ),
    "ADMINDATE" -> ((col:String, df:DataFrame)=> {df.withColumn(col,
      when(df("DEFER_REASON_C").isNotNull && (df("DEFER_REASON_C") =!= "-1"), null)
        .otherwise(coalesce(df("IMMUNIZATION_TIME"), df("IMMUNE_DATE")))) }),
    "DOCUMENTEDDATE" -> mapFrom("ENTRY_DATE"),
    "LOCALIMMUNIZATIONCD" -> mapFrom("IMMUNZATN_ID"),
    "LOCALIMMUNIZATIONDESC" -> mapFrom("NAME")
  )

  afterMap = (df: DataFrame) => {
    val dedup = df.filter("ADMINDATE is not null")
    val groups = Window.partitionBy(dedup("PATIENTID"), dedup("ENCOUNTERID"), dedup("LOCALIMMUNIZATIONCD"), dedup("ADMINDATE")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }
}

//  val i = new ImmunizationImmunization(cfg); val imm=build(i) ; imm.show
