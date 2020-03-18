package com.humedica.mercury.etl.epic_v2.encounterprovider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by abendiganavale on 4/19/18.
  */
class EncounterproviderProfbilling(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.claim.ClaimProfbillingtemptable"
    ,"cdr.map_predicate_values")

  map = Map(
    "DATASRC" -> literal("profbilling"),
    "ENCOUNTERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("", lit("profbilling."), df("PAT_ID"), lit("."), df("TXN_TX_ID")))
    }),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERTIME" -> mapFrom("ORIG_SERVICE_DATE"),
    "FACILITYID" -> ((col: String, df: DataFrame) => {
      val facility_col = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
        "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "FACILITYID")
      df.withColumn(col,
        when(lit("'Dept'") === facility_col, df("DEPTID"))
          .when(lit("'Loc'") === facility_col, df("LOC_ID"))
          .when(lit("'coalesceDeptPrimary'") === facility_col, coalesce(df("DEPTID"), when(df("LOC_ID").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("LOC_ID"))).otherwise(null)))
          .when(lit("'coalesceLocDept'") === facility_col, coalesce(df("LOC_ID"),df("DEPTID")))
          .otherwise(coalesce(df("DEPTID"), when(df("LOC_ID").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("LOC_ID"))).otherwise(null)))
      )
    })
  )

  afterMap = (df: DataFrame) => {

    val fpiv = unpivot(
      Seq("PROV_ID","REFERRING_PROV_ID","BILLING_PROVIDER_ID","PERFORMING_PROV_ID"),
      Seq("Visit Provider", "Referring Provider", "Billing Provider", "Performing Provider"),typeColumnName = "PROVIDERROLE")

    val df1 = fpiv("PROVIDERID",df)

    val include_group = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),
      "PROFBILLING", "CLINICALENCOUNTER", "PROFBILLING_TXN", "INCLUDE")
    val groups = Window.partitionBy(df1("PATIENTID"), df1("ENCOUNTERID"), df1("PROVIDERID"), df1("PROVIDERROLE")).orderBy(df1("POST_DATE").desc)
    val addColumn = df1.withColumn("rn", row_number.over(groups))
    addColumn.filter("PATIENTID IS NOT NULL AND PROVIDERID IS NOT NULL AND ENCOUNTERTIME IS NOT NULL AND PAT_ENC_CSN_ID IS NULL AND 'Y' = " + include_group + " AND rn = 1").drop("rn")
  }

}
