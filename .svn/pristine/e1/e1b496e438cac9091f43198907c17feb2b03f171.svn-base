package com.humedica.mercury.etl.epic_v2.encounterprovider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class EncounterproviderMedorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("medorders", "temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable", "zh_claritydept"
                  ,"cdr.map_predicate_values")


  columnSelect = Map(
    "temptable" -> List("PAT_ENC_CSN_ID", "ENCOUNTER_PRIMARY_LOCATION", "DEPARTMENT_ID"),
    "zh_claritydept" -> List("DEPARTMENT_ID", "REV_LOC_ID")
  )

  beforeJoin = Map(
    "medorders" -> ((df: DataFrame) => {
      val fil = df.filter("authrzing_prov_id is not null and authrzing_prov_id <> '-1' and pat_id is not null and pat_id <> '-1' and pat_enc_csn_id is not null and pat_enc_csn_id <> '-1'").repartition(1000)
      val groups = Window.partitionBy(fil("authrzing_prov_id"), fil("pat_id"), fil("pat_enc_csn_id")).orderBy(fil("UPDATE_DATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    }
      )
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("medorders")
      .join(dfs("temptable")
        .join(dfs("zh_claritydept"), Seq("DEPARTMENT_ID"), "left_outer")
        , Seq("PAT_ENC_CSN_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("medorders"),
    "ENCOUNTERTIME" -> cascadeFrom(Seq("ORDER_INST", "ORDERING_DATE")),
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
    "PROVIDERID" -> mapFrom("AUTHRZING_PROV_ID"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "PROVIDERROLE" -> literal("Med Authorizing Provider")
  )

}