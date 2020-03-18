package com.humedica.mercury.etl.epic_v2.allergies

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.{ EntitySource}
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 * Created by bhenriksen on 1/19/17.
 */
class AllergiesAllergies(config: Map[String,String]) extends EntitySource(config: Map[String,String])  {

  tables = List(
    "allergies",
    "zh_cl_elg_ndc_list",
    "zh_cl_elg"
    )

  columnSelect = Map(
    "allergies" -> List("DATE_NOTED", "ALRGY_ENTERED_DTTM", "STATUS", "ALLERGEN_ID", "PAT_ID", "UPDATE_DATE", "DESCRIPTION"),
    "zh_cl_elg" -> List("ALLERGEN_ID", "ALLERGEN_TYPE_C"),
    "zh_cl_elg_ndc_list" -> List("ALLERGEN_ID", "MED_INTRCT_NDC")
  )

  beforeJoin = Map(
    "allergies" -> includeIf(
        "coalesce(DATE_NOTED ,ALRGY_ENTERED_DTTM ) IS NOT NULL " +
          "AND STATUS <> '2' AND STATUS is not null " +
          "AND ALLERGEN_ID is not null " +
          "AND PAT_ID is not null")
    )


  join = (dfs: Map[String,DataFrame]) =>
    dfs("allergies")
      .join(dfs("zh_cl_elg")
        .join(dfs("zh_cl_elg_ndc_list"), Seq("ALLERGEN_ID"), "left_outer")
      ,Seq("ALLERGEN_ID"),"left_outer")


  map = Map(
    "DATASRC" -> literal("Allergies"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ONSETDATE" -> cascadeFrom(Seq("DATE_NOTED", "ALRGY_ENTERED_DTTM")),
    "LOCALALLERGENCD" -> mapFrom("ALLERGEN_ID"),
    "LOCALALLERGENDESC" -> mapFrom("DESCRIPTION"),
    "LOCALSTATUS" -> mapFrom("STATUS", nullIf = Seq(null),prefix=config(CLIENT_DS_ID)+"."),      //prefix with '[client_ds_id].'
    "LOCALALLERGENTYPE" -> ((col, df) => df.withColumn(col, concat(lit(config(CLIENT_DS_ID) + "."), (when(isnull(df("ALLERGEN_TYPE_C")), "Unknown").otherwise(df("ALLERGEN_TYPE_C"))))
    )),
    "LOCALNDC" -> mapFrom("MED_INTRCT_NDC")
  )


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PAT_ID"), df("ALLERGEN_ID"), df("UPDATE_DATE")).orderBy(df("PAT_ID"), df("ALLERGEN_ID"), df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  mapExceptions = Map(
    ("H458934_EP2", "LOCALSTATUS") -> mapFrom("STATUS", prefix="ep.")
  )
}
