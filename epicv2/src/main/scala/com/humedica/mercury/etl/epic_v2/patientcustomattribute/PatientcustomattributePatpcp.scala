package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
  * Created by abendiganavale on 7/17/18.
  */
class PatientcustomattributePatpcp(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_pat_pcp", "zh_providerattr", "cdr.map_predicate_values")

  columnSelect = Map(
    "zh_pat_pcp" -> List("EFF_DATE", "TERM_DATE", "PCP_PROV_ID", "PAT_ID","RELATIONSHIP_C","DELETED_YN"),
    "zh_providerattr" -> List("PROV_ID", "PROV_NAME", "EXTRACT_DATE")
  )

  beforeJoin = Map(
    "zh_pat_pcp" -> ((df: DataFrame) => {
      val df1 = df.repartition(500)
      val list_relationship_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PAT_PCP", "PATIENT_ATTRIBUTE", "ZH_PAT_PCP", "RELATIONSHIP_C")
      val fil = df1.filter("pat_id is not null and RELATIONSHIP_C in (" + list_relationship_c + ")")
      val groups = Window.partitionBy(fil("PAT_ID")).orderBy(fil("TERM_DATE").desc_nulls_first, fil("EFF_DATE").desc_nulls_last)
      val t = fil.withColumn("pcp_rownumber", row_number.over(groups))
      t.filter("pcp_rownumber = 1 and eff_date is not null").drop("pcp_rownumber")
    }),
    "zh_providerattr" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PROV_ID")).orderBy(df("EXTRACT_DATE").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_pat_pcp").join(dfs("zh_providerattr"), dfs("zh_providerattr")("PROV_ID") === dfs("zh_pat_pcp")("PCP_PROV_ID"), "inner")
  }

  afterJoin = (df: DataFrame) => {

    val list_eff_term_dt = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PAT_PCP", "PATIENT_ATTRIBUTE", "ZH_PAT_PCP", "EFF_TERM_DATES")
    val addColumn1 = df.withColumn("EFF_DT", when(lit("'Y'") === list_eff_term_dt, substring(df("EFF_DATE"),1,10)).otherwise(null))
                       .withColumn("TERM_DT", when(lit("'Y'") === list_eff_term_dt, when(df("TERM_DATE").isNull, lit("CURRENT")).otherwise(substring(df("TERM_DATE"),1, 10))))
                       .withColumn("PROV_NAME_ID", when(df("PROV_NAME").isNotNull, concat_ws("", df("PROV_NAME"), lit(", "), df("PROV_ID"))).otherwise(df("PROV_ID")))

    val fpiv1 = unpivot(
      Seq("EFF_DT", "TERM_DT", "PROV_NAME_ID"),
      Seq("CH002786", "CH002787", "CH002788"), typeColumnName = "ATTRIBUTEVALUETYPE"
    )
    fpiv1("ATTRIBUTEVALUE", addColumn1)
  }

  map = Map(
    "DATASRC" -> literal("pat_pcp"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ATTRIBUTE_TYPE_CUI" -> mapFrom("ATTRIBUTEVALUETYPE"),
    "ATTRIBUTE_VALUE" -> mapFrom("ATTRIBUTEVALUE")
  )

}
