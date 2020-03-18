package com.humedica.mercury.etl.asent.procedure

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Engine
import scala.collection.JavaConverters._

class ProcedureCharges(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_charge_code_de",
    "as_charges", "cdr.map_custom_proc")

  columnSelect = Map(
    "as_zc_charge_code_de" -> List("CPT4CODE", "ENTRYNAME", "ID"),
    "as_charges" -> List("BILLING_LOCATION_ID", "CHARGE_CODE_ID", "PATIENT_MRN", "ENCOUNTER_DATE_TIME", "BILLING_PROVIDER_ID",
      "REFERRING_PROVIDER_ID", "ENCOUNTER_DATE_TIME", "ENCOUNTER_ID", "PERFORMING_PROVIDER_ID", "CHARGE_ID", "BILLING_STATUS",
      "LINKED_ICD9_ID", "MODIFIER_ID", "LAST_UPDATED_DATE"),
    "cdr.map_custom_proc" -> List("LOCALCODE", "GROUPID", "DATASRC", "MAPPEDVALUE")
  )

  beforeJoin = Map(
    "as_charges" -> includeIf("BILLING_STATUS not in ('R', 'C') and CHARGE_CODE_ID is not null"),
    "cdr.map_custom_proc" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'charges'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_charges")
      .join(dfs("as_zc_charge_code_de"), dfs("as_zc_charge_code_de")("id") === dfs("as_charges")("charge_code_id"), "left_outer")
      .join(dfs("cdr.map_custom_proc"), dfs("cdr.map_custom_proc")("localcode") === concat_ws(".", lit(config(CLIENT_DS_ID)), dfs("as_charges")("charge_code_id")), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("CHARGE_ID"), df("LINKED_ICD9_ID"), df("MODIFIER_ID"), df("MAPPEDVALUE")).orderBy(df("LAST_UPDATED_DATE").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  map = Map(
    "DATASRC" -> literal("charges"),
    "LOCALCODE" -> ((col, df) => df.withColumn(col, concat_ws(".", lit(config(CLIENT_DS_ID)), df("CHARGE_CODE_ID")))),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "PROCEDUREDATE" -> mapFrom("ENCOUNTER_DATE_TIME"),
    "LOCALBILLINGPROVIDERID" -> mapFrom("BILLING_PROVIDER_ID"),
    "LOCALNAME" -> mapFrom("ENTRYNAME"),
    "REFERPROVIDERID" -> mapFrom("REFERRING_PROVIDER_ID"),
    "ACTUALPROCDATE" -> mapFrom("ENCOUNTER_DATE_TIME", nullIf = Seq("1900-01-01 00:00:00")),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PERFORMINGPROVIDERID" -> mapFrom("PERFORMING_PROVIDER_ID"),
    "SOURCEID" -> mapFrom("CHARGE_ID"),
    "MAPPEDCODE" -> ((col, df) => df.withColumn(col, when(df("MAPPEDVALUE").isNotNull, df("MAPPEDVALUE")).otherwise(substring(df("CPT4CODE"), 1, 5)))),
    "CODETYPE" -> ((col, df) => df.withColumn(col, when(df("MAPPEDVALUE").isNotNull, (lit("CUSTOM"))).otherwise(
      expr("CASE WHEN substr(CPT4CODE,1,5) rlike '^[0-9]{4}[0-9A-Z]$' THEN 'CPT4' " +
        "WHEN substr(CPT4CODE,1,5) rlike '^[A-Z]{1,1}[0-9]{4}$' THEN 'HCPCS' " +
        "WHEN substr(CPT4CODE,1,5) rlike '^[0-9]{2,2}\\.[0-9]{1,2}$' THEN 'ICD9' " +
        "ELSE NULL END"))))
  )

  afterMap = (df: DataFrame) => {
    val cols = Engine.schema.getStringList("Procedure").asScala.map(_.split("-")(0).toUpperCase())
    df.select(cols.map(col): _*).distinct
  }  
  
  mapExceptions = Map(
    ("H053731_AS ENT", "MAPPEDCODE") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("MAPPEDVALUE").isNotNull, df("MAPPEDVALUE"))
        .when(length(df("CPT4CODE")) === 6, substring(df("CPT4CODE"), 2, 5))
        .otherwise(df("CPT4CODE")))
    })
  )
}