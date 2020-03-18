package com.humedica.mercury.etl.asent.claim

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Engine
import scala.collection.JavaConverters._

class ClaimCharges(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_charges",
    "as_encounters",
    "as_zc_charge_code_de",
    "as_zc_cpt4_modifier_de")

  columnSelect = Map(
    "as_encounters" -> List("ENCOUNTER_ID", "APPOINTMENT_STATUS_ID"),
    "as_zc_charge_code_de" -> List("CPT4CODE", "ID"),
    "as_charges" -> List("ENCOUNTER_ID", "PATIENT_MRN", "UNITS_TO_BILL_FOR", "CHARGE_CODE_ID", "ENCOUNTER_DATE_TIME", "PERFORMING_PROVIDER_ID", "MODIFIER_ID"
      , "MODIFIER_NUMBER", "CHARGE_ID", "LINKED_ICD9_ID", "LAST_UPDATED_DATE", "BILLING_STATUS", "BILLING_PROVIDER_ID"),
    "as_zc_cpt4_modifier_de" -> List("ENTRYCODE", "ID")
  )

  beforeJoin = Map(
    "as_encounters" -> ((df1: DataFrame) => {
      val df = df1.repartition(1000)
        .withColumnRenamed("ENCOUNTER_ID", "ENCOUNTER_ID_ae")
      df.filter("APPOINTMENT_STATUS_ID IS NULL OR APPOINTMENT_STATUS_ID = 2")
    }),
    "as_zc_cpt4_modifier_de" -> ((df1: DataFrame) => {
      df1.filter("length(ENTRYCODE) <= 2")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_charges")
      .join(dfs("as_encounters"), dfs("as_encounters")("ENCOUNTER_ID_ae") === dfs("as_charges")("ENCOUNTER_ID"), "inner")
      .join(dfs("as_zc_charge_code_de"), dfs("as_zc_charge_code_de")("ID") === dfs("as_charges")("CHARGE_CODE_ID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val fil = df.filter("BILLING_STATUS not in ('R','C')")
    val groups = Window.partitionBy(fil("CHARGE_ID"), fil("LINKED_ICD9_ID"), fil("MODIFIER_ID")).orderBy(fil("LAST_UPDATED_DATE").desc_nulls_last)
    val dedup = fil.withColumn("rn", row_number.over(groups)).filter("rn = 1").drop("rn")
    val unpivot = dedup.groupBy("ENCOUNTER_ID", "PATIENT_MRN", "UNITS_TO_BILL_FOR", "CHARGE_CODE_ID", "ENCOUNTER_DATE_TIME", "PERFORMING_PROVIDER_ID", "BILLING_PROVIDER_ID", "CPT4CODE")
      .pivot("MODIFIER_NUMBER", Seq("1", "2", "3", "4")).agg(max("MODIFIER_ID"))
    val zh_mod1 = table("as_zc_cpt4_modifier_de").withColumnRenamed("ENTRYCODE", "ENTRYCODE_1").withColumnRenamed("ID", "ID_1")
    val zh_mod2 = table("as_zc_cpt4_modifier_de").withColumnRenamed("ENTRYCODE", "ENTRYCODE_2").withColumnRenamed("ID", "ID_2")
    val zh_mod3 = table("as_zc_cpt4_modifier_de").withColumnRenamed("ENTRYCODE", "ENTRYCODE_3").withColumnRenamed("ID", "ID_3")
    val zh_mod4 = table("as_zc_cpt4_modifier_de").withColumnRenamed("ENTRYCODE", "ENTRYCODE_4").withColumnRenamed("ID", "ID_4")

    unpivot
      .join(zh_mod1, unpivot("1") === zh_mod1("ID_1"), "left_outer")
      .join(zh_mod2, unpivot("2") === zh_mod2("ID_2"), "left_outer")
      .join(zh_mod3, unpivot("3") === zh_mod3("ID_3"), "left_outer")
      .join(zh_mod4, unpivot("4") === zh_mod4("ID_4"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("charges"),
    "CLAIMID" -> mapFrom("ENCOUNTER_ID"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "QUANTITY" -> mapFrom("UNITS_TO_BILL_FOR"),
    "LOCALCPT" -> mapFrom("CHARGE_CODE_ID"),
    "SERVICEDATE" -> mapFrom("ENCOUNTER_DATE_TIME"),
    "CLAIMPROVIDERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PERFORMING_PROVIDER_ID") === lit("0"), null).otherwise(df("PERFORMING_PROVIDER_ID")))
    }),
    "LOCALBILLINGPROVIDERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("BILLING_PROVIDER_ID") === lit("0"), null).otherwise(df("BILLING_PROVIDER_ID")))
    }),
    "MAPPEDCPT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(substring(df("CPT4CODE"), 1, 5) rlike "^[0-9]{4}[0-9A-Z]$", substring(df("CPT4CODE"), 1, 5)).otherwise(null))
    }),
    "MAPPEDHCPCS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("CPT4CODE") rlike "^[A-Z]{1,1}[0-9]{4}$", df("CPT4CODE")).otherwise(null))
    }),
    "LOCALCPTMOD1" -> mapFrom("1"),
    "LOCALCPTMOD2" -> mapFrom("2"),
    "LOCALCPTMOD3" -> mapFrom("3"),
    "LOCALCPTMOD4" -> mapFrom("4"),
    "MAPPEDCPTMOD1" -> mapFrom("ENTRYCODE_1"),
    "MAPPEDCPTMOD2" -> mapFrom("ENTRYCODE_2"),
    "MAPPEDCPTMOD3" -> mapFrom("ENTRYCODE_3"),
    "MAPPEDCPTMOD4" -> mapFrom("ENTRYCODE_4")
  )

  afterMap = (df: DataFrame) => {
    val cols = Engine.schema.getStringList("Claim").asScala.map(_.split("-")(0).toUpperCase())
    df.select(cols.map(col): _*).distinct
  }

  mapExceptions = Map(
    ("H053731_AS_ENT", "MAPPEDCPT") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("CPT4CODE")) === 6, substring(df("CPT4CODE"), 2, 5))
        .when(substring(df("CPT4CODE"), 1, 5) rlike "^[0-9]{4}[0-9A-Z]$", substring(df("CPT4CODE"), 1, 5))
        .otherwise(null))
    })
  )

}