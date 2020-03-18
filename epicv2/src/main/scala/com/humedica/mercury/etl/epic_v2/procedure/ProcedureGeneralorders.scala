package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

class ProcedureGeneralorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable",
    "generalorders",
    "zh_clarity_eap",
    "zh_clarity_eap_ot",
    "cdr.map_custom_proc",
    "cdr.map_predicate_values"
  )

  columnSelect = Map(
    "temptable" -> List("PAT_ENC_CSN_ID", "ROW_NUM_ENC_LVL","CONTACT_DATE"),
    "generalorders" -> List("PAT_ENC_CSN_ID","PROC_CODE","PAT_ID","ORDER_TYPE_C","PERFORMING_MD", "SPECIMN_TAKEN_TIME", "PROC_BGN_TIME", "RESULT_TIME", "ORDER_STATUS_C",
      "PROC_START_TIME","ORDER_TIME","BILLING_PROV_ID","ORDER_DESCRIPTION", "AUTHRZING_PROV_ID", "REFERRING_PROV_ID", "CPT_CODE", "ORDER_PROC_ID", "PROC_ID","UPDATE_DATE"),
    "zh_clarity_eap" -> List("PROC_ID","PROC_CODE"),
    "zh_clarity_eap_ot" -> List("PROC_ID","CPT_CODE","CONTACT_DATE", "CONTACT_COMMENT")
  )

  beforeJoin = Map(
    "temptable" -> renameColumn("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_ev"),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'GENERALORDERS' and GROUPID = '" + config(GROUP) + "'").select("MAPPEDVALUE", "LOCALCODE")
    }),
    "zh_clarity_eap" -> ((df: DataFrame) => {
      df.withColumnRenamed("PROC_CODE","PROC_CODE_zh")
    }),    
    "zh_clarity_eap_ot" -> ((df: DataFrame) => {
      val fil = df.filter("CPT_CODE is not null")
      val groups = Window.partitionBy(fil("PROC_ID")).orderBy(fil("CONTACT_DATE").desc_nulls_last, fil("CONTACT_COMMENT").desc_nulls_last)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn", "CONTACT_DATE").withColumnRenamed("CPT_CODE","CPT_CODE_zh")
    }) 
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("generalorders")
      .join(dfs("zh_clarity_eap"), Seq("PROC_ID"), "left_outer")   
      .join(dfs("zh_clarity_eap_ot"), Seq("PROC_ID"), "left_outer")           
      .join(dfs("temptable"),dfs("generalorders")("PAT_ENC_CSN_ID") === dfs("temptable")("PAT_ENC_CSN_ID_ev") && (dfs("temptable")("ROW_NUM_ENC_LVL") === "1"),"left_outer" )
      .join(dfs("cdr.map_custom_proc"), dfs("cdr.map_custom_proc")("LOCALCODE") === 
        concat(lit(config(CLIENT_DS_ID)+"."), coalesce(dfs("generalorders")("proc_code"), dfs("zh_clarity_eap")("PROC_CODE_zh"))), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val addColumn = df.withColumn("LOCALCODE", concat(lit(config(CLIENT_DS_ID)+"."), coalesce(df("proc_code"), df("PROC_CODE_zh"))))
      .withColumn("STANDARDCODE", coalesce(df("CPT_CODE_zh"),df("CPT_CODE")))
    val fil = addColumn.filter("PAT_ID is not null and PAT_ID <> '-1' and LOCALCODE is not null")
    val t_order_type_c = mpv(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),"GENERALORDERS", "PROCEDUREDO", "GENERALORDERS", "ORDER_TYPE_C")
      .select("COLUMN_VALUE")
    fil.join(t_order_type_c, fil("ORDER_TYPE_C") === t_order_type_c("COLUMN_VALUE"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("generalorders"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID", nullIf=Seq("-1")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PERFORMINGPROVIDERID" -> mapFrom("PERFORMING_MD", nullIf=Seq("-1")),
    "PROCEDUREDATE" -> cascadeFrom(Seq("SPECIMN_TAKEN_TIME", "PROC_BGN_TIME", "RESULT_TIME", "PROC_START_TIME", "CONTACT_DATE","ORDER_TIME")),
    "LOCALBILLINGPROVIDERID" -> mapFrom("BILLING_PROV_ID", nullIf=Seq("-1")),
    "LOCALNAME" -> mapFrom("ORDER_DESCRIPTION"),
    "ORDERINGPROVIDERID" -> mapFrom("AUTHRZING_PROV_ID", nullIf=Seq("-1")),
    "REFERPROVIDERID" -> mapFrom("REFERRING_PROV_ID", nullIf=Seq("-1")),
    "MAPPEDCODE" -> mapFromRequiredLength("STANDARDCODE", 5),
    "CODETYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("STANDARDCODE") rlike "^[0-9]{4}[0-9A-Z]$","CPT4")
        .when(df("STANDARDCODE") rlike "[A-Z]{1,1}[0-9]{4}$","HCPCS")
        .when(df("STANDARDCODE") rlike "^[0-9]{2,2}\\.[0-9]{1,2}$", "ICD9").otherwise(null)
      )})
  )


  afterMap = (df1: DataFrame) => {
    val df=df1.repartition(1000)
    val groups = Window.partitionBy(df("PAT_ENC_CSN_ID"), df("cpt_code"), df("ORDER_PROC_ID"), df("MAPPEDVALUE")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rownbr", row_number.over(groups))
    addColumn.filter("rownbr = 1 and COLUMN_VALUE is not null and PROCEDUREDATE is not null and ORDER_STATUS_C in ('5', '3', '-1')")
  }

}