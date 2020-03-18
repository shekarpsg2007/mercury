package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

class ProcedureGeneralorderscustom(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable",
    "generalorders",
    "zh_clarity_eap",    
    "cdr.map_custom_proc",
    "cdr.map_predicate_values"
  )

  columnSelect = Map(
    "temptable" -> List("PAT_ENC_CSN_ID", "ROW_NUM_ENC_LVL","CONTACT_DATE"),
    "zh_clarity_eap" -> List("PROC_ID","PROC_CODE")    
  )

  beforeJoin = Map(
    "temptable" -> renameColumn("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_ev"),
    "cdr.map_custom_proc" -> ((df: DataFrame) => {
      df.filter("DATASRC = 'GENERALORDERS' and GROUPID = '" + config(GROUP) + "'").select("MAPPEDVALUE", "LOCALCODE")
    }),
    "zh_clarity_eap" -> ((df: DataFrame) => {
      df.withColumnRenamed("PROC_CODE","PROC_CODE_zh")
    })    
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("generalorders")
      .join(dfs("zh_clarity_eap"), Seq("PROC_ID"), "left_outer")       
      .join(dfs("temptable"),dfs("generalorders")("PAT_ENC_CSN_ID") === dfs("temptable")("PAT_ENC_CSN_ID_ev") && (dfs("temptable")("ROW_NUM_ENC_LVL") === "1"),"left_outer" )
      .join(dfs("cdr.map_custom_proc"), dfs("cdr.map_custom_proc")("LOCALCODE") === 
        concat(lit(config(CLIENT_DS_ID)+"."), coalesce(dfs("generalorders")("proc_code"), dfs("zh_clarity_eap")("PROC_CODE_zh"))), "left_outer")  
  }

  afterJoin = (df: DataFrame) => {
    val addColumn = df.withColumn("LOCALCODE", concat(lit(config(CLIENT_DS_ID)+"."), coalesce(df("proc_code"), df("PROC_CODE_zh"))))  
    val fil = addColumn.filter("PAT_ID is not null and PAT_ID <> '-1' and ORDER_STATUS_C in ('5', '3', '-1') and LOCALCODE is not null")
    val t_order_type_c = mpv(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID),"GENERALORDERS", "PROCEDUREDO", "GENERALORDERS", "ORDER_TYPE_C")
      .select("COLUMN_VALUE")
    fil.join(t_order_type_c, fil("ORDER_TYPE_C") === t_order_type_c("COLUMN_VALUE"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("generalorders_custom"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID", nullIf=Seq("-1")),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PERFORMINGPROVIDERID" -> mapFrom("PERFORMING_MD", nullIf=Seq("-1")),
    "PROCEDUREDATE" -> cascadeFrom(Seq("SPECIMN_TAKEN_TIME", "PROC_BGN_TIME", "RESULT_TIME", "PROC_START_TIME", "CONTACT_DATE")),
    "LOCALBILLINGPROVIDERID" -> mapFrom("BILLING_PROV_ID", nullIf=Seq("-1")),
    "LOCALNAME" -> mapFrom("ORDER_DESCRIPTION"),
    "ORDERINGPROVIDERID" -> mapFrom("AUTHRZING_PROV_ID", nullIf=Seq("-1")),
    "REFERPROVIDERID" -> mapFrom("REFERRING_PROV_ID", nullIf=Seq("-1")),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "CODETYPE" -> literal("CUSTOM")
  )


  afterMap = (df1: DataFrame) => {
    val df=df1.repartition(1000)
    val groups = Window.partitionBy(df("PAT_ENC_CSN_ID"), df("cpt_code"), df("ORDER_PROC_ID"), df("MAPPEDVALUE")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rownbr", row_number.over(groups))
    addColumn.filter("rownbr = 1 and ORDER_STATUS_C in ('5', '3') and MAPPEDVALUE is not null and PROCEDUREDATE is not null")
  }

}

