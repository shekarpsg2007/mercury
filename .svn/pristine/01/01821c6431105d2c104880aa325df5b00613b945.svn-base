package com.humedica.mercury.etl.epic_v2.procedure


import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cdivakaran on 7/21/17.
  */
class ProcedureClqanswerqa(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables=List("cl_qanswer_qa", "cdr.map_custom_proc", "pat_enc_form_ans", "zh_cl_qquest")


  columnSelect = Map(
    "cl_qanswer_qa" -> List("QUEST_ID", "ANSWER_ID"),
    "pat_enc_form_ans" -> List("PAT_ID", "PAT_ENC_CSN_ID", "CONTACT_DATE", "QF_HQA_ID", "QF_STAT_CHNG_INST"),
    "zh_cl_qquest" -> List("QUEST_ID", "QUEST_NAME"),
    "cdr.map_custom_proc" -> List("GROUPID", "DATASRC", "LOCALCODE", "MAPPEDVALUE")
  )


  beforeJoin = Map(
    "cdr.map_custom_proc" -> includeIf("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'cl_qanswer_qa'")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("cl_qanswer_qa")
      .join(dfs("cdr.map_custom_proc"), concat(lit(config(CLIENT_DS_ID)+"."), dfs("cl_qanswer_qa")("QUEST_ID")) === dfs("cdr.map_custom_proc")("localcode"), "inner")
      .join(dfs("pat_enc_form_ans"), dfs("cl_qanswer_qa")("ANSWER_ID") === dfs("pat_enc_form_ans")("QF_HQA_ID"), "inner")
      .join(dfs("zh_cl_qquest"), Seq("QUEST_ID"), "left_outer")
  }

  afterJoin = includeIf("PAT_ID is not null and CONTACT_DATE is not null and QUEST_ID is not null")


  map = Map(
    "DATASRC" -> literal("cl_qanswer_qa"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "PROCEDUREDATE" -> mapFrom("CONTACT_DATE"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALCODE" -> mapFrom("QUEST_ID", prefix=config(CLIENT_DS_ID) + "."),
    "CODETYPE" -> literal("CUSTOM"),
    "MAPPEDCODE" -> mapFrom("MAPPEDVALUE"),
    "LOCALNAME" -> mapFrom("QUEST_NAME")
  )

  afterMap = (df1: DataFrame) => {
    val df=df1.repartition(1000)
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("PROCEDUREDATE"), df("QUEST_ID")).orderBy(df("QF_STAT_CHNG_INST").desc_nulls_last)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1")
  }

}
