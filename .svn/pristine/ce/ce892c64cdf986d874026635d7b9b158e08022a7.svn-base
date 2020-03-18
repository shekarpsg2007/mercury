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


class EncounterproviderHspatndprov(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List("temptable:epic_v2.clinicalencounter.ClinicalencounterTemptable","hsp_atnd_prov")



  columnSelect = Map(
    "temptable" -> List("HOSP_ADMSN_TIME", "HOSP_DISCH_TIME", "PAT_ENC_CSN_ID", "CHECKIN_TIME", "ED_ARRIVAL_TIME", "CONTACT_DATE"),
    "hsp_atnd_prov" -> List("PAT_ENC_CSN_ID", "PAT_ID", "PROV_ID", "ATTEND_TO_DATE", "ATTEND_FROM_DATE", "UPDATE_DATE", "LINE")
  )



  beforeJoin = Map(
    "hsp_atnd_prov" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("PAT_ENC_CSN_ID"), df1("PROV_ID")).orderBy(df1("UPDATE_DATE").desc_nulls_last)
      df1.withColumn("rn_hsp", row_number.over(groups))
        .filter("rn_hsp = 1 and coalesce(prov_id, '-1') <> '-1' and coalesce(pat_id, '-1') <> '-1' and coalesce(pat_enc_csn_id, '-1') <> '-1'")
        .withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_hsp")
    })

  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("hsp_atnd_prov"), dfs("temptable")("PAT_ENC_CSN_ID") === dfs("hsp_atnd_prov")("PAT_ENC_CSN_ID_hsp"))
  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PAT_ENC_CSN_ID_hsp")).orderBy(df("LINE").desc_nulls_last)
    df.withColumn("line_rn", row_number.over(groups))
  }


  map = Map(
    "DATASRC" -> literal("hsp_atnd_prov"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID_hsp"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERTIME" -> ((col: String, df: DataFrame) => {
        df.withColumn(col, when(df("line_rn") === lit("1"),
            coalesce(df("ATTEND_TO_DATE"),df("HOSP_DISCH_TIME"),df("CHECKIN_TIME"),df("ED_ARRIVAL_TIME"),df("HOSP_ADMSN_TIME"),df("CONTACT_DATE"),df("ATTEND_FROM_DATE")))
            .otherwise(coalesce(df("ATTEND_TO_DATE"),df("CHECKIN_TIME"),df("ED_ARRIVAL_TIME"),df("HOSP_ADMSN_TIME"),df("CONTACT_DATE"),df("ATTEND_FROM_DATE"))))
    }),
    "PROVIDERID" -> mapFrom("PROV_ID"),
    "PROVIDERROLE" -> literal("Hsp Attending Provider")
  )


  afterMap = (df: DataFrame) => {
    df.filter("encountertime is not null")
  }

}
