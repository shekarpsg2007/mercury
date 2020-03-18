package com.humedica.mercury.etl.epic_v2.encounterprovider

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



class EncounterproviderMarbillingprovider(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinicalencounter:epic_v2.clinicalencounter.ClinicalencounterEncountervisit", "medadminrec")

  columnSelect = Map(
    "clinicalencounter" -> List("ARRIVALTIME", "ENCOUNTERID"),
    "medadminrec" -> List("MAR_ENC_CSN", "PAT_ID", "MAR_BILLING_PROV_ID")
  )

  beforeJoin = Map(
    "medadminrec" -> ((df: DataFrame) => {
      val df1 = df.repartition()
      val groups = Window.partitionBy(df1("MAR_ENC_CSN"),df1("PAT_ID"),df1("MAR_BILLING_PROV_ID")).orderBy(df1("MAR_ENC_CSN"))
      df1.withColumn("mar_rn", row_number.over(groups))
        .filter("mar_rn=1 and coalesce(mar_billing_prov_id, '-1') <> '-1' and coalesce(pat_id, '-1') <> '-1' and coalesce(mar_enc_csn, '-1') <> '-1'")
        .distinct()
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinicalencounter")
      .join(dfs("medadminrec"),
        dfs("clinicalencounter")("ENCOUNTERID") === dfs("medadminrec")("MAR_ENC_CSN"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("medadminrec"),
    "ENCOUNTERID" -> mapFrom("MAR_ENC_CSN", nullIf=Seq("-1")),
    "PATIENTID" -> mapFrom("PAT_ID", nullIf=Seq("-1")),
    "ENCOUNTERTIME" -> mapFrom("ARRIVALTIME"),
    "PROVIDERID" -> mapFrom("MAR_BILLING_PROV_ID", nullIf = Seq("-1")),
    "PROVIDERROLE" -> literal("Mar Billing Provider")
  )

}

//test
// val mbp = new EncounterproviderMarbillingprovider(cfg); val mp = build(mbp); mp.show(false) ; mp.count;
