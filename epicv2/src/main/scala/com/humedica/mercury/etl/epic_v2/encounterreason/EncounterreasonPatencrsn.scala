package com.humedica.mercury.etl.epic_v2.encounterreason

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class EncounterreasonPatencrsn(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("pat_enc_rsn")

      beforeJoin = Map(
        "pat_enc_rsn" -> ((df: DataFrame) => {
          df.filter("pat_id is not null and enc_reason_name is not null")
        })
      )

      join = noJoin()

      map = Map(
        "DATASRC" -> literal("pat_enc_rsn"),
        "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "REASONTIME" -> mapFrom("CONTACT_DATE"),
        "LOCALREASONTEXT" -> cascadeFrom(Seq("ENC_REASON_NAME", "ENC_REASON_OTHER"))
      )

 }