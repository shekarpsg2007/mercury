package com.humedica.mercury.etl.epic_v2.medmapsource

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Functions

class MedmapsourceMedorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("tempmedmaporders:epic_v2.rxordersandprescriptions.RxordersandprescriptionsMedorders",
                    "zh_claritymed")

      columns = List("DATASRC", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALNDC", "HAS_NDC", "NO_NDC", "NUM_RECS")

      beforeJoin = Map(
        "tempmedmaporders" -> ((df: DataFrame) => {
          val grouped_meds = df.filter("LOCALMEDCODE != '-1'").groupBy(df("LOCALMEDCODE"), df("LOCALDESCRIPTION"))
            .agg(count("*").alias("CT"))
          grouped_meds
        })
      )

      join = (dfs: Map[String, DataFrame]) => {
        dfs("tempmedmaporders")
          .join(dfs("zh_claritymed"), dfs("tempmedmaporders")("LOCALMEDCODE") === dfs("zh_claritymed")("medication_id"), "left_outer")
      }

      map = Map(
        "DATASRC" -> literal("medorders"),
        "LOCALMEDCODE" -> mapFrom("LOCALMEDCODE"),
        "LOCALDESCRIPTION" -> mapFrom("LOCALDESCRIPTION"),
        "LOCALNDC" -> mapFrom("RAW_11_DIGIT_NDC"),
        "HAS_NDC" -> literal("0"),
        "NO_NDC" -> literal("0"),
        "NUM_RECS" -> literal("0")
      )

      afterMap = (df: DataFrame) => {
        val grouped_df = df.groupBy(df("LOCALMEDCODE"), df("LOCALDESCRIPTION"), df("DATASRC"), df("LOCALNDC"))
          .agg(sum(when(df("raw_11_digit_ndc").isNull, df("CT")).otherwise(0)).alias("NO_NDC")
              ,sum(when(df("raw_11_digit_ndc").isNotNull, df("CT")).otherwise(0)).alias("HAS_NDC")
              ,sum("CT").alias("NUM_RECS"))
        grouped_df
      }

 }

// TEST
// val m = new MedmapsourceMedorders(cfg) ; val mm = build(m) ; mm.orderBy(desc("NUM_RECS")).show
