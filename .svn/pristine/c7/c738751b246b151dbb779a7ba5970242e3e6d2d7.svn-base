package com.humedica.mercury.etl.epic_v2.medmapsource

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Functions

class MedmapsourceImmunization (config: Map[String, String]) extends EntitySource(config: Map[String, String]){

  tables = List("tempimmunization:epic_v2.immunization.ImmunizationImmunization")

  columns = List("DATASRC", "LOCALMEDCODE", "LOCALDESCRIPTION", "HAS_NDC", "NO_NDC", "NUM_RECS")

  join = noJoin()

  map = Map(
    "DATASRC" -> literal("immunization"),
    "LOCALMEDCODE" -> mapFrom("LOCALIMMUNIZATIONCD"),
    "LOCALDESCRIPTION" -> mapFrom("LOCALIMMUNIZATIONDESC"),
    "LOCALNDC" -> literal(null),
    "HAS_NDC" -> literal("0"),
    "NO_NDC" -> literal("0"),
    "NUM_RECS" -> literal("0")
  )

  afterMap = (df: DataFrame) => {
    val grouped_df = df.groupBy(df("LOCALMEDCODE"), df("LOCALDESCRIPTION"), df("DATASRC"))
      .agg(sum(when(df("LOCALNDC").isNotNull, 1).otherwise(0)).alias("HAS_NDC")
           ,sum(when(df("LOCALNDC").isNull, 1).otherwise(0)).alias("NO_NDC")
          ,count("*").alias("NUM_RECS"))
    grouped_df
  }

}
// TEST
// val i = new MedmapsourceImmunization(cfg) ; val imm = build(i) ; imm.orderBy(desc("NUM_RECS")).show
