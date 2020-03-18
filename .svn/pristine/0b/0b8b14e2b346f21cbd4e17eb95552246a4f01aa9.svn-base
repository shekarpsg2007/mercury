package com.humedica.mercury.etl.epic_v2.patientaddress

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.{EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Auto-generated on 01/27/2017
 */


class PatientaddressPatreg(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patreg", "zh_state")

  columnSelect = Map(
    "patreg" -> List("STATE_C", "UPDATE_DATE", "PAT_ID", "ZIP", "ADD_LINE_1", "ADD_LINE_2", "CITY"),
    "zh_state" -> List("STATE_C", "ABBR")
  )



  join = (dfs: Map[String, DataFrame]) => {
    dfs("patreg")
      .join(dfs("zh_state"), Seq("STATE_C"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    df.groupBy("PAT_ID", "ADD_LINE_1", "ADD_LINE_2", "CITY", "ABBR", "ZIP")
      .agg(max("UPDATE_DATE").as("ADDRESS_DATE"))
  }




  map = Map(
    "DATASRC" -> literal("patreg"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ADDRESS_DATE" -> mapFrom("ADDRESS_DATE"),
    "STATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        regexp_replace(df("ABBR"), "[-0-9 ]", ""))
    }),
    "ZIPCODE" -> standardizeZip("ZIP", zip5=true),
    "ADDRESS_LINE1" -> mapFrom("ADD_LINE_1"),
    "ADDRESS_LINE2" -> mapFrom("ADD_LINE_2"),
    "CITY" -> mapFrom("CITY")
  )


  afterMap = (df: DataFrame) => {
    df.filter("PATIENTID is not null and coalesce(ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE) is not null " +
      "and length(ZIPCODE) in (5,9,10) and (length(STATE) = 2 or STATE is null)")

  }


}

