package com.humedica.mercury.etl.asent.labresult

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class LabresultVitals(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_unit_code_de",
    "as_vitals",
    "cdr.zcm_obstype_code")

  columnSelect = Map(
    "as_zc_unit_code_de" -> List("ENTRYMNEMONIC", "ID", "ENTRYNAME"),
    "as_vitals" -> List("CHILD_ID", "VITAL_ENTRY_NAME", "PATIENT_MRN", "ENCOUNTER_ID", "UNITS_ID", "PERFORMED_DATETIME", "VITAL_VALUE", "STATUS_ID", "LAST_UPDATED_DATE"),
    "cdr.zcm_obstype_code" -> List("OBSTYPE", "OBSCODE", "OBSTYPE_STD_UNITS", "LOCALUNIT", "GROUPID", "DATASRC")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'vitals'"),
    "as_vitals" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("CHILD_ID")).orderBy(df("LAST_UPDATED_DATE").desc)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn=1").drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_vitals")
      .join(dfs("as_zc_unit_code_de"), dfs("as_vitals")("UNITS_ID") === dfs("as_zc_unit_code_de")("ID"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    df.withColumn("LOCALRESULT_25", when(!df("VITAL_VALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
      when(locate(" ", df("VITAL_VALUE"), 25) === 0, expr("substr(VITAL_VALUE,1,length(VITAL_VALUE))"))
        .otherwise(expr("substr(VITAL_VALUE,1,locate(' ', VITAL_VALUE, 25))"))).otherwise(null))
      .withColumn("LOCALRESULT_NUMERIC", when(df("VITAL_VALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), df("VITAL_VALUE")).otherwise(null))
  }

  map = Map(
    "DATASRC" -> literal("vitals"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "DATEAVAILABLE" -> mapFrom("PERFORMED_DATETIME"),
    "DATECOLLECTED" -> mapFrom("PERFORMED_DATETIME"),
    "LABRESULT_DATE" -> mapFrom("PERFORMED_DATETIME"),
    "LOCALCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, concat_ws("_", df("VITAL_ENTRY_NAME"), df("ENTRYNAME")))
    }),
    "LOCALNAME" -> mapFrom("LOCALCODE"),
    "LOCALUNITS" -> mapFrom("ENTRYMNEMONIC"),
    "LABRESULTID" -> mapFrom("CHILD_ID"),
    "LOCALUNITID" -> mapFrom("UNITS_ID"),
    "LOCALRESULT" -> mapFrom("VITAL_VALUE"),
    "LOCALRESULT_INFERRED" -> extract_value(),
    "LOCALRESULT_NUMERIC" -> mapFrom("LOCALRESULT_NUMERIC"),
    "LOCALUNITS" -> mapFrom("ENTRYMNEMONIC")
  )

  afterMap = (df: DataFrame) => {
    val obs = table("cdr.zcm_obstype_code").withColumnRenamed("GROUPID", "GROUPID2").withColumnRenamed("DATASRC", "DATASRC2")
    val joined = df.join(obs, obs("GROUPID2") === df("GROUPID") && obs("DATASRC2") === df("DATASRC") && obs("OBSCODE") === df("LOCALCODE"), "inner")
    val groups = Window.partitionBy(joined("LABRESULTID")).orderBy(joined("DATECOLLECTED").desc)
    val df2 = joined.withColumn("LABROW", row_number.over(groups))
    df2.filter("LABROW =1 AND OBSTYPE='LABRESULT' AND VITAL_VALUE is not null")
  }
}
