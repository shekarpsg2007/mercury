package com.humedica.mercury.etl.asent.observation


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class ObservationVitalsphq(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_vitals", "as_zc_unit_code_de", "cdr.zcm_obstype_code")

  columnSelect = Map(
    "as_vitals" -> List("VITAL_ENTRY_NAME", "PERFORMED_DATETIME", "PATIENT_MRN", "STATUS_ID", "ENCOUNTER_ID", "FINDING_ANSWER_DET", "UNITS_ID", "CHILD_ID", "LAST_UPDATED_DATE"),
    "as_zc_unit_code_de" -> List("ENTRYNAME", "ID"),
    "cdr.zcm_obstype_code" -> List("OBSCODE", "GROUPID", "DATASRC", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "DATATYPE")
  )

  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> includeIf("GROUPID = '" + config(GROUP) + "' AND DATASRC = 'vitals_phq'"),
    "as_vitals" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("CHILD_ID")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
      val fil = df.withColumn("rn", row_number.over(groups))
        .filter("rn =1 AND STATUS_ID != '6' AND PATIENT_MRN IS NOT NULL AND PERFORMED_DATETIME IS NOT NULL AND FINDING_ANSWER_DET IS NOT NULL").drop("rn")
      val zc_unit = table("as_zc_unit_code_de")
      val prejoin = fil.join(zc_unit, fil("UNITS_ID") === zc_unit("ID"), "left_outer")
      prejoin.withColumn("LOCALCODE", concat_ws(".", lit(config(CLIENT_DS_ID)), concat_ws("_", prejoin("VITAL_ENTRY_NAME"), prejoin("ENTRYNAME"))))
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_vitals")
      .join(dfs("cdr.zcm_obstype_code"), dfs("cdr.zcm_obstype_code")("obscode") === dfs("as_vitals")("localcode"), "inner")
  }

  map = Map(
    "DATASRC" -> literal("vitals_phq"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "OBSDATE" -> mapFrom("PERFORMED_DATETIME"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "STATUSCODE" -> mapFrom("STATUS_ID"),
    "LOCALRESULT" -> ((col, df) => {
      df.withColumn(col,
        when(df("FINDING_ANSWER_DET").isNotNull && df("DATATYPE") === lit("CV"), concat_ws(".", lit(config(CLIENT_DS_ID)), df("FINDING_ANSWER_DET"))).otherwise(df("FINDING_ANSWER_DET")))
    }),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"), df("OBSDATE")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
    df.withColumn("rw", row_number.over(groups))
      .filter("rw=1")
  }

}
