package com.humedica.mercury.etl.cerner_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Engine
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._


/**
 * Auto-generated on 08/09/2018
 */

class ObservationClinicalevent(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event", "zh_code_value", "cdr.zcm_obstype_code", "cdr.map_predicate_values")

  columnSelect = Map(
    "clinical_event" -> List("PERSON_ID", "ENCNTR_ID", "EVENT_CD", "RESULT_VAL", "RESULT_UNITS_CD", "EVENT_END_DT_TM",
      "EVENT_CLASS_CD", "RESULT_STATUS_CD", "UPDT_DT_TM", "EVENT_TAG"),
    "zh_code_value" -> List("CODE_VALUE", "DESCRIPTION"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "LOCALUNIT", "OBSTYPE_STD_UNITS", "CUI")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val list_height_source = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "RESULTSNUMERIC", "OBSERVATION", "RESULTSNUMERIC", "EVENT_CD_SRC")
      val list_result_val = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "RESULTSNUMERIC", "OBSERVATION", "RESULTSNUMERIC", "RESULT_VAL")
      val list_height_meas = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "RESULTSNUMERIC", "OBSERVATION", "RESULTSNUMERIC", "EVENT_CD_MSMT")
      val height_encs = df.filter("event_cd in (" + list_height_source + ") and result_val in (" + list_result_val + ")")
        .select("ENCNTR_ID")
        .withColumnRenamed("ENCNTR_ID", "ENCNTR_ID_height")
        .distinct()

      df.join(height_encs, df("ENCNTR_ID") === height_encs("ENCNTR_ID_height"), "left_outer")
        .filter("encntr_id_height is null or (encntr_id_height is not null and event_cd not in (" + list_height_meas + "))")
        .withColumn("LOCALCODE", concat(lit(config(CLIENT_DS_ID) + "."), df("EVENT_CD"), lit("_"), df("RESULT_UNITS_CD")))
        .drop("ENCNTR_ID_height")
    }),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("groupid = '" + config(GROUP) + "' and lower(datasrc) = 'clinical_event' and obstype <> 'LABRESULT'")
        .drop("GROUPID", "DATASRC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinical_event")
      .join(dfs("cdr.zcm_obstype_code"), lower(dfs("clinical_event")("LOCALCODE")) === lower(dfs("cdr.zcm_obstype_code")("OBSCODE")), "inner")
      .join(dfs("zh_code_value"), dfs("clinical_event")("EVENT_CD") === dfs("zh_code_value")("CODE_VALUE"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("clinical_event"),
    "LOCALCODE" -> mapFrom("LOCALCODE"),
    "OBSDATE" -> mapFrom("EVENT_END_DT_TM"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "STATUSCODE" -> mapFrom("RESULT_STATUS_CD"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALRESULT" -> ((col: String, df: DataFrame) => {
      val list_date = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DATE", "OBSERVATION", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      val list_txt = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "TXT", "OBSERVATION", "CLINICAL_EVENT", "EVENT_CLASS_CD")
      df.withColumn(col,
        substring(when(df("CUI") === lit("CH002104"), concat_ws(".", df("DESCRIPTION"), df("EVENT_TAG")))
          .when(df("CUI") === lit("CH002709") && df("EVENT_CLASS_CD").isin(list_date: _*), substring(df("RESULT_VAL"), 3, 8))
          .when(df("CUI") === lit("CH002754") && df("EVENT_CLASS_CD") === lit("236"), df("RESULT_VAL"))
          .when(df("CUI") =!= lit("CH001150") && df("EVENT_CLASS_CD").isin(list_txt: _*), concat_ws("", df("DESCRIPTION"), lit("."), df("RESULT_VAL")))
          .otherwise(df("RESULT_VAL"))
        ,1, 255))
    }),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "LOCAL_OBS_UNIT" -> mapFrom("LOCALUNIT"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val list_result_status = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "OBSERVATION", "CLINICAL_EVENT", "RESULT_STATUS_CD")
    val groups = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("LOCALCODE"), df("OBSDATE"), df("OBSTYPE"))
      .orderBy(df("UPDT_DT_TM").desc_nulls_last)
    val dedupe = df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1 and patientid is not null and obsdate is not null and (statuscode is null or statuscode not in (" + list_result_status + "))")
      .drop("rn")

    val list_gest_age_weeks = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "OBSERVATION", "GESTATIONAL_AGE", "WEEKS")
    val list_gest_age_days = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "OBSERVATION", "GESTATIONAL_AGE", "DAYS")

    val cols = Engine.schema.getStringList("Observation").asScala.map(_.split("-")(0).toUpperCase())

    val df_no_gest = dedupe.filter("localcode not in (" + list_gest_age_days + "," + list_gest_age_weeks + ")")
      .select(cols.map(col): _*)
    val df_gest_weeks = dedupe.filter("localcode in (" + list_gest_age_weeks + ")")
      .select(cols.map(col): _*)
    val df_gest_days = dedupe.filter("localcode in (" + list_gest_age_days + ")")
      .select(cols.map(col): _*)

    val cols_w = df_gest_weeks.columns.map(c => df_gest_weeks(c).as(c + "_w"))
    val cols_d = df_gest_days.columns.map(c => df_gest_days(c).as(c + "_d"))

    val df_gest_weeks2 = df_gest_weeks.select(cols_w: _*)
    val df_gest_days2 = df_gest_days.select(cols_d: _*)

    val joined = df_gest_weeks2.join(df_gest_days2, df_gest_weeks2("PATIENTID_w") === df_gest_days2("PATIENTID_d")
      && df_gest_weeks2("ENCOUNTERID_w") === df_gest_days2("ENCOUNTERID_d")
      , "full_outer")

    val joined2 = joined.withColumn("GROUPID", coalesce(joined("GROUPID_w"), joined("GROUPID_d")))
      .withColumn("DATASRC", coalesce(joined("DATASRC_w"), joined("DATASRC_d")))
      .withColumn("FACILITYID", coalesce(joined("FACILITYID_w"), joined("FACILITYID_d")))
      .withColumn("ENCOUNTERID", coalesce(joined("ENCOUNTERID_w"), joined("ENCOUNTERID_d")))
      .withColumn("PATIENTID", coalesce(joined("PATIENTID_w"), joined("PATIENTID_d")))
      .withColumn("OBSDATE", coalesce(joined("OBSDATE_w"), joined("OBSDATE_d")))
      .withColumn("OBSTYPE", coalesce(joined("OBSTYPE_w"), joined("OBSTYPE_d")))
      .withColumn("STATUSCODE", coalesce(joined("STATUSCODE_w"), joined("STATUSCODE_d")))
      .withColumn("RESULTDATE", coalesce(joined("RESULTDATE_w"), joined("RESULTDATE_d")))
      .withColumn("CLIENT_DS_ID", coalesce(joined("CLIENT_DS_ID_w"), joined("CLIENT_DS_ID_d")))
      .withColumn("OBSRESULT", coalesce(joined("OBSRESULT_w"), joined("OBSRESULT_d")))
      .withColumn("HGPID", coalesce(joined("HGPID_w"), joined("HGPID_d")))
      .withColumn("GRP_MPI", coalesce(joined("GRP_MPI_w"), joined("GRP_MPI_d")))
      .withColumn("STD_OBS_UNIT", coalesce(joined("STD_OBS_UNIT_w"), joined("STD_OBS_UNIT_d")))
      .withColumn("ROW_SOURCE", coalesce(joined("ROW_SOURCE_w"), joined("ROW_SOURCE_d")))
      .withColumn("MODIFIED_DATE", coalesce(joined("MODIFIED_DATE_w"), joined("MODIFIED_DATE_d")))
      .withColumn("LOCALCODE", trim(concat_ws(" ", joined("LOCALCODE_w"), joined("LOCALCODE_d"))))
      .withColumn("LOCALRESULT", concat(coalesce(joined("LOCALRESULT_w"), lit("0")), lit("W"), coalesce(joined("LOCALRESULT_d"), lit("0")), lit("D")))
      .withColumn("LOCAL_OBS_UNIT", lit("week,day"))
      .select(cols.map(col): _*)
      .distinct

    df_no_gest.union(joined2)
  }

}