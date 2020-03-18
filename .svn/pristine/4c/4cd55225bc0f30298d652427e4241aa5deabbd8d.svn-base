package com.humedica.mercury.etl.asent.patientreportedmeds

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class PatientreportedmedsErx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_erx",
    "as_zc_medication_de")

  columnSelect = Map(
    "as_erx" -> List("PATIENT_MRN", "CHILD_MEDICATION_ID", "ORDER_DATE", "DRUG_FORM", "ENCOUNTER_ID",
      "DRUG_FORM", "ORDERED_BY_ID", "MANAGED_BY_ID", "DOSE", "DRUG_STRENGTH",
      "UNITS_OF_MEASURE", "DOSE", "THERAPY_END_DATE", "DRUG_DESCRIPTION", "MED_DICT_DE",
      "ORDER_DATE", "MEDICATION_NDC", "ORDER_STATUS_ID", "PRESCRIBE_ACTION_ID", "PARENT_MEDICATION_ID"),
    "as_zc_medication_de" -> List("GPI_TC3", "ROUTEOFADMINDE", "ID", "NDC")
  )

  beforeJoin = Map(
    "as_erx" -> ((df: DataFrame) => {
      val toDrop = df.filter("ORDER_STATUS_ID not in ('15','8') OR prescribe_action_id != '9'").withColumn("TEMP", lit("1")).select("PARENT_MEDICATION_ID", "TEMP")
      val joined = df.join(toDrop, Seq("PARENT_MEDICATION_ID"), "left_outer")
      joined.filter("TEMP is null OR TEMP != '1'")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_erx")
      .join(dfs("as_zc_medication_de"), dfs("as_zc_medication_de")("ID") === dfs("as_erx")("med_dict_de"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("erx"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "REPORTEDMEDID" -> mapFrom("CHILD_MEDICATION_ID"),
    "ACTIONTIME" -> mapFrom("ORDER_DATE", nullIf = Seq("1900-01-01")),
    "LOCALCATEGORYCODE" -> ((col, df) => df.withColumn(col, when(df("ORDER_STATUS_ID").equalTo("8"), lit("2")).otherwise(lit("1")))),
    "LOCALDOSEUNIT" -> mapFrom("DRUG_FORM"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "LOCALFORM" -> mapFrom("DRUG_FORM"),
    "LOCALGPI" -> mapFrom("GPI_TC3"),
    "LOCALPROVIDERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(when(df("ORDERED_BY_ID") === 0, null).otherwise(df("ORDERED_BY_ID")),
        when(df("MANAGED_BY_ID") === 0, null).otherwise(df("MANAGED_BY_ID"))))
    }),
    "LOCALQTYOFDOSEUNIT" -> mapFrom("DOSE"),
    "LOCALROUTE" -> mapFrom("ROUTEOFADMINDE"),
    "LOCALSTRENGTHPERDOSEUNIT" -> mapFrom("DRUG_STRENGTH"),
    "LOCALSTRENGTHUNIT" -> mapFrom("UNITS_OF_MEASURE"),
    "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DOSE").rlike("^[+-]?\\d+\\.?\\d*$"), df("DOSE")).otherwise(null)
        .multiply(when(df("DRUG_STRENGTH").rlike("^[+-]?\\d+\\.?\\d*$"), df("DRUG_STRENGTH")).otherwise(null)))
    }),
    "DISCONTINUEDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("THERAPY_END_DATE") === "1900-01-01 00:00:00.0", null)
        .otherwise(concat(substring(df("THERAPY_END_DATE"), 1, 11), lit("00:00:00"))))
    }),
    "LOCALDRUGDESCRIPTION" -> mapFrom("DRUG_DESCRIPTION"),
    "LOCALMEDCODE" -> mapFrom("MED_DICT_DE"),
    "LOCALNDC" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(regexp_replace(df("NDC"), "[0-]", "")) > 0, substring(regexp_replace(df("NDC"), "-", ""), 1, 11))
        .when(length(regexp_replace(df("MEDICATION_NDC"), "[0-]", "")) > 0, substring(regexp_replace(df("MEDICATION_NDC"), "-", ""), 1, 11))
        .otherwise(null))
    }),
    "MEDREPORTEDTIME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ORDER_DATE") === "1900-01-01 00:00:00.0", null)
        .otherwise(concat(substring(df("ORDER_DATE"), 1, 11), lit("00:00:00"))))
    })
  )

  beforeJoinExceptions =
    Map("H969222_AS_ENT" -> Map("as_erx" -> ((df: DataFrame) => {
      val toDrop = df.filter("ORDER_STATUS_ID not in ('19','8') OR prescribe_action_id != '9'").withColumn("TEMP", lit("1")).select("PARENT_MEDICATION_ID", "TEMP")
      val joined = df.join(toDrop, Seq("PARENT_MEDICATION_ID"), "left_outer")
      joined.filter("TEMP is null OR TEMP != '1'")
    })))


  afterMap = (df: DataFrame) => {
    df.filter("PATIENTID is not null")
  }
}