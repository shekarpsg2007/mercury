package com.humedica.mercury.etl.asent.rxordersandprescriptions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._

class RxordersandprescriptionsErx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_erx",
    "as_zc_medication_de")


  columnSelect = Map(
    "as_erx" -> List("THERAPY_END_DATE", "PRESCRIBEDBYID", "FREQUENCY_UNITS_ID", "ORDER_DATE", "ORDER_STATUS_ID",
      "PATIENT_MRN", "CHILD_MEDICATION_ID", "MEDICATION_NDC", "DAYS_SUPPLY", "THERAPY_START_DATE",
      "ENCOUNTER_ID", "EXP_DATE", "SITE_ID", "DAW_FLAG", "FREQUENCY_UNITS_ID",
      "DRUG_FORM", "DRUG_DESCRIPTION", "DAYS_TO_TAKE", "DRUG_FORM", "MED_DICT_DE",
      "ORDERED_BY_ID", "DOSE", "DRUG_STRENGTH", "UNITS_OF_MEASURE", "DOSE",
      "REFILLS", "ORDER_STATUS_ID", "QTY_TO_DISPENSE", "FREE_TEXT_SIG", "LAST_UPDATED_DATE", "PRESCRIBE_ACTION_ID", "MANAGED_BY_ID", "PARENT_MEDICATION_ID"),
    "as_zc_medication_de" -> List("GPI_TC3", "NDC", "ROUTEOFADMINDE", "ID")
  )

  beforeJoin = Map(
    "as_erx" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("CHILD_MEDICATION_ID")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
      df.withColumn("rn_ae1", row_number.over(groups))
    }))

  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_erx")
      .join(dfs("as_zc_medication_de"), dfs("as_zc_medication_de")("ID") === dfs("as_erx")("MED_DICT_DE"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val fil = df.filter("rn_ae1 = 1 and (ORDER_STATUS_ID not in ('5', '10') or ORDER_STATUS_ID is null)").withColumn("ORDER_STATUS_SORT", when(df("ORDER_STATUS_ID").isNotNull, lit("1")).otherwise(lit("0")))
    val groups = Window.partitionBy(fil("CHILD_MEDICATION_ID")).orderBy(fil("LAST_UPDATED_DATE").desc_nulls_last)
    val groups1 = Window.partitionBy(fil("CHILD_MEDICATION_ID")).orderBy(fil("ORDER_STATUS_SORT").desc_nulls_last, fil("LAST_UPDATED_DATE").desc_nulls_last)
    val groups2 = Window.partitionBy(fil("PARENT_MEDICATION_ID"))
    val addcolumn = fil.withColumn("rn", row_number.over(groups))
      .withColumn("LASTKNOWNSTATUS", first("ORDER_STATUS_ID").over(groups1))
      .withColumn("ENTERED_IN_ERROR", sum(when(fil("ORDER_STATUS_ID") === "5", 1).otherwise(0)).over(groups2))
      .withColumn("PRESCRIBE_ACTION_FLAG", sum(when(fil("PRESCRIBE_ACTION_ID") === "9", 0).otherwise(1)).over(groups2))
      .withColumn("ORDER_STATUS_FLAG", min(when(fil("ORDER_STATUS_ID").isin("18", "19"), 0).otherwise(1)).over(groups2))
    addcolumn.filter("rn = '1'")
  }

  map = Map(
    "DATASRC" -> literal("erx"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
    "FACILITYID" -> mapFrom("SITE_ID"),
    "RXID" -> mapFrom("CHILD_MEDICATION_ID"),
    "LOCALMEDCODE" -> mapFrom("MED_DICT_DE"),
    "LOCALDESCRIPTION" -> mapFrom("DRUG_DESCRIPTION"),
    "ALTMEDCODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, trim(df("MEDICATION_NDC")))
    }),
    "LOCALSTRENGTHPERDOSEUNIT" -> mapFrom("DRUG_STRENGTH"),
    "LOCALSTRENGTHUNIT" -> mapFrom("UNITS_OF_MEASURE"),
    "LOCALROUTE" -> mapFrom("ROUTEOFADMINDE", nullIf = Seq("0")),
    "FILLNUM" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("REFILLS").rlike("^[+-]?\\d+\\.?\\d*$"), df("REFILLS")).otherwise(9999))
    }),
    "QUANTITYPERFILL" -> mapFrom("QTY_TO_DISPENSE"),
    "LOCALPROVIDERID" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, coalesce(when(df("ORDERED_BY_ID") === 0, null).otherwise(df("ORDERED_BY_ID")),
        when(df("MANAGED_BY_ID") === 0, null).otherwise(df("MANAGED_BY_ID"))))
    }),
    "ISSUEDATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(coalesce(df("ORDER_STATUS_ID"), df("LASTKNOWNSTATUS"), lit("15")) === "15",
        when(df("PRESCRIBE_ACTION_ID").isin("2", "4", "12", "8", "15", "19", "9"), df("ORDER_DATE"))
          .otherwise(coalesce(df("THERAPY_START_DATE"), df("ORDER_DATE"))))
        .otherwise(df("THERAPY_START_DATE")))
    }),
    "SIGNATURE" -> mapFrom("FREE_TEXT_SIG"),
    "ORDERTYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("PRESCRIBE_ACTION_FLAG") === "0", "CH002046")
          .when(df("ORDER_STATUS_FLAG") === "0", "CH002045")
          .otherwise("CH002047"))
    }),
    "ORDERSTATUS" -> mapFrom("ORDER_STATUS_ID"),
    "VENUE" -> literal("1"),
    "LOCALDAYSUPPLIED" -> mapFrom("DAYS_SUPPLY"),
    "LOCALFORM" -> mapFrom("DRUG_FORM"),
    "LOCALQTYOFDOSEUNIT" -> mapFrom("DOSE"),
    "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DOSE").rlike("^[+-]?\\d+\\.?\\d*$"), df("DOSE")).otherwise(null)
        .multiply(when(df("DRUG_STRENGTH").rlike("^[+-]?\\d+\\.?\\d*$"), df("DRUG_STRENGTH")).otherwise(null)))
    }
      ),
    "LOCALDOSEFREQ" -> mapFrom("FREQUENCY_UNITS_ID", nullIf = Seq("0"), prefix = config(CLIENT_DS_ID) + "."),
    "LOCALDURATION" -> mapFrom("DAYS_TO_TAKE"),
    "LOCALNDC" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, trim(when(df("NDC").isNotNull, substring(regexp_replace(df("NDC"), "-", ""), 1, 11))
        .otherwise(substring(regexp_replace(df("MEDICATION_NDC"), "-", ""), 1, 11))))
    }),
    "LOCALGPI" -> mapFrom("GPI_TC3"),
    "LOCALDAW" -> mapFrom("DAW_FLAG"),
    "ORDERVSPRESCRIPTION" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ORDER_STATUS_ID").isin("18", "19"), "O").otherwise("P"))
    }),
    "LOCALDOSEUNIT" -> mapFrom("DRUG_FORM")
  )

  afterMap = (df: DataFrame) => {
    val addColumn = df.withColumn("DISCONTINUEDATE", when(substring(df("THERAPY_END_DATE"), 1, 10) === "1900-01-01", null).otherwise(
      when(concat(substring(df("THERAPY_END_DATE"), 1, 10), lit(" 00:00:00")) lt substring(df("ISSUEDATE"), 1, 19), null).otherwise(concat(substring(df("THERAPY_END_DATE"), 1, 10), lit(" 00:00:00")))))
      .withColumn("EXPIREDATE", when(df("EXP_DATE") lt df("ISSUEDATE"), null).otherwise(df("EXP_DATE")))
    addColumn.filter("ISSUEDATE is not null and PATIENTID is not null")
  }
}

// Test
// val brx = new RxordersandprescriptionsErx(cfg); val rx = build(brx); rx.show(false) ; rx.count()
