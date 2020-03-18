package com.humedica.mercury.etl.asent.patientreportedmeds


import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class PatientreportedmedsReconciledlist(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_reconciled_list",
    "as_erx",
    "as_zc_medication_de")

  columnSelect = Map(
    "as_reconciled_list" -> List("ITEMCHILDID", "ENCOUNTERID", "RECORDEDDTTM", "PATIENTID", "ITEMTYPE"),
    "as_erx" -> List("PATIENT_MRN", "DRUG_FORM", "DRUG_FORM", "DOSE", "ROUTOFADMINISTRATIONDE",
      "DRUG_STRENGTH", "UNITS_OF_MEASURE", "DOSE", "DRUG_DESCRIPTION", "MED_DICT_DE",
      "NDC", "CHILD_MEDICATION_ID", "MEDICATION_NDC", "THERAPY_END_DATE", "LAST_UPDATED_DATE"),
    "as_zc_medication_de" -> List("GPI_TC3", "ID")
  )

  beforeJoin = Map(
    "as_erx" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("CHILD_MEDICATION_ID")).orderBy(df("LAST_UPDATED_DATE").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups)).filter("rn=1").drop("rn")
    }),
    "as_reconciled_list" -> ((df: DataFrame) => {
      val fil = df.filter("RECORDEDDTTM IS NOT NULL AND ITEMTYPE = 'ME'")
      val groups = Window.partitionBy(fil("PATIENTID"), fil("ENCOUNTERID"), fil("ITEMCHILDID"), fil("RECORDEDDTTM")).orderBy(fil("PATIENTID").desc_nulls_last)
      fil.withColumn("rn", row_number.over(groups)).filter("rn=1").drop("rn")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("as_reconciled_list")
      .join(dfs("as_erx"), dfs("as_erx")("CHILD_MEDICATION_ID") === dfs("as_reconciled_list")("ITEMCHILDID"), "inner")
      .join(dfs("as_zc_medication_de"), dfs("as_zc_medication_de")("ID") === dfs("as_erx")("MED_DICT_DE"), "inner")
  }


  afterJoin = (df: DataFrame) => {
    val fil = df.filter("PATIENT_MRN is not null AND (RecordedDTTM < Therapy_End_Date OR Therapy_End_Date IS NULL)")
    val groups = Window.partitionBy(fil("PATIENTID"), fil("ENCOUNTERID"), fil("ITEMCHILDID"), fil("RECORDEDDTTM")).orderBy(fil("PATIENTID").desc_nulls_last)
    fil.withColumn("rn", row_number.over(groups)).filter("rn=1").drop("rn")
  }

  map = Map(
    "DATASRC" -> literal("reconciled_list"),
    "PATIENTID" -> mapFrom("PATIENT_MRN"),
    "REPORTEDMEDID" -> ((col, df) => df.withColumn(col, concat(df("ITEMCHILDID"), df("ENCOUNTERID")))),
    "LOCALDOSEUNIT" -> mapFrom("DRUG_FORM"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "LOCALFORM" -> mapFrom("DRUG_FORM"),
    "LOCALGPI" -> mapFrom("GPI_TC3"),
    "LOCALQTYOFDOSEUNIT" -> mapFrom("DOSE"),
    "LOCALROUTE" -> mapFrom("ROUTOFADMINISTRATIONDE"),
    "LOCALSTRENGTHPERDOSEUNIT" -> mapFrom("DRUG_STRENGTH"),
    "LOCALSTRENGTHUNIT" -> mapFrom("UNITS_OF_MEASURE"),
    "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DOSE").rlike("^[+-]?\\d+\\.?\\d*$"), df("DOSE")).otherwise(null)
        .multiply(when(df("DRUG_STRENGTH").rlike("^[+-]?\\d+\\.?\\d*$"), df("DRUG_STRENGTH")).otherwise(null)))
    }),
    "LOCALDRUGDESCRIPTION" -> mapFrom("DRUG_DESCRIPTION"),
    "LOCALMEDCODE" -> mapFrom("MED_DICT_DE"),
    "LOCALNDC" -> cascadeFrom(Seq("NDC", "MEDICATION_NDC")),
    "MEDREPORTEDTIME" -> mapFrom("RECORDEDDTTM")
  )


  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("REPORTEDMEDID")).orderBy(df("RECORDEDDTTM").desc_nulls_last)
    val df2 = df.withColumn("rn", row_number.over(groups))
    df2.filter("rn=1 and PATIENTID is not null")
  }

}
