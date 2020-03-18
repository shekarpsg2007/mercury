package com.humedica.mercury.etl.athena.patientdetail

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.athena.util.UtilSplitTable

class PatientdetailPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {
  tables = List("patient", "patientrace",
    "temp_patient:athena.util.UtilSplitPatient",
    "temp_fileid_dates:athena.util.UtilFileIdDates",
    "cdr.map_predicate_values"
  )

  columnSelect = Map(
    "patient" -> List("PATIENT_ID", "Test_Patient_YN", "ENTERPRISE_ID", "DOB", "DECEASED_DATE", "CDC_ETHNICITY_CODE",
      "ETHNICITY", "FIRST_NAME", "NEW_PATIENT_ID", "SEX", "PATIENT_STATUS", "ISO_639_2_CODE", "HUM_LANGUAGE",
      "LAST_NAME", "MARITAL_STATUS", "MIDDLE_INITIAL", "CDC_RACE_CODE", "PATIENT_SSN",
      "PATIENT_LAST_SEEN_DATE", "REGISTRATION_DATE", "MOBILE_PHONE", "PATIENT_HOME_PHONE",
      "WORK_PHONE", "EMAIL", "FILEID", "marital_status", "CITY", "HUM_STATE", "ZIP"),
    "patientrace" -> List("patient_id", "fileid", "deleted_datetime", "primary_race_y_n", "race"),
    "temp_fileid_dates" -> List("fileid", "filedate")
  )

  beforeJoin = Map(
    "patient" -> ((df: DataFrame) => {
      val df1 =
        df.filter(
          " not (coalesce (Test_Patient_YN ,'X') = 'Y')")

      df1.withColumnRenamed("UPDT_DT_TM", "update_date")
    }),
    "patientrace" -> ((df: DataFrame) => {

      val df1 = df.filter("deleted_datetime IS NULL AND (primary_race_y_n = 'Y' or primary_race_y_n IS NULL)")
      val groups1 = Window.partitionBy(df1("patient_id")).orderBy(df1("fileid").desc)
      val df2 = df1.withColumn("pat_race_row", row_number.over(groups1))
        .withColumnRenamed("fileid", "fileidpatrace")
      df2.filter("pat_race_row = 1")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {

    val splitjointype = new UtilSplitTable(config).patprovJoinType
    dfs("patient").join(dfs("temp_patient"), Seq("Patient_Id"), splitjointype)
      .join(dfs("patientrace"), Seq("Patient_Id"), "left_outer")
      .join(dfs("temp_fileid_dates"), Seq("fileid"), "left_outer")

  }

  afterJoin = (dfs: DataFrame) => {
    val df = dfs.withColumn("update_date",
      coalesce(dfs("filedate"), dfs("Patient_Last_Seen_Date"), dfs("REGISTRATION_DATE"), current_date())
    ).filter("update_date is not null")


    val groupFirstName = Window.partitionBy(df("patient_id"), upper(df("first_name"))).orderBy(df("filedate").desc_nulls_last, df("fileid").desc_nulls_last)
    val dfFirstName = df.withColumn("first_row", row_number.over(groupFirstName))
    val dfFirstNamefil = dfFirstName.filter("first_row = 1 and first_name is not null")
      .withColumn("Patientdetailtype", lit("FIRST_NAME"))
      .withColumn("Localvalue", dfFirstName("first_name"))
    val dfFName = dfFirstNamefil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val groupLastName = Window.partitionBy(df("patient_id"), upper(df("last_name"))).orderBy(df("filedate").desc_nulls_last, df("fileid").desc_nulls_last)
    val dfLastName = df.withColumn("last_row", row_number.over(groupLastName))
    val dfLastNamefil = dfLastName.filter("last_row = 1 and last_name is not null")
      .withColumn("Patientdetailtype", lit("LAST_NAME"))
      .withColumn("Localvalue", dfLastName("last_name"))
    val dfLname = dfLastNamefil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val groupMiddleName = Window.partitionBy(df("patient_id"), upper(df("middle_initial"))).orderBy(df("filedate").desc_nulls_last, df("fileid").desc_nulls_last)
    val dfMiddleName = df.withColumn("middle_row", row_number.over(groupMiddleName))
    val dfMiddleNamefil = dfMiddleName.filter("middle_row = 1 and middle_initial is not null")
      .withColumn("Patientdetailtype", lit("MIDDLE_NAME"))
      .withColumn("Localvalue", dfMiddleName("middle_initial"))
    val dfMidname = dfMiddleNamefil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val groupGender = Window.partitionBy(df("patient_id"), upper(df("sex"))).orderBy(df("filedate").desc_nulls_last, df("fileid").desc_nulls_last)
    val dfGender = df.withColumn("gender_row", row_number.over(groupGender))
    val dfGenderfil = dfGender.filter("gender_row = 1 and sex is not null")
      .withColumn("Patientdetailtype", lit("GENDER"))
    val dfg = dfGenderfil.withColumn("Localvalue", dfGenderfil("sex"))
    val dfGend = dfg.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val raceDF = df.withColumn("Localvalue", coalesce(df("cdc_race_code"), df("race")))
    val groupRace = Window.partitionBy(raceDF("patient_id"), upper(coalesce(raceDF("race"), raceDF("cdc_race_code")))).orderBy(raceDF("filedate").desc_nulls_last, raceDF("fileid").desc_nulls_last)
    val dfRace = raceDF.withColumn("race_row", row_number.over(groupRace))
    val dfRacefil = dfRace.filter("race_row = 1 and Localvalue is not null")
      .withColumn("Patientdetailtype", lit("RACE"))
    val dfRc = dfRacefil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val ethnicDF = df.withColumn("Localvalue", coalesce(df("cdc_ethnicity_code"), df("ethnicity")))
    val groupEthicity = Window.partitionBy(ethnicDF("patient_id"), ethnicDF("Localvalue")).orderBy(ethnicDF("filedate").desc_nulls_last, ethnicDF("fileid").desc_nulls_last)
    val dfEthnicity = ethnicDF.withColumn("ethnicity_row", row_number.over(groupEthicity))
    val dfEthinicityfil = dfEthnicity.filter("ethnicity_row = 1 and Localvalue is not null")
      .withColumn("Patientdetailtype", lit("ETHNICITY"))
    val dfEth = dfEthinicityfil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val langDF = df.withColumn("Localvalue", coalesce(df("iso_639_2_code"), df("hum_language")))
    val groupLang = Window.partitionBy(langDF("patient_id"), langDF("Localvalue")).orderBy(langDF("filedate").desc_nulls_last, langDF("fileid").desc_nulls_last)
    val dfLanguage = langDF.withColumn("language_row", row_number.over(groupLang))
    val dfLanguagefil = dfLanguage.filter("language_row = 1 and Localvalue is not null")
      .withColumn("Patientdetailtype", lit("LANGUAGE"))
    val dfLang = dfLanguagefil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val maritalDF = df.withColumn("Localvalue", df("marital_status"))
    val groupMarital = Window.partitionBy(maritalDF("patient_id"), upper(maritalDF("Localvalue"))).orderBy(maritalDF("filedate").desc_nulls_last, maritalDF("fileid").desc_nulls_last)
    val dfMartial = maritalDF.withColumn("mstatus_row", row_number.over(groupMarital))
    val dfMaritalfil = dfMartial.filter("mstatus_row = 1 and Localvalue is not null")
      .withColumn("Patientdetailtype", lit("MARITAL"))
    val dfMart = dfMaritalfil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val cityDF = df.withColumn("Localvalue", df("city"))
    val groupCity = Window.partitionBy(cityDF("patient_id"), upper(cityDF("Localvalue"))).orderBy(cityDF("filedate").desc_nulls_last, cityDF("fileid").desc_nulls_last)
    val dfCity = cityDF.withColumn("city_row", row_number.over(groupCity))
    val dfCityfil = dfCity.filter("city_row = 1 and Localvalue is not null")
      .withColumn("Patientdetailtype", lit("CITY"))
    val dfCiti = dfCityfil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    val stateDF = df.withColumn("Localvalue", df("HUM_STATE"))
    val groupState = Window.partitionBy(stateDF("patient_id"), upper(stateDF("Localvalue"))).orderBy(stateDF("filedate").desc_nulls_last, stateDF("fileid").desc_nulls_last)
    val dfState = stateDF.withColumn("state_row", row_number.over(groupState))
    val dfStatefil = dfState.filter("state_row = 1 and Localvalue is not null")
      .withColumn("Patientdetailtype", lit("STATE"))
    val dfSt = dfStatefil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


     val satZip = standardizeZip("zip", true)
   // val satZip = standardizeZip("zip", false)
    val locdf = satZip("zip", df)
    var zipDF = locdf.withColumn("Localvalue",
      when(locdf("zip") === "00000", null)
        .otherwise(locdf("zip"))
    )

    zipDF = zipDF.withColumn("Patientdetailtype", lit("ZIPCODE"))
    val groupZip = Window.partitionBy(zipDF("patient_id"), zipDF("Localvalue")).orderBy(zipDF("filedate").desc_nulls_last, zipDF("fileid").desc_nulls_last)
    val dfZ = zipDF.withColumn("zip_row", row_number.over(groupZip))
    val dfzipfil = dfZ.filter("zip_row = 1 and Localvalue is not null")
    val dfZc = dfzipfil.select("PATIENT_ID", "update_date", "Patientdetailtype", "Localvalue")


    dfFName.union(dfLname).union(dfMidname)
      .union(dfGend).union(dfRc).union(dfEth)
      .union(dfLang).union(dfMart).union(dfCiti)
      .union(dfSt).union(dfZc)


  }

  map = Map(
    "DATASRC" -> literal("patient"),
    "PATIENTID" -> mapFrom("PATIENT_ID"),
    "LOCALVALUE" -> mapFrom("Localvalue"),
    "PATIENTDETAILTYPE" -> mapFrom("Patientdetailtype"),
    "PATDETAIL_TIMESTAMP" -> mapFrom("update_date")
  )
}
