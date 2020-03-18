package com.humedica.mercury.etl.cerner_v2.patientdetail

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class PatientdetailPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  tables = List(
    "person",
    "zh_code_value", "cdr.map_predicate_values")

  columnSelect = Map(
    "person" -> List("NATIONALITY_CD", "PERSON_ID", "BIRTH_DT_TM", "DECEASED_DT_TM", "UPDT_DT_TM",
      "DECEASED_CD", "ETHNIC_GRP_CD", "NAME_FIRST", "SEX_CD", "religion_cd",
      "LANGUAGE_CD", "NAME_LAST", "MARITAL_TYPE_CD", "NAME_MIDDLE", "RACE_CD"),
    "zh_code_value" -> List("CODE_VALUE", "DISPLAY")
  )


  beforeJoin = Map(
    "person" -> ((df: DataFrame) => {
      df.filter(
        "not (LOWER(name_last) LIKE 'zz%' OR (LOWER(name_full_formatted) LIKE '%patient%' " +
          " AND LOWER(name_full_formatted) LIKE '%test%'))" +
          "  and person_id is not null")

      .withColumnRenamed("UPDT_DT_TM", "update_date")
   }))



  join = (dfs: Map[String, DataFrame]) => {
    dfs("person")
      .join(dfs("zh_code_value"), dfs("zh_code_value")("CODE_VALUE") === dfs("person")("religion_cd"), "left_outer")


  }


  afterJoin = (df: DataFrame) => {
    var dfFN = df.withColumnRenamed("name_first", "localvalue")
      .withColumn("patientdetailtype", lit("FIRST_NAME"))
    val groupsFN = Window.partitionBy(dfFN("PERSON_ID"), upper(dfFN("localvalue"))).orderBy(dfFN("update_date").desc_nulls_last)
    val df2 = dfFN.withColumn("first_row", row_number.over(groupsFN))
    dfFN = df2.filter(" first_row = 1 and localvalue is not null ")
    dfFN = dfFN.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    var dfLN = df.withColumnRenamed("name_last", "localvalue")
      .withColumn("patientdetailtype", lit("LAST_NAME"))
    val groupsLN = Window.partitionBy(dfLN("PERSON_ID"), upper(dfLN("localvalue"))).orderBy(dfLN("update_date").desc_nulls_last)
    dfLN = dfLN.withColumn("last_row", row_number.over(groupsLN))
    dfLN = dfLN.filter(" last_row = 1 and localvalue is not null ")
    dfLN = dfLN.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    val dfg = df.withColumn("localvalue", when(df("sex_cd") === "0", null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("sex_cd"))))
    var dfGender = dfg.withColumn("patientdetailtype", lit("GENDER"))
    val groupsGender = Window.partitionBy(dfGender("PERSON_ID"), upper(dfGender("localvalue"))).orderBy(dfGender("update_date").desc_nulls_last)
    dfGender = dfGender.withColumn("gender_row", row_number.over(groupsGender))
    dfGender = dfGender.filter(" gender_row = 1 and localvalue is not null ")
    dfGender = dfGender.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    val dfr = df.withColumn("localvalue", when(df("race_cd") === "0", null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("race_cd"))))
    var dfRace = dfr.withColumn("patientdetailtype", lit("RACE"))
    val groupsRace = Window.partitionBy(dfRace("PERSON_ID"), upper(dfRace("localvalue"))).orderBy(dfRace("update_date").desc_nulls_last)
    dfRace = dfRace.withColumn("race_row", row_number.over(groupsRace))
    dfRace = dfRace.filter(" race_row = 1 and localvalue is not null  ")
    dfRace = dfRace.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    val racedf = df.withColumn("race", when(df("race_cd") === "0", null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("race_cd"))))
    val dfethval = racedf.withColumn("ethnicity_value", when(racedf("ETHNIC_GRP_CD") === "0", null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("ethnic_grp_cd"))))
    var dfEth = dfethval.withColumn("localvalue", when(dfethval("ethnicity_value").isNull, dfethval("race")).otherwise(dfethval("ethnicity_value")))
    dfEth = dfEth.withColumn("patientdetailtype", lit("ETHNICITY"))
    val groupsEthnicity = Window.partitionBy(dfEth("PERSON_ID"), dfEth("localvalue")).orderBy(dfEth("update_date").desc_nulls_last)
    dfEth = dfEth.withColumn("ethnicity_row", row_number.over(groupsEthnicity))
    dfEth = dfEth.filter(" ethnicity_row = 1 and localvalue is not null ")
    dfEth = dfEth.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    val dfdeathind = df.withColumn("localvalue", when(df("deceased_cd") === "0", null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("deceased_cd"))))
    var dfDEC = dfdeathind.withColumn("patientdetailtype", lit("DECEASED"))
    val groupsDEC = Window.partitionBy(dfDEC("PERSON_ID"), dfDEC("localvalue")).orderBy(dfDEC("update_date").desc_nulls_last)
    dfDEC = dfDEC.withColumn("status_row", row_number.over(groupsDEC))
    dfDEC = dfDEC.filter(" status_row = 1  and localvalue is not null ")
    dfDEC = dfDEC.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")



    val dfEGrp = df.withColumn("localvalue", when(df("nationality_cd") === "0", null).otherwise(df("nationality_cd")))
    var dfEth_Grp = dfEGrp.withColumn("patientdetailtype", lit("ETHNIC_GROUP"))
    val groupsEth_Grp = Window.partitionBy(dfEth_Grp("PERSON_ID"), upper(dfEth_Grp("localvalue"))).orderBy(dfEth_Grp("update_date").desc_nulls_last)
    dfEth_Grp = dfEth_Grp.withColumn("ethnic_grp_row", row_number.over(groupsEth_Grp))
    dfEth_Grp = dfEth_Grp.filter(" ethnic_grp_row = 1  and localvalue is not null ")
    dfEth_Grp = dfEth_Grp.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    val dfL = df.withColumn("localvalue", when(df("language_cd") === "0", null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("language_cd"))))
    var dfLang = dfL.withColumn("patientdetailtype", lit("LANGUAGE"))
    val groupsLang = Window.partitionBy(dfLang("PERSON_ID"), upper(dfLang("localvalue"))).orderBy(dfLang("update_date").desc_nulls_last)
    dfLang = dfLang.withColumn("language_row", row_number.over(groupsLang))
    dfLang = dfLang.filter(" language_row = 1  and localvalue is not null ")
    dfLang = dfLang.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    val dfM = df.withColumn("localvalue", when(df("marital_type_cd") === "0", null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("marital_type_cd"))))
    var dfMARITAL = dfM.withColumn("patientdetailtype", lit("MARITAL"))
    val groupsMARITAL = Window.partitionBy(dfMARITAL("PERSON_ID"), upper(dfMARITAL("localvalue"))).orderBy(dfMARITAL("update_date").desc_nulls_last)
    dfMARITAL = dfMARITAL.withColumn("marital_row", row_number.over(groupsMARITAL))
    dfMARITAL = dfMARITAL.filter(" marital_row = 1 and localvalue is not null  ")
    dfMARITAL = dfMARITAL.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    val dfMid = df.withColumnRenamed("name_middle", "localvalue")
    var dfMID_DNAME = dfMid.withColumn("patientdetailtype", lit("MIDDLE_NAME"))
    val groupsMIDNAME = Window.partitionBy(dfMID_DNAME("PERSON_ID"), upper(dfMID_DNAME("localvalue"))).orderBy(dfMID_DNAME("update_date").desc_nulls_last)
    dfMID_DNAME = dfMID_DNAME.withColumn("middle_row", row_number.over(groupsMIDNAME))
    dfMID_DNAME = dfMID_DNAME.filter(" middle_row = 1 and localvalue is not null   ")
    dfMID_DNAME = dfMID_DNAME.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")


    val dfReg = df.withColumnRenamed("display", "localvalue")
    var dfReligionCD = dfReg.withColumn("patientdetailtype", lit("RELIGION"))
    val groupsRelCd = Window.partitionBy(dfReligionCD("PERSON_ID"), upper(dfReligionCD("localvalue"))).orderBy(dfReligionCD("update_date").desc_nulls_last)
    dfReligionCD = dfReligionCD.withColumn("religion_row", row_number.over(groupsRelCd))
    dfReligionCD = dfReligionCD.filter(" religion_row = 1 and localvalue is not null  ")
    dfReligionCD = dfReligionCD.select("PERSON_ID", "update_date", "patientdetailtype", "localvalue")

    dfFN.union(dfLN).union(dfGender).union(dfRace).union(dfEth)
      .union(dfDEC).union(dfEth_Grp).union(dfLang).union(dfMARITAL)
      .union(dfMID_DNAME).union(dfReligionCD)


  }

  map = Map(
    "DATASRC" -> literal("patient"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "patdetail_timestamp" -> mapFrom("update_date"),
    "patientdetailtype" -> mapFrom("patientdetailtype"),
    "localvalue" -> mapFrom("localvalue")
  )

}

