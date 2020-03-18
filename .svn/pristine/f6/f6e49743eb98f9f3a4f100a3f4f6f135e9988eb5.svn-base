package com.humedica.mercury.etl.fdr.patient

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/19/2017
 */


class PatientPatients(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("Patient:"+config("EMR")+"@Patient", "Patientdetail:"+config("EMR")+"@Patientdetail",
    "Patientidentifier:"+config("EMR")+"@Patientidentifier", "Patientaddress:"+config("EMR")+"@Patientaddress",
    "Patientcontact:"+config("EMR")+"@Patientcontact", "cdr.map_ethnicity", "cdr.map_gender", "cdr.map_race",
    "cdr.map_deceased_indicator", "cdr.map_marital_status", "cdr.map_language")



  columns=List("CLIENT_ID", "CLIENT_DS_ID", "DATASRC", "PATIENT_ID", "PAT_TIMESTAMP", "DOB", "FIRST_NAME", "LAST_NAME", "MIDDLE_NAME",
  "SSN", "MRN", "GENDER", "DOD", "RACE", "ETHNICITY", "DEATH_IND", "LANGUAGE", "CURRENT_PCP", "MARITAL_STATUS", "RELIGION",
  "HM_ADDR_LINE_1", "HM_ADDR_LINE_2", "HM_ADDR_CITY", "HM_ADDR_STATE", "HM_ADDR_ZIP", "HOME_PHONE", "WORK_PHONE", "CELL_PHONE",
  "WORK_EMAIL", "PRSNL_EMAIL", "HGPID", "MPI")


  beforeJoin = Map(
    "Patientdetail" -> ((df: DataFrame) => {
      df.withColumn("ETH", when(df("PATIENTDETAILTYPE") === "ETHNICITY", df("LOCALVALUE")))
        .withColumn("GEN", when(df("PATIENTDETAILTYPE") === "GENDER", df("LOCALVALUE")))
        .withColumn("RAC", when(df("PATIENTDETAILTYPE") === "RACE", df("LOCALVALUE")))
        .withColumn("DEA", when(df("PATIENTDETAILTYPE") === "DECEASED", df("LOCALVALUE")))
        .withColumn("MAR", when(df("PATIENTDETAILTYPE") === "MARITAL", df("LOCALVALUE")))
        .withColumn("LAN", when(df("PATIENTDETAILTYPE") === "LANGUAGE", df("LOCALVALUE")))
        .drop("DATASRC").drop("HGPID")
    }),
    "Patientidentifier" -> ((df: DataFrame) => {
      df.drop("DATASRC").drop("HGPID")
    }),
    "Patientaddress" -> ((df: DataFrame) => {
      df.drop("DATASRC").drop("HGPID")
    }),
    "Patientcontact" -> ((df: DataFrame) => {
      df.drop("DATASRC").drop("HGPID")
    }),
    "cdr.map_ethnicity" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").withColumnRenamed("CUI", "ETH_CUI").drop("GROUPID")
    }),
    "cdr.map_gender" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").withColumnRenamed("CUI", "GEN_CUI").drop("GROUPID")
    }),
    "cdr.map_race" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").withColumnRenamed("CUI", "RAC_CUI").drop("GROUPID")
    }),
    "cdr.map_deceased_indicator" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").withColumnRenamed("CUI", "DEA_CUI").drop("GROUPID")
    }),
    "cdr.map_marital_status" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").withColumnRenamed("CUI", "MAR_CUI").drop("GROUPID")
    }),
  "cdr.map_language" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").withColumnRenamed("CUI", "LAN_CUI").drop("GROUPID")
  })
  )



  join = (dfs: Map[String, DataFrame]) => {
    dfs("Patient")
      .join(dfs("Patientdetail"),Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")
      .join(dfs("Patientidentifier"), Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")
      .join(dfs("Patientaddress"), Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")
      .join(dfs("Patientcontact"), Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")
      .join(dfs("cdr.map_ethnicity"), dfs("Patientdetail")("ETH") === dfs("cdr.map_ethnicity")("MNEMONIC"), "left_outer")
      .join(dfs("cdr.map_gender"), dfs("Patientdetail")("GEN") === dfs("cdr.map_gender")("MNEMONIC"), "left_outer")
      .join(dfs("cdr.map_race"), dfs("Patientdetail")("RAC") === dfs("cdr.map_race")("MNEMONIC"), "left_outer")
      .join(dfs("cdr.map_deceased_indicator"), coalesce(dfs("Patientdetail")("DEA"), lit("DEFAULT")) === dfs("cdr.map_deceased_indicator")("MNEMONIC"), "left_outer")
      .join(dfs("cdr.map_marital_status"), dfs("Patientdetail")("MAR") === dfs("cdr.map_marital_status")("LOCAL_CODE"), "left_outer")
      .join(dfs("cdr.map_language"), dfs("Patientdetail")("LAN") === dfs("cdr.map_language")("MNEMONIC"), "left_outer")


  }


  afterJoin = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID")).orderBy(df("ETH_CUI").desc, df("PATDETAIL_TIMESTAMP").desc)
    val groups1 = Window.partitionBy(df("PATIENTID")).orderBy(df("GEN_CUI").desc, df("PATDETAIL_TIMESTAMP").desc)
    val groups2 = Window.partitionBy(df("PATIENTID")).orderBy(df("RAC_CUI").desc, df("PATDETAIL_TIMESTAMP").desc)
    val groups3 = Window.partitionBy(df("PATIENTID")).orderBy(df("DEA_CUI").desc, df("PATDETAIL_TIMESTAMP").desc)
    val groups4 = Window.partitionBy(df("PATIENTID")).orderBy(df("MAR_CUI").desc, df("PATDETAIL_TIMESTAMP").desc)
    val groups5 = Window.partitionBy(df("PATIENTID")).orderBy(df("LAN_CUI").desc, df("PATDETAIL_TIMESTAMP").desc)
    df.withColumn("ETHNICITY", max("ETH").over(groups)).withColumn("ETHNIC_CUI", max("ETH_CUI").over(groups))
      .withColumn("GENDER", max("GEN").over(groups1)).withColumn("GENDER_CUI", max("GEN_CUI").over(groups1))
      .withColumn("RACE", max("RAC").over(groups2)).withColumn("RACE_CUI", max("RAC_CUI").over(groups2))
      .withColumn("DEATH", max("DEA").over(groups3)).withColumn("DEATH_CUI", max("DEA_CUI").over(groups3))
      .withColumn("MARITAL", max("MAR").over(groups4)).withColumn("MARITAL_CUI", max("MAR_CUI").over(groups4))
      .withColumn("LANGUAGE", max("LAN").over(groups5)).withColumn("LANGUAGE_CUI", max("LAN_CUI").over(groups5))


  }






  map = Map(
    "CLIENT_ID" -> mapFrom("GROUPID"),
    "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
    "DATASRC" -> mapFrom("DATASRC"),
    "PATIENT_ID" -> mapFrom("PATIENTID"),
    "PAT_TIMESTAMP" -> ((col: String, df:DataFrame) => df.withColumn(col, substring(df("PATDETAIL_TIMESTAMP"),1,19))),
    "DOB" -> ((col: String, df:DataFrame) => df.withColumn(col, substring(regexp_replace(df("DATEOFBIRTH"),"-",""),1,8))),
    "FIRST_NAME" -> ((col: String, df: DataFrame) => df.withColumn(col, when(df("PATIENTDETAILTYPE") === "FIRST_NAME", df("LOCALVALUE")))),
    "LAST_NAME" -> ((col: String, df: DataFrame) => df.withColumn(col, when(df("PATIENTDETAILTYPE") === "LAST_NAME", df("LOCALVALUE")))),
    "MIDDLE_NAME" -> ((col: String, df: DataFrame) => df.withColumn(col, when(df("PATIENTDETAILTYPE") === "MIDDLE_NAME", df("LOCALVALUE")))),
    "SSN" -> ((col: String, df: DataFrame) => df.withColumn(col, when(df("IDTYPE") === "SSN", df("IDVALUE")))),
    "MRN" -> ((col: String, df: DataFrame) => df.withColumn(col, when(df("IDTYPE") === "MRN", df("IDVALUE")))),
    "GENDER" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("GENDER").isNull && df("GENDER_CUI").isNull, "CH999999")
          .when(df("GENDER").isNotNull && df("GENDER_CUI").isNull, "CH999990")
          .otherwise(df("GENDER_CUI"))
      )
    }),
    "DOD" -> ((col: String, df:DataFrame) => df.withColumn(col, substring(regexp_replace(df("DATEOFDEATH"), "-", ""),1,8))),
    "RACE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("RACE").isNull && df("RACE_CUI").isNull, "CH999999")
          .when(df("RACE").isNotNull && df("RACE_CUI").isNull, "CH999990")
          .otherwise(df("RACE_CUI"))
      )
    }),
    "ETHNICITY" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("ETHNICITY").isNull && df("ETHNIC_CUI").isNull, "CH999999")
          .when(df("ETHNICITY").isNotNull && df("ETHNIC_CUI").isNull, "CH999990")
          .otherwise(df("ETHNIC_CUI"))
      )
      }),
    "DEATH_IND" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("DEATH").isNull && df("DEATH_CUI").isNull, "CH999999")
          .when(df("DEATH").isNotNull && df("DEATH_CUI").isNull, "CH999990")
          .otherwise(df("DEATH_CUI"))
      )
    }),
    "LANGUAGE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("LANGUAGE").isNull && df("LANGUAGE_CUI").isNull, "CH999999")
          .when(df("LANGUAGE").isNotNull && df("LANGUAGE_CUI").isNull, "CH999990")
          .otherwise(df("LANGUAGE_CUI"))
      )
    }),
    "MARITAL" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("MARITAL").isNull && df("MARITAL_CUI").isNull, "CH999999")
          .when(df("MARITAL").isNotNull && df("MARITAL_CUI").isNull, "CH999990")
          .otherwise(df("MARITAL_CUI"))
      )
    }),
    "HM_ADDR_LINE_1" -> mapFrom("ADDRESS_LINE1"),
    "HM_ADDR_LINE_2" -> mapFrom("ADDRESS_LINE2"),
    "HM_ADDR_CITY" -> mapFrom("CITY"),
    "HM_ADDR_STATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(length(df("STATE")) leq 2, df("STATE")).otherwise(null))
    }),
    "HM_ADDR_ZIP" -> mapFrom("ZIPCODE"),
    "HOME_PHONE" -> mapFrom("HOME_PHONE"),
    "WORK_PHONE" -> mapFrom("WORK_PHONE"),
    "CELL_PHONE" -> mapFrom("CELL_PHONE"),
    "WORK_EMAIL" -> mapFrom("WORK_EMAIL"),
    "PRSNL_EMAIL" -> mapFrom("PERSONAL_EMAIL")
  )



  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID")).orderBy(df("PATDETAIL_TIMESTAMP"))
    val addColumn = df.withColumn("DOB", first("DOB").over(groups))
      .withColumn("DOD", first("DOD").over(groups))
      .withColumn("FIRST_NAME", max("FIRST_NAME").over(groups))
      .withColumn("LAST_NAME", max("LAST_NAME").over(groups))
      .withColumn("MIDDLE_NAME", max("MIDDLE_NAME").over(groups))
      .withColumn("MRN", first("MRN").over(groups))
      .withColumn("SSN", first("SSN").over(groups))
      .withColumn("HM_ADDR_LINE_1", first("HM_ADDR_LINE_1").over(groups))
      .withColumn("HM_ADDR_LINE_2", first("HM_ADDR_LINE_2").over(groups))
      .withColumn("HM_ADDR_CITY", first("HM_ADDR_CITY").over(groups))
      .withColumn("HM_ADDR_STATE", first("HM_ADDR_STATE").over(groups))
      .withColumn("HM_ADDR_ZIP", first("HM_ADDR_ZIP").over(groups))
      .withColumn("HOME_PHONE", first("HOME_PHONE").over(groups))
      .withColumn("WORK_PHONE", first("WORK_PHONE").over(groups))
      .withColumn("CELL_PHONE", first("CELL_PHONE").over(groups))
      .withColumn("WORK_EMAIL", first("WORK_EMAIL").over(groups))
      .withColumn("PRSNL_EMAIL", first("PRSNL_EMAIL").over(groups))
      .withColumn("rn", row_number().over(groups))
      addColumn.filter("rn = 1 and DOB is not null and ")


  }



  //val es = new PatientPatients(cfg)
  //val spat = build(es, allColumns=true)


 }

