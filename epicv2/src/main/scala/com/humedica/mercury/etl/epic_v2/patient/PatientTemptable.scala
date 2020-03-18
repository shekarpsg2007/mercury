package com.humedica.mercury.etl.epic_v2.patient

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
  * Created by cdivakaran on 5/26/17.
  */
class PatientTemptable (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {


  cacheMe = true

  tables = List("patreg",
      "pat_race",
      "identity_id", "patient_3", "patient_fyi_flags", "cdr.map_predicate_values", "zh_state")

  columns = List("PATIENTID", "MEDICALRECORDNUMBER", "IDENTITY_TYPE_ID", "IDENTITY_SUBTYPE_ID", "MRNID", "DATEOFBIRTH",
    "DATEOFDEATH", "PAT_STATUS", "MARITAL_STATUS", "MIDDLE_NAME", "FIRSTNAME", "LASTNAME", "GENDER", "RACE",
    "RA_UPDATE_DATE", "UPDATE_DATE", "ID_UPDATE_DATE", "CITY", "STATE", "ZIP", "LANGUAGE", "SSN", "IDENTITY_TYPE_ID_RNK",
    "INC_PAT3", "INC_PFF")


  columnSelect = Map(
    "patreg" -> List("PAT_MRN_ID", "BIRTH_DATE", "DEATH_DATE", "PAT_STATUS_C", "MARITAL_STATUS_C", "PAT_MIDDLE_NAME", "PAT_NAME",
      "ETHNIC_GROUP_C", "PAT_FIRST_NAME", "PAT_LAST_NAME", "SEX_C", "CITY", "STATE_C", "ZIP", "SSN", "LANGUAGE_C", "UPDATE_DATE",
      "PAT_ID"),
    "pat_race" -> List("PATIENT_RACE_C","PAT_ID", "LINE",  "UPDATE_DATE"),
    "identity_id" -> List("IDENTITY_ID", "IDENTITY_TYPE_ID", "PAT_ID", "UPDATE_DATE"),
    "patient_3" -> List("IS_TEST_PAT_YN", "PAT_ID", "UPDATE_DATE"),
    "patient_fyi_flags" -> List("PAT_FLAG_TYPE_C", "PATIENT_ID"),
    "zh_state" -> List("STATE_C", "NAME")
  )

  beforeJoin = Map(
    "patreg" -> ((df: DataFrame) => {
      val fil = df.filter("lower(PAT_NAME) not like 'zz%'")
      fil.groupBy("PAT_ID", "PAT_MRN_ID", "BIRTH_DATE", "DEATH_DATE", "PAT_STATUS_C", "MARITAL_STATUS_C", "PAT_MIDDLE_NAME",
      "ETHNIC_GROUP_C", "PAT_FIRST_NAME", "PAT_LAST_NAME", "SEX_C", "CITY", "STATE_C", "ZIP", "SSN", "LANGUAGE_C")
        .agg(max("UPDATE_DATE").as("UPDATE_DATE")).repartition(1000)
    }),
    "identity_id" -> ((df: DataFrame) => {

      val mpv1 = mpv(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "IDENTITY_ID", "PATIENT_ID", "IDENTITY_ID", "IDENTITY_TYPE_ID")
      val mpv2 = mpv(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "IDENTITY_ID", "PATIENT", "IDENTITY_ID", "IDENTITY_TYPE_ID")
      val mpvout = mpv1.union(mpv2).select("COLUMN_VALUE")
      val id_subtype =  mpvout.withColumn("IDENTITY_TYPE_ID", when(instr(mpvout("COLUMN_VALUE"), ".") === 0, mpvout("COLUMN_VALUE"))
          .otherwise(split(mpvout("COLUMN_VALUE"), "\\.").getItem(1))
      )
        .withColumn("IDENTITY_SUBTYPE_ID", when(instr(mpvout("COLUMN_VALUE"), ".") === 0, null)
          .otherwise(split(mpvout("COLUMN_VALUE"), "\\.").getItem(0)))
      val joined = df.join(id_subtype, Seq("IDENTITY_TYPE_ID"), "inner")
      val groups = Window.partitionBy(joined("PAT_ID"), joined("IDENTITY_TYPE_ID"))
        .orderBy(joined("UPDATE_DATE").desc_nulls_last, joined("IDENTITY_SUBTYPE_ID").asc_nulls_last)
      joined.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .select("IDENTITY_TYPE_ID", "IDENTITY_ID", "PAT_ID", "IDENTITY_SUBTYPE_ID", "UPDATE_DATE")
        .withColumnRenamed("UPDATE_DATE","UPDATE_DATE_id")
        .repartition(1000)

    }),
    "pat_race" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("PAT_ID"), df1("LINE")).orderBy(df1("UPDATE_DATE").desc_nulls_last)
      val addColumn = df1.withColumn("ra_rn", row_number.over(groups))
      addColumn.filter("ra_rn =1").select("PATIENT_RACE_C", "PAT_ID", "UPDATE_DATE")
        .withColumnRenamed("UPDATE_DATE", "UPDATE_DATE_ra")
    }),
    "patient_3" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("PAT_ID")).orderBy(df1("UPDATE_DATE").desc_nulls_last)
      val addColumn = df1.withColumn("p3_rnbr", row_number.over(groups))
      addColumn.filter("p3_rnbr =1")
        .select("IS_TEST_PAT_YN", "PAT_ID", "UPDATE_DATE")
        .withColumnRenamed("UPDATE_DATE", "UPDATE_DATE_pat3")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("patreg")
      .join(dfs("identity_id"), Seq("PAT_ID"), "left_outer")
      .join(dfs("pat_race"), Seq("PAT_ID"), "left_outer")
      .join(dfs("patient_3"), Seq("PAT_ID"), "left_outer")
      .join(dfs("patient_fyi_flags"),dfs("patreg")("PAT_ID")=== dfs("patient_fyi_flags")("PATIENT_ID"),"left_outer")
      .join(dfs("zh_state"), Seq("STATE_C"), "left_outer")
  }

  map = Map(
    "PATIENTID" -> mapFrom("PAT_ID"),
    "MEDICALRECORDNUMBER" -> mapFrom("IDENTITY_ID"),
    "IDENTITY_TYPE_ID" -> mapFrom("IDENTITY_TYPE_ID"),
    "IDENTITY_SUBTYPE_ID" -> mapFrom("IDENTITY_SUBTYPE_ID"),
    "MRNID" -> mapFrom("IDENTITY_ID"),
    "DATEOFBIRTH" -> mapFrom("BIRTH_DATE"),
    "DATEOFDEATH" -> mapFrom("DEATH_DATE"),
    "PAT_STATUS" -> mapFrom("PAT_STATUS_C"),
    "MARITAL_STATUS" -> mapFrom("MARITAL_STATUS_C"),
    "MIDDLE_NAME" -> mapFrom("PAT_MIDDLE_NAME"),
    "FIRSTNAME" -> mapFrom("PAT_FIRST_NAME"),
    "LASTNAME" -> mapFrom("PAT_LAST_NAME"),
    "GENDER" -> mapFrom("SEX_C"),
    "RACE" -> mapFrom("PATIENT_RACE_C"),
    "RA_UPDATE_DATE" -> mapFrom("UPDATE_DATE_ra"),
    "UPDATE_DATE" -> mapFrom("UPDATE_DATE"),
    "ID_UPDATE_DATE" -> mapFrom("UPDATE_DATE_id"),
    "CITY" -> mapFrom("CITY"),
    "STATE" -> mapFrom("NAME"),
    "LANGUAGE" -> mapFrom("LANGUAGE_C"),
    "SSN" -> standardizeSSN("SSN"),
    "IDENTITY_TYPE_ID_RNK" -> ((col:String, df:DataFrame) => {
      val list_identity_type_pid = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATREG", "PATIENT", "IDENTITY_ID", "IDENTITY_TYPE_ID")
      df.withColumn(col, when(df("IDENTITY_TYPE_ID").isin(list_identity_type_pid: _*), lit("1")).otherwise(lit("0")))
    }),
    "INC_PAT3" -> ((col:String, df:DataFrame)=> {
      df.withColumn(col, when((df("IS_TEST_PAT_YN") =!= "Y") || isnull(df("IS_TEST_PAT_YN")), 1).otherwise(0))
    }),
    "INC_PFF" -> ((col:String, df:DataFrame)=> {
      df.withColumn(col, when((df("PAT_FLAG_TYPE_C") =!= "9") || isnull(df("PAT_FLAG_TYPE_C")), 1).otherwise(0))
    }),
    "ZIP" -> standardizeZip("ZIP", true)
  )


  afterMap = (df: DataFrame) => {
    df.withColumn("SSN", when(df("SSN") === "123-45-6789", null).otherwise(df("SSN")))
      .filter("INC_PAT3 = 1 and INC_PFF = 1")
  }

  mapExceptions = Map(
    ("H406239_EP2", "MEDICALRECORDNUMBER") -> mapFrom("PAT_MRN_ID"),
    ("H984787_EP2", "MEDICALRECORDNUMBER") -> mapFrom("PAT_MRN_ID"),
    ("H135535_EP2", "MEDICALRECORDNUMBER") -> mapFrom("PAT_MRN_ID"),
    ("H553173_EP2", "DATEOFDEATH") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(date_format(df("DEATH_DATE"), "yyyyMMdd").isin("19800101", "20010101"), null).otherwise(df("DEATH_DATE")))
    })
  )

}
