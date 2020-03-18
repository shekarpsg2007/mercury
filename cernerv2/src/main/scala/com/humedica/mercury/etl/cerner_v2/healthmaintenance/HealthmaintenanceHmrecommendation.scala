package com.humedica.mercury.etl.cerner_v2.healthmaintenance

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Created by abendiganavale on 9/17/18.
  */
class HealthmaintenanceHmrecommendation(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("hm_recommendation"
                ,"patient:cerner_v2.patient.PatientPatient"
                ,"cdr.map_screening_tests"
                ,"cdr.map_predicate_values"
  )

  columnSelect = Map(
    "hm_recommendation" -> List("PERSON_ID", "EXPECT_ID", "LAST_SATISFACTION_DT_TM", "UPDT_DT_TM", "DUE_DT_TM", "STATUS_FLAG"),
    "patient" -> List("PATIENTID","DATEOFBIRTH","CLIENT_DS_ID"),
    "cdr.map_screening_tests" -> List("GROUPID", "LOCAL_CODE", "HTS_CODE")
  )


  beforeJoin = Map(
    "hm_recommendation" -> ((df: DataFrame) => {
      val list_cc_screen_21_29 = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "HM_RECOMMENDATION", "HEALTH_MAINTENANCE", "HM_RECOMMENDATION", "EXPECT_ID_CC_SCREEN_21_29")
      val list_cc_screen_30_65 = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "HM_RECOMMENDATION", "HEALTH_MAINTENANCE", "HM_RECOMMENDATION", "EXPECT_ID_CC_SCREEN_30_65")
      val list_diab_screen_incl = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "HM_RECOMMENDATION", "HEALTH_MAINTENANCE", "HM_RECOMMENDATION", "EXPECT_ID_DIAB_SCREEN_INC")
      val list_diab_screen_excl = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "HM_RECOMMENDATION", "HEALTH_MAINTENANCE", "HM_RECOMMENDATION", "EXPECT_ID_DIAB_SCREEN_EXC")

      val groups = Window.partitionBy(df("PERSON_ID"))
      df.withColumn("expect_id_cc_screen", when(df("EXPECT_ID").isin(list_cc_screen_21_29,list_cc_screen_30_65), df("EXPECT_ID")))
                         .withColumn("expect_id_diab_screen", when(df("EXPECT_ID").isin(list_diab_screen_incl,list_diab_screen_excl), df("EXPECT_ID")))
                         .withColumn("cnt_dupe_cc_screenings", count("expect_id_cc_screen").over(groups))
                         .withColumn("cnt_dupe_diab_screenings", count("expect_id_diab_screen").over(groups))
    }),
    "patient" -> ((df: DataFrame) => {
      val fil = df.filter("CLIENT_DS_ID = " + config(CLIENT_DS_ID) + "")
                        .groupBy(df("PATIENTID")).agg(max(df("DATEOFBIRTH")).as("DOB"))
      fil.withColumn("age", regexp_replace(months_between(current_date(),fil("DOB"))/12 ,"[.][0-9]+$",""))
    }),
    "cdr.map_screening_tests" -> includeIf("GROUPID = '" + config (GROUP) + "'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("hm_recommendation")
      .join(dfs("patient"), dfs("hm_recommendation")("PERSON_ID") === dfs("patient")("PATIENTID"), "inner")
      .join(dfs("cdr.map_screening_tests"), dfs("hm_recommendation")("EXPECT_ID") === dfs("cdr.map_screening_tests")("LOCAL_CODE"), "inner")
  }

  afterJoin = (df: DataFrame) => {
    val list_cc_screen_21_29 = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "HM_RECOMMENDATION", "HEALTH_MAINTENANCE", "HM_RECOMMENDATION", "EXPECT_ID_CC_SCREEN_21_29")
    val list_cc_screen_30_65 = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "HM_RECOMMENDATION", "HEALTH_MAINTENANCE", "HM_RECOMMENDATION", "EXPECT_ID_CC_SCREEN_30_65")
    val list_diab_screen_excl = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "HM_RECOMMENDATION", "HEALTH_MAINTENANCE", "HM_RECOMMENDATION", "EXPECT_ID_DIAB_SCREEN_EXC")

    val fil1 = df.filter("(cnt_dupe_cc_screenings < 2 " +
      "or (cnt_dupe_cc_screenings = 2 and expect_id not in (" + list_cc_screen_21_29 + "," + list_cc_screen_30_65 + ") )" +
      "or (cnt_dupe_cc_screenings = 2 and expect_id in (" + list_cc_screen_21_29 + ") and age between '21' and '29')" +
      "or (cnt_dupe_cc_screenings = 2 and expect_id in (" + list_cc_screen_30_65 + ") and age between '30' and '65'))" +
      "and (cnt_dupe_diab_screenings < 2 or " +
      "(cnt_dupe_diab_screenings = 2 and expect_id not in (" + list_diab_screen_excl + ")))")

    val groups1 = Window.partitionBy(fil1("PERSON_ID"),fil1("EXPECT_ID"))
    val addColumn1 = fil1.withColumn("max_satisfaction_dt_tm", max("LAST_SATISFACTION_DT_TM").over(groups1))
                       .withColumn("max_due_dt_tm", max("DUE_DT_TM").over(groups1))
                       .withColumn("max_documented_date", max("UPDT_DT_TM").over(groups1))

    val groups2 = Window.partitionBy(addColumn1("PERSON_ID"),addColumn1("EXPECT_ID")).orderBy(addColumn1("LAST_SATISFACTION_DT_TM").desc_nulls_last, addColumn1("UPDT_DT_TM").desc_nulls_last)
    val addColumn2 = addColumn1.withColumn("rn", row_number.over(groups2))
    val fil2 = addColumn2.filter("rn = 1").drop("rn")

    fil2.withColumn("last_satisfied_dt", when(fil2("cnt_dupe_cc_screenings") < lit(2), fil2("max_satisfaction_dt_tm"))
                                        .when(!fil2("EXPECT_ID").isin(list_cc_screen_21_29,list_cc_screen_30_65), fil2("max_satisfaction_dt_tm"))
                                        .when(fil2("EXPECT_ID").isin(list_cc_screen_21_29,list_cc_screen_30_65),
                                            when(fil2("age").between(lit("21"),lit("65")), fil2("max_satisfaction_dt_tm"))
                                           .when(fil2("age") >= lit("66"), lit("9999-12-31"))
                                           .when(fil2("age") <= lit("20"), null)
                                        )
                   )
       .withColumn("due_dt", when(fil2("UPDT_DT_TM") === fil2("max_documented_date") && fil2("STATUS_FLAG") === lit("7"), lit("9999-12-31"))
                            .when(!fil2("EXPECT_ID").isin(list_cc_screen_21_29,list_cc_screen_30_65), fil2("max_due_dt_tm"))
                            .when(fil2("EXPECT_ID").isin(list_cc_screen_21_29,list_cc_screen_30_65),
                              when(fil2("age").between(lit("21"),lit("65")), fil2("max_satisfaction_dt_tm"))
                                .when(fil2("age") >= lit("66"), lit("9999-12-31"))
                                .when(fil2("age") <= lit("20"), null)
                            )
                  )
  }


  map = Map(
    "DATASRC" -> literal("hm_recommendation"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "LOCALCODE" -> mapFrom("EXPECT_ID"),
    "HM_CUI" -> mapFrom("HTS_CODE"),
    "DOCUMENTED_DATE" -> mapFrom("max_documented_date"),
    "LAST_SATISFIED_DATE" -> mapFrom("last_satisfied_dt"),
    "NEXT_DUE_DATE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("due_dt").isNotNull, df("due_dt"))
                        .otherwise(date_sub(current_date(),2))
      )
    })
  )

}
