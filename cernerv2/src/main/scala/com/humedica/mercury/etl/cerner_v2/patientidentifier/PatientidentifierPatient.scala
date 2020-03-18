package com.humedica.mercury.etl.cerner_v2.patientidentifier

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 08/09/2018
  */


class PatientidentifierPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {
  tables = List("person_alias",
    "person",
    "cdr.map_predicate_values")

  columnSelect = Map(
    "person_alias" -> List("person_alias_type_cd", "alias_pool_cd", "ALIAS", "person_id", "UPDT_DT_TM", "active_ind", "end_effective_dt_tm","beg_effective_dt_tm"),
    "person" -> List("NATIONALITY_CD", "PERSON_ID", "BIRTH_DT_TM", "DECEASED_DT_TM", "UPDT_DT_TM",
      "DECEASED_CD", "ETHNIC_GRP_CD", "NAME_FIRST", "SEX_CD", "religion_cd",
      "LANGUAGE_CD", "NAME_LAST", "MARITAL_TYPE_CD", "NAME_MIDDLE", "RACE_CD")

  )


  beforeJoin = Map(
    "person" -> ((df: DataFrame) => {
      val df1 = df.filter(
        "not (LOWER(name_last) LIKE 'zz%' OR (LOWER(name_full_formatted) LIKE '%patient%' " +
          " AND LOWER(name_full_formatted) LIKE '%test%'))" +
          "  and person_id is not null")
      df1.withColumnRenamed("UPDT_DT_TM", "update_date")

    }),
    "person_alias" -> ((df: DataFrame) => {

      val list_cmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val list_ssn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SSN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val list_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val list_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "ALIAS_POOL_CD")
      val list_rmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val list_rmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")


      val fil = df.filter(" end_effective_dt_tm > current_date() ")
      val list1 = list_cmrn_person_alias_type_cd ::: list_ssn_person_alias_type_cd ::: list_person_alias_type_cd


      val fil1 = fil.filter(fil("person_alias_type_cd").isin(list1: _*) || fil("alias_pool_cd").isin(list_alias_pool_cd: _*))
      val fil2 = fil.filter(fil("person_alias_type_cd").isin(list_rmrn_person_alias_type_cd: _*) && fil("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*))
      val fil3 = fil1.union(fil2)


      val groups = Window.partitionBy(fil3("person_id"), fil3("person_alias_type_cd"), fil3("alias")).orderBy(fil3("UPDT_DT_TM").desc, fil3("active_ind").desc)
      val fil4 = fil3.withColumn("alias_row", row_number.over(groups))

      val groups1 = Window.partitionBy(fil4("person_id"), fil4("person_alias_type_cd")).orderBy(fil4("UPDT_DT_TM").desc, fil4("active_ind").desc).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      val addColumn1 = fil4.withColumn("active_patient_flag", first("active_ind").over(groups1))
      addColumn1.filter("alias_row = 1").withColumnRenamed("UPDT_DT_TM", "pa_update_date")


    }
      ))

  join = (dfs: Map[String, DataFrame]) => {
    dfs("person")
      .join(dfs("person_alias"), Seq("PERSON_ID"), "left_outer")

  }


  afterJoin = (df: DataFrame) => {


    val list_rmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")
    val list_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "ALIAS_POOL_CD")
    val list_cmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
    val list_cmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")
    val list_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
    val df_active = df.filter(" active_ind = '1' and active_patient_flag = '1' ")
      .withColumn("ID_SUBTYPE",
        when(df("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*), lit("MRN"))
          .when(df("alias_pool_cd").isin(list_cmrn_alias_pool_cd: _*), lit("CMRN"))
          .otherwise(null))



    val list_ssn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SSN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
    val df1 = df_active.withColumn("SSN", when(df_active("person_alias_type_cd").isin(list_ssn_person_alias_type_cd: _*), df_active("alias")))
    val groups1 = Window.partitionBy(df1("PERSON_ID"), df1("ssn")).orderBy(df1("pa_update_date").desc)
    val df2 = df1.withColumn("ssn_row", row_number.over(groups1))
    val df3 = df2.filter("ssn_row = 1 ")
    val satSSN = standardizeSSN("SSN")
    val dfSSN = satSSN("SSN", df3).filter("SSN is not null")
      .withColumn("IDTYPE", lit("SSN")).withColumnRenamed("SSN", "IDVALUE")
      .select("PERSON_ID","idtype","idvalue", "id_subtype")


    val df5 = df_active.withColumn("idvalue", when(df_active("person_alias_type_cd").isin(list_person_alias_type_cd: _*), df_active("alias")))
    val df6 = df_active.withColumn("idvalue", when(df_active("alias_pool_cd").isin(list_alias_pool_cd: _*), df_active("alias")))
    val df7 = df5.union(df6)
    val df8 = df7.withColumn("idtype", lit("MRN"))
    val groups2 = Window.partitionBy(df8("PERSON_ID"), df8("idvalue")).orderBy(df8("pa_update_date").desc)
    val df9 = df8.withColumn("mrn_row", row_number.over(groups2))
    val dfMRN = df9.filter("mrn_row = 1 and idvalue is not null ")
      .select("PERSON_ID","idtype","idvalue", "id_subtype")



    val df10 = df_active.withColumn("idtype", lit("MRN"))
    val list_rmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
    val df11 = df10.withColumn("idvalue", when(df10("person_alias_type_cd").isin(list_rmrn_person_alias_type_cd: _*) && df10("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*), df10("alias")))
    val groups3 = Window.partitionBy(df11("PERSON_ID"), df11("idvalue")).orderBy(df11("pa_update_date").desc)
    val df12 = df11.withColumn("rmrn_row", row_number.over(groups3))
    val dfRMRN = df12.filter("rmrn_row = 1 and idvalue is not null ")
      .select("PERSON_ID","idtype","idvalue", "id_subtype")

    dfSSN.union(dfMRN).union(dfRMRN)
  }

  afterJoinExceptions = Map(
    "H984197_CR2" -> ((df: DataFrame) => {


      val list_rmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")
      val list_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "ALIAS_POOL_CD")
      val list_cmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val list_cmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")
      val list_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val df_active = df.filter(" active_ind = '1' and active_patient_flag = '1' ")
        .withColumn("ID_SUBTYPE",
          when(df("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*), lit("MRN"))
            .when(df("alias_pool_cd").isin(list_cmrn_alias_pool_cd: _*), lit("CMRN"))
            .otherwise(null))



      val list_ssn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SSN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val df1 = df_active.withColumn("SSN", when(df_active("person_alias_type_cd").isin(list_ssn_person_alias_type_cd: _*), df_active("alias")))
      val groups1 = Window.partitionBy(df1("PERSON_ID"), df1("ssn")).orderBy(df1("pa_update_date").desc)
      val df2 = df1.withColumn("ssn_row", row_number.over(groups1))
      val df3 = df2.filter("ssn_row = 1 ")
      val satSSN = standardizeSSN("SSN")
      val dfSSN = satSSN("SSN", df3).filter("SSN is not null")
        .withColumn("IDTYPE", lit("SSN")).withColumnRenamed("SSN", "IDVALUE")
        .select("PERSON_ID","idtype","idvalue", "id_subtype")

      val df5 = df_active.withColumn("idvalue", when(df_active("person_alias_type_cd").isin(list_person_alias_type_cd: _*), df_active("alias")))
      val df6 = df_active.withColumn("idvalue", when(df_active("alias_pool_cd").isin(list_alias_pool_cd: _*), df_active("alias")))
      val df7 = df5.union(df6)
      val df8 = df7.withColumn("idtype", lit("MRN_CMRN"))

      val groups2 = Window.partitionBy(df8("PERSON_ID"), df8("idvalue")).orderBy(df8("pa_update_date").desc)
      val df9 = df8.withColumn("mrn_row", row_number.over(groups2))
      val dfMRN = df9.filter("mrn_row = 1 and idvalue is not null ")
        .select("PERSON_ID","idtype","idvalue", "id_subtype")



      val df10 = df_active.withColumn("idtype", lit("MRN"))
      val list_rmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val df11 = df10.withColumn("idvalue", when(df10("person_alias_type_cd").isin(list_rmrn_person_alias_type_cd: _*) && df10("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*), df10("alias")))
      val groups3 = Window.partitionBy(df11("PERSON_ID"), df11("idvalue")).orderBy(df11("pa_update_date").desc)
      val df12 = df11.withColumn("rmrn_row", row_number.over(groups3))
      val dfRMRN = df12.filter("rmrn_row = 1 and idvalue is not null ")
        .select("PERSON_ID","idtype","idvalue", "id_subtype")


      dfSSN.union(dfMRN).union(dfRMRN)
    }),
    "H984945_CR2" -> ((df: DataFrame) => {


      val list_rmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")
      val list_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "ALIAS_POOL_CD")
      val list_cmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val list_cmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")
      val list_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val df_active = df.filter(" active_ind = '1' and active_patient_flag = '1' ")
        .withColumn("ID_SUBTYPE",
          when(df("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*), lit("MRN"))
            .when(df("alias_pool_cd").isin(list_cmrn_alias_pool_cd: _*), lit("CMRN"))
            .otherwise(null))



      val list_ssn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SSN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val df1 = df_active.withColumn("SSN", when(df_active("person_alias_type_cd").isin(list_ssn_person_alias_type_cd: _*), df_active("alias")))
      val groups1 = Window.partitionBy(df1("PERSON_ID"), df1("ssn")).orderBy(df1("pa_update_date").desc)
      val df2 = df1.withColumn("ssn_row", row_number.over(groups1))
      val df3 = df2.filter("ssn_row = 1 ")
      val satSSN = standardizeSSN("SSN")
      val dfSSN = satSSN("SSN", df3).filter("SSN is not null")
        .withColumn("IDTYPE", lit("SSN")).withColumnRenamed("SSN", "IDVALUE")
        .select("PERSON_ID","idtype","idvalue", "id_subtype")

      val df5 = df_active.withColumn("idvalue", when(df_active("person_alias_type_cd").isin(list_person_alias_type_cd: _*), df_active("alias")))
      val df6 = df_active.withColumn("idvalue", when(df_active("alias_pool_cd").isin(list_alias_pool_cd: _*), df_active("alias")))
      val df7 = df5.union(df6)
      val df8 = df7.withColumn("idtype",lit("MRN"))
        .withColumn("id_subtype",lit("CMRN"))
      val groups2 = Window.partitionBy(df8("PERSON_ID"), df8("idvalue")).orderBy(df8("pa_update_date").desc)
      val df9 = df8.withColumn("mrn_row", row_number.over(groups2))
      val dfMRN = df9.filter("mrn_row = 1 and idvalue is not null ")
        .select("PERSON_ID","idtype","idvalue", "id_subtype")
      val df10 = df_active.withColumn("idtype", lit("MRN"))
      val list_rmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val df11 = df10.withColumn("idvalue", when(df10("person_alias_type_cd").isin(list_rmrn_person_alias_type_cd: _*) && df10("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*), df10("alias")))
        .withColumn("id_subtype", lit(null))
      val groups3 = Window.partitionBy(df11("PERSON_ID"), df11("idvalue")).orderBy(df11("pa_update_date").desc)
      val df12 = df11.withColumn("rmrn_row", row_number.over(groups3))
      val dfRMRN = df12.filter("rmrn_row = 1 and idvalue is not null ")
        .select("PERSON_ID","idtype","idvalue", "id_subtype")


      dfSSN.union(dfMRN).union(dfRMRN)
    }),
    "H565676_CR2" -> (
      (df: DataFrame) => {


        val list_rmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")
        val list_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "ALIAS_POOL_CD")
        val list_cmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val list_cmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")
        val list_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val df_active = df.filter(" active_ind = '1' and active_patient_flag = '1' ")
          .withColumn("ID_SUBTYPE",
            when(df("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*), lit("MRN"))
              .when(df("alias_pool_cd").isin(list_cmrn_alias_pool_cd: _*), lit("CMRN"))
              .otherwise(null))



        val list_ssn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SSN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val df1 = df_active.withColumn("SSN", when(df_active("person_alias_type_cd").isin(list_ssn_person_alias_type_cd: _*), df_active("alias")))
        val groups1 = Window.partitionBy(df1("PERSON_ID"), df1("ssn")).orderBy(df1("pa_update_date").desc)
        val df2 = df1.withColumn("ssn_row", row_number.over(groups1))
        val df3 = df2.filter("ssn_row = 1 ")
        val satSSN = standardizeSSN("SSN")
        val dfSSN = satSSN("SSN", df3).filter("SSN is not null")
          .withColumn("IDTYPE", lit("SSN")).withColumnRenamed("SSN", "IDVALUE")
          .select("PERSON_ID","idtype","idvalue", "id_subtype")


        val df5 = df_active.withColumn("idvalue", when(df_active("person_alias_type_cd").isin(list_person_alias_type_cd: _*), df_active("alias")))
        val df6 = df_active.withColumn("idvalue", when(df_active("alias_pool_cd").isin(list_alias_pool_cd: _*), df_active("alias")))
        val df7 = df5.union(df6)
        val df8 = df7.withColumn("idtype", lit("MRN"))
        val groups2 = Window.partitionBy(df8("PERSON_ID"), df8("idvalue")).orderBy(df8("pa_update_date").desc)
        val df9 = df8.withColumn("mrn_row", row_number.over(groups2))
        val dfMRN = df9.filter("mrn_row = 1 and idvalue is not null ")
          .select("PERSON_ID","idtype","idvalue", "id_subtype")



        val df10 = df_active.withColumn("idtype", lit("MRN"))
        val list_rmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val df11 = df10.withColumn("idvalue", when(df10("person_alias_type_cd").isin(list_rmrn_person_alias_type_cd: _*) && df10("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*), df10("alias")))
        val groups3 = Window.partitionBy(df11("PERSON_ID"), df11("idvalue")).orderBy(df11("pa_update_date").desc)
        val df12 = df11.withColumn("rmrn_row", row_number.over(groups3))
        val dfRMRN = df12.filter("rmrn_row = 1 and idvalue is not null ")
          .select("PERSON_ID","idtype","idvalue", "id_subtype")

        val df14 = df_active.withColumn("EMPI", when(df_active("person_alias_type_cd")=== lit("2"), df_active("alias")))
        val groupsEmpi = Window.partitionBy(df14("PERSON_ID"),when(df14("EMPI").isNull, lit("0")).otherwise(lit("1")))
          .orderBy(df14("end_effective_dt_tm").desc, df14("beg_effective_dt_tm").desc, df14("pa_update_date").desc_nulls_last)

        val df15 = df14.withColumn("empi_row", row_number.over(groupsEmpi))
        val dfEMPI= df15.filter("empi_row = 1 and empi is not null")

          .withColumn("IDTYPE", lit("EMPI")).withColumnRenamed("EMPI", "IDVALUE")
          .withColumn("id_subtype",lit(null))
          .select("PERSON_ID","idtype","idvalue", "id_subtype")

        dfSSN.union(dfMRN).union(dfRMRN).union(dfEMPI)
      }

      )

  )


  map = Map(
    "DATASRC" -> literal("patient"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "idtype" -> mapFrom("IDTYPE"),
    "idvalue" -> mapFrom("IDVALUE"),
    "id_subtype" -> mapFrom("ID_SUBTYPE")
  )


  beforeJoinExceptions = Map(
    "H667594_CR2" -> Map(
      "person" -> ((df: DataFrame) => {
        val df1 = df.filter(
          "not (LOWER(name_last) LIKE 'zz%' OR (LOWER(name_full_formatted) LIKE '%patient%' " +
            "AND LOWER(name_full_formatted) LIKE '%test%')) " +
            "and person_id is not null and (person_id <> '0' and name_full_formatted <> ',' and " +
            "(species_cd = 848 or (species_cd = 0 and (name_first is null or name_first not like '%PET') and " +
            "(name_middle is null or name_middle <> 'PET'))))")

        df1.withColumnRenamed("UPDT_DT_TM", "update_date")
      }),
      "person_alias" -> ((df: DataFrame) => {

        val list_cmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val list_ssn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SSN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val list_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val list_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "ALIAS_POOL_CD")
        val list_rmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val list_rmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")


        val fil = df.filter(" end_effective_dt_tm > current_date() ")
        val list1 = list_cmrn_person_alias_type_cd ::: list_ssn_person_alias_type_cd ::: list_person_alias_type_cd


        val fil1 = fil.filter(fil("person_alias_type_cd").isin(list1: _*) || fil("alias_pool_cd").isin(list_alias_pool_cd: _*))
        val fil2 = fil.filter(fil("person_alias_type_cd").isin(list_rmrn_person_alias_type_cd: _*) && fil("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*))
        val fil3 = fil1.union(fil2)


        val groups = Window.partitionBy(fil3("person_id"), fil3("person_alias_type_cd"), fil3("alias")).orderBy(fil3("UPDT_DT_TM").desc, fil3("active_ind").desc)
        val fil4 = fil3.withColumn("alias_row", row_number.over(groups))

        val groups1 = Window.partitionBy(fil4("person_id"), fil4("person_alias_type_cd")).orderBy(fil4("UPDT_DT_TM").desc, fil4("active_ind").desc).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        val addColumn1 = fil4.withColumn("active_patient_flag", first("active_ind").over(groups1))
        addColumn1.filter("alias_row = 1").withColumnRenamed("UPDT_DT_TM", "pa_update_date")


      }
        )))

}
