package com.humedica.mercury.etl.cerner_v2.patient

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 08/09/2018
  */


class PatientPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  tables = List("person_alias", "person", "cdr.map_predicate_values")

  columnSelect = Map(
    "person_alias" -> List("PERSON_ALIAS_TYPE_CD", "ALIAS_POOL_CD", "ALIAS", "PERSON_ID", "UPDT_DT_TM", "ACTIVE_IND", "END_EFFECTIVE_DT_TM"),
    "person" -> List("PERSON_ID", "BIRTH_DT_TM", "DECEASED_DT_TM", "UPDT_DT_TM", "NAME_FIRST", "NAME_LAST", "NAME_MIDDLE",
      "SPECIES_CD", "NAME_FULL_FORMATTED")
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
      val list_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val list_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "ALIAS_POOL_CD")
      val list_rmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
      val list_rmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")

      val fil = df.filter("end_effective_dt_tm > current_date()")
      val fil1 = fil.filter(fil("person_alias_type_cd").isin(list_person_alias_type_cd: _*) || fil("alias_pool_cd").isin(list_alias_pool_cd: _*))
      val fil2 = fil.filter(fil("person_alias_type_cd").isin(list_rmrn_person_alias_type_cd: _*) && fil("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*))
      val fil3 = fil1.union(fil2)

      val groups = Window.partitionBy(fil3("person_id"), fil3("person_alias_type_cd"), fil3("alias"))
        .orderBy(fil3("UPDT_DT_TM").desc_nulls_last, fil3("active_ind").desc_nulls_last)
      val fil4 = fil3.withColumn("alias_row", row_number.over(groups))

      val groups1 = Window.partitionBy(fil4("person_id"), fil4("person_alias_type_cd"))
        .orderBy(fil4("UPDT_DT_TM").desc_nulls_last, fil4("active_ind").desc_nulls_last)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      val addColumn1 = fil4.withColumn("active_patient_flag", first("active_ind").over(groups1))

      val df1 = addColumn1.filter("alias_row = 1")
      val df2 = df1.withColumn("MRN",
        when(df1("ACTIVE_IND") === lit("1") &&
            (df1("PERSON_ALIAS_TYPE_CD").isin(list_person_alias_type_cd: _*) || df1("ALIAS_POOL_CD").isin(list_alias_pool_cd: _*)),
            df1("ALIAS"))
          .otherwise(null))
        .withColumn("RMRN", when(df1("ACTIVE_IND") === lit("1") &&
            df1("PERSON_ALIAS_TYPE_CD").isin(list_rmrn_person_alias_type_cd: _*) &&
            df1("ALIAS_POOL_CD").isin(list_rmrn_alias_pool_cd: _*),
            df1("ALIAS"))
          .otherwise(null))

      df2.withColumnRenamed("UPDT_DT_TM", "pa_update_date")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("person")
      .join(dfs("person_alias"), Seq("PERSON_ID"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("patient"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "DATEOFBIRTH" -> ((col: String, df: DataFrame) => {
      val groups = Window.partitionBy(df("PERSON_ID"))
        .orderBy(df("ACTIVE_IND").desc_nulls_last, when(df("BIRTH_DT_TM").isNotNull, lit("1")).otherwise(lit("0")).desc, df("update_date").desc_nulls_last)
      df.withColumn(col, first(df("BIRTH_DT_TM"), ignoreNulls = true).over(groups))
    }),
    "DATEOFDEATH" -> ((col: String, df: DataFrame) => {
      val groups = Window.partitionBy(df("PERSON_ID"))
        .orderBy(df("update_date").asc_nulls_first)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      df.withColumn(col, last(df("DECEASED_DT_TM")).over(groups))
    }),
    "MEDICALRECORDNUMBER" -> ((col: String, df: DataFrame) => {
      val groups = Window.partitionBy(df("PERSON_ID"))
        .orderBy(df("ACTIVE_IND").desc_nulls_last, when(df("MRN").isNotNull, lit("1")).otherwise(lit("0")).desc, df("pa_update_date").desc_nulls_last)
      df.withColumn(col, first(df("MRN"), ignoreNulls = true).over(groups))
    })
  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("PATIENTID")).orderBy(df("update_date").desc_nulls_last)
    df.withColumn("rn", row_number.over(groups))
      .filter("rn = 1")
      .drop("rn")
  }

  mapExceptions = Map(
    ("H984197_CR2", "MEDICALRECORDNUMBER") -> ((col: String, df: DataFrame) => {
        val groups = Window.partitionBy(df("PERSON_ID"))
          .orderBy(df("ACTIVE_IND").desc_nulls_last, when(df("RMRN").isNotNull, lit("1")).otherwise(lit("0")).desc, df("pa_update_date").desc_nulls_last)
        df.withColumn(col, first(df("RMRN"), ignoreNulls = true).over(groups))
      }),
    ("H984945_CR2", "MEDICALRECORDNUMBER") -> ((col: String, df: DataFrame) => {
      val groups = Window.partitionBy(df("PERSON_ID"))
        .orderBy(df("ACTIVE_IND").desc_nulls_last, when(df("RMRN").isNotNull, lit("1")).otherwise(lit("0")).desc, df("pa_update_date").desc_nulls_last)
      df.withColumn(col, first(df("RMRN"), ignoreNulls = true).over(groups))
    })
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
        val list_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val list_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "PATIENT", "PATIENT", "PERSON_ALIAS", "ALIAS_POOL_CD")
        val list_rmrn_person_alias_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD")
        val list_rmrn_alias_pool_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD")

        val fil = df.filter("end_effective_dt_tm > current_date()")
        val fil1 = fil.filter(fil("person_alias_type_cd").isin(list_person_alias_type_cd: _*) || fil("alias_pool_cd").isin(list_alias_pool_cd: _*))
        val fil2 = fil.filter(fil("person_alias_type_cd").isin(list_rmrn_person_alias_type_cd: _*) && fil("alias_pool_cd").isin(list_rmrn_alias_pool_cd: _*))
        val fil3 = fil1.union(fil2)

        val groups = Window.partitionBy(fil3("person_id"), fil3("person_alias_type_cd"), fil3("alias"))
          .orderBy(fil3("UPDT_DT_TM").desc_nulls_last, fil3("active_ind").desc_nulls_last)
        val fil4 = fil3.withColumn("alias_row", row_number.over(groups))

        val groups1 = Window.partitionBy(fil4("person_id"), fil4("person_alias_type_cd"))
          .orderBy(fil4("UPDT_DT_TM").desc_nulls_last, fil4("active_ind").desc_nulls_last)
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        val addColumn1 = fil4.withColumn("active_patient_flag", first("active_ind").over(groups1))

        val df1 = addColumn1.filter("alias_row = 1")
        val df2 = df1.withColumn("MRN",
          when(df1("ACTIVE_IND") === lit("1") &&
            (df1("PERSON_ALIAS_TYPE_CD").isin(list_person_alias_type_cd: _*) || df1("ALIAS_POOL_CD").isin(list_alias_pool_cd: _*)),
            df1("ALIAS"))
            .otherwise(null))
          .withColumn("RMRN", when(df1("ACTIVE_IND") === lit("1") &&
            df1("PERSON_ALIAS_TYPE_CD").isin(list_rmrn_person_alias_type_cd: _*) &&
            df1("ALIAS_POOL_CD").isin(list_rmrn_alias_pool_cd: _*),
            df1("ALIAS"))
            .otherwise(null))

        df2.withColumnRenamed("UPDT_DT_TM", "pa_update_date")
      })
    )
  )

}