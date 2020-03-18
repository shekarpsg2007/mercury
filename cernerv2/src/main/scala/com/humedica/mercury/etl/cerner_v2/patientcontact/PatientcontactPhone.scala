package com.humedica.mercury.etl.cerner_v2.patientcontact

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.Types._

/**
  * Auto-generated on 08/09/2018
  */


class PatientcontactPhone(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("phone", "cdr.map_predicate_values", "patient:cerner_v2.patient.PatientPatient")

  columnSelect = Map(
    "phone" -> List("PHONE_NUM", "UPDT_DT_TM", "PARENT_ENTITY_ID", "PHONE_TYPE_CD", "ACTIVE_IND"),
    "patient" -> List("PATIENTID")
  )

  beforeJoin = Map(
    "phone" -> ((df: DataFrame) => {
      val list_hm_phone_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "HOME_PHONE", "PATIENT_CONTACT", "PHONE", "PHONE_TYPE_CD")

      val list_wk_phone_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "WORK_PHONE", "PATIENT_CONTACT", "PHONE", "PHONE_TYPE_CD")

      val list_cell_phone_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CELL_PHONE", "PATIENT_CONTACT", "PHONE", "PHONE_TYPE_CD")

      val fil = df.filter(
        "parent_entity_name = 'PERSON' " +
          "AND active_ind <> '0' " +
          "AND phone_num is not null " +
          "AND phone_num not like '%@%' " +
          "AND length(phone_num) < 50")
      val list_phone_all = list_hm_phone_type_cd ::: list_wk_phone_type_cd ::: list_cell_phone_type_cd
      val fil1 = fil.filter(fil("PHONE_TYPE_CD").isin(list_phone_all: _*))
      val groups = Window.partitionBy(fil1("PARENT_ENTITY_ID"), fil1("phone_type_cd"), fil1("UPDT_DT_TM") ).orderBy(fil1("UPDT_DT_TM").desc)
      val addColumn = fil1.withColumn("rownumber", row_number.over(groups))
      val df1 = addColumn.filter("parent_entity_id is not null AND updt_dt_tm is not null AND rownumber = 1 ")
      val df2 = df1.withColumn("HOME_PHONE", when(df1("phone_type_cd").isin(list_hm_phone_type_cd: _*), df1("PHONE_NUM")).otherwise(nullColumn))
      val df3 = df2.withColumn("WORK_PHONE", when(df2("phone_type_cd").isin(list_wk_phone_type_cd: _*), df2("PHONE_NUM")).otherwise(nullColumn))
      df3.withColumn("CELL_PHONE", when(df3("phone_type_cd").isin(list_cell_phone_type_cd: _*), df3("PHONE_NUM")).otherwise(nullColumn))
    })
  )


  map = Map(
    "DATASRC" -> literal("phone"),
    "UPDATE_DT" -> mapFrom("UPDT_DT_TM"),
    "PATIENTID" -> mapFrom("PARENT_ENTITY_ID"),
    "CELL_PHONE" -> mapFrom("CELL_PHONE"),
    "HOME_PHONE" -> mapFrom("HOME_PHONE"),
    "WORK_PHONE" -> mapFrom("WORK_PHONE")
  )

  joinExceptions = Map(
    "H667594_CR2" -> ((dfs: Map[String, DataFrame]) => {
      dfs("phone")
        .join(dfs("patient"), dfs("phone")("PARENT_ENTITY_ID") === dfs("patient")("PATIENTID"), "inner")
    })
  )

}