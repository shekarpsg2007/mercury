package com.humedica.mercury.etl.cerner_v2.patientcontact

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 08/09/2018
  */


class PatientcontactAddress(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("address", "cdr.map_predicate_values")

  columnSelect = Map(
    "address" -> List("UPDT_DT_TM", "PARENT_ENTITY_ID", "STREET_ADDR", "ADDRESS_TYPE_CD")
  )

  beforeJoin = Map(
    "address" -> ((df: DataFrame) => {
      val list_EMAIL_ADDRESS = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EMAIL", "PATIENT_CONTACT", "ADDRESS", "ADDRESS_TYPE_CD")
      val fil = df.filter(
        "parent_entity_name = 'PERSON' " +
          "AND PARENT_ENTITY_ID is not null " +
          "AND UPDT_DT_TM is not null " +
          " AND instr(STREET_ADDR,'@') > 1")
      val fil1 = fil.filter(fil("address_type_cd").isin(list_EMAIL_ADDRESS: _*))
      val groups = Window.partitionBy(fil1("UPDT_DT_TM"), fil1("PARENT_ENTITY_ID"), fil1("STREET_ADDR")).orderBy(fil1("UPDT_DT_TM").desc)
      val addColumn = fil1.withColumn("rownumber", row_number.over(groups))
      addColumn.filter("rownumber = 1")
    }))



  map = Map(
    "DATASRC" -> literal("address"),
    "UPDATE_DT" -> mapFrom("UPDT_DT_TM"),
    "PATIENTID" -> mapFrom("PARENT_ENTITY_ID"),
    "PERSONAL_EMAIL" -> mapFrom("STREET_ADDR")
  )

}
