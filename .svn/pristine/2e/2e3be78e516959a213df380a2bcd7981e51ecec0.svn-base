package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Auto-generated on 02/01/2017
  */

class ObservationSmrtdtaelem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("smrtdta_elem",
    "cdr.zcm_obstype_code")

  columnSelect = Map(
    "smrtdta_elem" -> List("PAT_LINK_ID", "CUR_VALUE_DATETIME", "ELEMENT_ID", "CONTACT_SERIAL_NUM", "SMRTDTA_ELEM_VALUE", "UPDATE_DATE"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE", "OBSTYPE_STD_UNITS", "DATATYPE")
  )

  beforeJoin = Map(
    "smrtdta_elem" -> includeIf("PAT_LINK_ID IS NOT NULL AND CUR_VALUE_DATETIME IS NOT NULL"),
    "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
      df.filter("datasrc = 'smrtdta_elem' and groupid = '" + config(GROUP) + "'")
        .drop("DATASRC", "GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("smrtdta_elem")
      .join(dfs("cdr.zcm_obstype_code"), concat(lit(config(CLIENT_DS_ID) + "."),dfs("smrtdta_elem")("ELEMENT_ID")) === dfs("cdr.zcm_obstype_code")("OBSCODE"))
  }


  map = Map(
    "DATASRC" -> literal("smrtdta_elem"),
    "LOCALCODE" -> mapFrom("ELEMENT_ID", prefix = config(CLIENT_DS_ID) + "."),
    "OBSDATE" -> mapFrom("CUR_VALUE_DATETIME"),
    "PATIENTID" -> mapFrom("PAT_LINK_ID"),
    "ENCOUNTERID" -> mapFrom("CONTACT_SERIAL_NUM"),
    "OBSTYPE" -> mapFrom("OBSTYPE"),
    "STD_OBS_UNIT" -> mapFrom("OBSTYPE_STD_UNITS")
  )

  afterMap = (df: DataFrame) => {
    val fpiv = unpivot(
      Seq("ELEMENT_ID", "SMRTDTA_ELEM_VALUE"),
      Seq("ELEMENT_ID", "SMRTDTA_ELEM_VALUE"),typeColumnName = "PIV_COL")
    val piv = fpiv("LOCALRES", df)
    val addColumn = piv.withColumn("LOCALRESULT", substring(when(piv("DATATYPE") === "CV" && piv("LOCALRES").isNotNull, concat(lit(config(CLIENT_DS_ID)+"."), piv("LOCALRES")))
      .otherwise(piv("LOCALRES")),1,255))
    val groups = Window.partitionBy(addColumn("ENCOUNTERID"), addColumn("PATIENTID"), addColumn("LOCALCODE"), addColumn("LOCALRESULT"), addColumn("OBSDATE"), addColumn("OBSTYPE"))
      .orderBy(addColumn("UPDATE_DATE").desc)
    val addColumn2 = addColumn.withColumn("rn", row_number.over(groups))
    addColumn2.filter("rn = 1 and PATIENTID is not null and OBSDATE is not null").drop("rn")
  }

}