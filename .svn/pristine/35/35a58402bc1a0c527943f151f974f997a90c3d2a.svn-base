package com.humedica.mercury.etl.athena.util

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._


class UtilSplitTable(config: Map[String, String]) {

  val dataSrc = "ATHENA"
  val entity = "WHITELIST"

  val columnValue = lookupValue(readTable("cdr.map_predicate_values", config).where("GROUPID='" + config(GROUP) + "' AND CLIENT_DS_ID='" + config(CLIENT_DS_ID) + "' AND DATA_SRC = '" + dataSrc + "' AND ENTITY ='" + entity + "' AND instr(table_name, '.') >0"), "1=1","COLUMN_VALUE")
 // println("columnValue: "+ columnValue)
  val columnName = lookupValue(readTable("cdr.map_predicate_values", config).where("GROUPID='" + config(GROUP) + "' AND CLIENT_DS_ID='" + config(CLIENT_DS_ID) + "' AND DATA_SRC = '" + dataSrc + "' AND ENTITY ='" + entity + "' AND instr(table_name, '.') >0"), "1=1","COLUMN_NAME")
 // println("columnName: "+ columnName)
  val tblName = lookupValue(readTable("cdr.map_predicate_values", config).where("GROUPID='" + config(GROUP) + "' AND CLIENT_DS_ID='" + config(CLIENT_DS_ID) + "' AND DATA_SRC = '" + dataSrc + "' AND ENTITY ='" + entity + "' AND instr(table_name, '.') >0"),  "1=1","TABLE_NAME")
  val tableName = if (tblName == null) null else tblName.substring(tblName.indexOf(".")+1)
 // println("tableName: "+ tableName)
  val schemaName = if (tblName == null) null else tblName.substring(1,tblName.indexOf("."))
 // println("schemaName: "+ schemaName)
  val patprovJoinType = if (columnValue == null) "left_outer" else "inner"
 // println("patprovJoinType: "+ patprovJoinType)

}

// test
// val a = new UtilSplitTable(cfg)

