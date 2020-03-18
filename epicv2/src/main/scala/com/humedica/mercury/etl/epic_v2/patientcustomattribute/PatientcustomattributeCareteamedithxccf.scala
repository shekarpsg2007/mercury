package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
  * Created by abendiganavale on 5/17/18.
  */
class PatientcustomattributeCareteamedithxccf(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("temptable:epic_v2.patient.PatientTemptable"
    ,"care_team_edit_hx"
    ,"patassessment"
    ,"smrtdta_elem"
    ,"zh_providerattr"
    ,"pat_attr_crosswalk"
    ,"cdr.map_predicate_values")
  
  columnSelect = Map(
    "temptable" -> List("PATIENTID"),
    "care_team_edit_hx" -> List("PAT_ID","LINE","CHANGE_DATETIME","CHANGE_USER_ID","CHANGE_TYPE_C","PROV_ID","EFF_NEW_DT"
      ,"TERMINATION_NEW_DT","LINE_NUM","RELATIONSHIP_NEW_C"),
    "patassessment" -> List("PAT_ENC_CSN_ID", "MEAS_VALUE", "FLO_MEAS_ID", "PAT_ID", "RECORDED_TIME", "ENTRY_TIME"),
    "smrtdta_elem" -> List("PAT_LINK_ID","SMRTDTA_ELEM_VALUE","ELEMENT_ID","VALUE_LINE","CONTACT_SERIAL_NUM","UPDATE_DATE"),
    "zh_providerattr" -> List("PROV_ID", "PROV_NAME","EXTRACT_DATE")
  )

  beforeJoin = Map(
    "care_team_edit_hx" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val list_relationship_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CARE_TEAM_EDIT_HX_CCF", "PATIENT_ATTRIBUTE", "CARE_TEAM_EDIT_HX", "RELATIONSHIP_NEW_C")
      val fil = df1.filter("RELATIONSHIP_NEW_C in (" + list_relationship_c + ")")
      val groups = Window.partitionBy(fil("PAT_ID")).orderBy(fil("CHANGE_DATETIME").desc, fil("TERMINATION_NEW_DT").desc_nulls_first, fil("EFF_NEW_DT").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1").drop("rn")
               .withColumnRenamed("PAT_ID", "PAT_ID_CARE")
               .withColumn("TERMDATE", when(fil("TERMINATION_NEW_DT").isNull, lit("CURRENT")).otherwise(substring(fil("TERMINATION_NEW_DT"),1, 10)))
               .withColumn("EFFDATE", substring(fil("EFF_NEW_DT"),1,10))
    }),
    "patassessment" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val list_flo_meas_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CARE_TEAM_EDIT_HX_CCF", "PATIENT_ATTRIBUTE", "PATASSESSMENT", "FLO_MEAS_ID")
      val fil = df1.filter("FLO_MEAS_ID in (" + list_flo_meas_id + ")")
      val groups = Window.partitionBy(fil("PAT_ID")).orderBy(fil("ENTRY_TIME").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      val fil1 = addColumn.filter("rn = 1")
               //.withColumn("MEAS_VAL", when(addColumn("MEAS_VALUE").isNotNull, concat_ws("", lit("High risk, "), addColumn("MEAS_VALUE")))
               //                        .otherwise(lit("High risk")))
                .withColumn("MEAS_VALUE2", regexp_replace(addColumn("MEAS_VALUE"),"\\?","-"))
                .drop("rn")
      val attr = table("pat_attr_crosswalk")
      val join1 =  fil1.join(attr, fil1("MEAS_VALUE2") === attr("MHC_SOURCE_VALUE") && (attr("ATTRIBUTE") === lit("STRATIFICATION")), "left_outer")
      join1.withColumn("MEAS_VAL", when(join1("MHC_MAPPED_VALUE").isNotNull, join1("MHC_MAPPED_VALUE")).otherwise(join1("MEAS_VALUE2")))
    }),
    "smrtdta_elem" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val list_element_id = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CARE_TEAM_EDIT_HX_CCF", "PATIENT_ATTRIBUTE", "SMRTDTA_ELEM", "ELEMENT_ID")
      val fil = df1.filter("ELEMENT_ID in (" + list_element_id + ")")
      val groups = Window.partitionBy(fil("ELEMENT_ID"),fil("PAT_LINK_ID"),fil("VALUE_LINE"),fil("CONTACT_SERIAL_NUM")).orderBy(fil("UPDATE_DATE").desc)
      val addColumn = fil.withColumn("rn", row_number.over(groups))
      val fil1 = addColumn.filter("rn = 1")
               .withColumn("ELEM_VALUE", regexp_replace(regexp_replace(addColumn("SMRTDTA_ELEM_VALUE"),"\"",""),"\\?","-") )
               .drop("rn")
      val attr = table("pat_attr_crosswalk")
      val join1 = fil1.join(attr, fil1("ELEM_VALUE") === attr("MHC_SOURCE_VALUE") && (attr("ATTRIBUTE") === lit("STATUS")), "left_outer")
      join1.withColumn("ELEM_VAL", when(join1("MHC_MAPPED_VALUE").isNotNull, join1("MHC_MAPPED_VALUE")).otherwise(join1("ELEM_VALUE")))
    }),
    "zh_providerattr" -> ((df: DataFrame) => {
      val df1 = df.repartition(500)
      val groups = Window.partitionBy(df1("PROV_ID")).orderBy(df1("EXTRACT_DATE").desc)
      val addColumn = df1.withColumn("rn", row_number.over(groups))
      addColumn.filter("rn = 1")
                .withColumnRenamed("PROV_ID", "PROV_ID_zh")
                .drop("rn")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("temptable")
      .join(dfs("care_team_edit_hx"), dfs("temptable")("PATIENTID") === dfs("care_team_edit_hx")("PAT_ID_CARE"), "left_outer")
      .join(dfs("patassessment"), dfs("temptable")("PATIENTID") === dfs("patassessment")("PAT_ID"), "left_outer")
      .join(dfs("smrtdta_elem"), dfs("temptable")("PATIENTID") === dfs("smrtdta_elem")("PAT_LINK_ID"), "left_outer")
      .join(dfs("zh_providerattr"), dfs("care_team_edit_hx")("PROV_ID") === dfs("zh_providerattr")("PROV_ID_zh"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val include_group = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CARE_TEAM_EDIT_HX_CCF", "PATIENT_ATTRIBUTE", "CARE_TEAM_EDIT_HX_CCF", "INCLUSION")
    val fil = df.filter("'Y' = (" + include_group + ")")
    val addColumn = fil.withColumn("PROV_NAME_ID", concat_ws("", when(df("PROV_NAME").isNotNull, concat_ws("", df("PROV_NAME"), lit(", "))).otherwise(null), df("PROV_ID")))
                       .withColumn("MEAS_VALUE1", when(df("MEAS_VAL").isNull && df("PAT_ID_CARE").isNotNull, lit("High risk")).otherwise(df("MEAS_VAL")) )
    val fpiv1 = unpivot(
      Seq("EFFDATE", "TERMDATE", "PROV_NAME_ID", "ELEM_VAL","MEAS_VALUE1"),
      Seq("CH002786", "CH002787", "CH002788", "CH002949","CH002950"), typeColumnName = "ATTRIBUTEVALUETYPE"
    )
    fpiv1("ATTRIBUTEVALUE", addColumn)
  }


  map = Map(
    "DATASRC" -> literal("care_team_edit_hx_ccf"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ATTRIBUTE_TYPE_CUI" -> mapFrom("ATTRIBUTEVALUETYPE"),
    "ATTRIBUTE_VALUE" -> mapFrom("ATTRIBUTEVALUE")
  )


  afterMap = (df: DataFrame) => {
    df.filter("ATTRIBUTE_VALUE <> '' and PATIENTID IS NOT NULL").distinct()
  }

}
