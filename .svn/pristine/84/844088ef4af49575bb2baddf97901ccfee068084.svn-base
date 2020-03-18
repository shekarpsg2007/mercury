package com.humedica.mercury.etl.cerner_v2.patientreportedmeds

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, _}

/**
  * Created by abendiganavale on 9/24/18.
  */
class PatientreportedmedsOrdercompliance(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("order_compliance",
    "order_compliance_detail",
    "orders",
    "order_action",
    "order_detail",
    "zh_code_value",
    "tempmed:cerner_v2.rxordersandprescriptions.RxordersandprescriptionsTempmed",
    "cdr.map_predicate_values"
  )

  columnSelect = Map(
    "order_compliance" -> List("ORDER_COMPLIANCE_ID","ENCNTR_ID","PERFORMED_DT_TM","UPDT_DT_TM","ENCNTR_COMPLIANCE_STATUS_FLAG"),
    "order_compliance_detail" -> List("ORDER_COMPLIANCE_ID","ORDER_COMPLIANCE_DETAIL_ID","ORDER_NBR","UPDT_DT_TM"),
    "orders" -> List("ORDER_ID","UPDT_DT_TM","PERSON_ID","CATALOG_CD"),
    "order_action" -> List("ORDER_ID","ACTION_SEQUENCE","UPDT_DT_TM","ACTION_TYPE_CD"),
    "order_detail" -> List("ORDER_ID","ACTION_SEQUENCE","UPDT_DT_TM","OE_FIELD_ID","OE_FIELD_DISPLAY_VALUE","OE_FIELD_VALUE"),
    "zh_code_value" -> List("CODE_VALUE","DESCRIPTION","DISPLAY"),
    "tempmed" -> List("CATALOG_CD","LOCALNDC")
  )

  beforeJoin = Map(
    "order_compliance" -> ((df: DataFrame) => {
      val df1 = df.repartition(500)
      val groups1 = Window.partitionBy(df1("ORDER_COMPLIANCE_ID")).orderBy(df1("UPDT_DT_TM").desc_nulls_last)
      val oc_tbl = df1.withColumn("rn_oc", row_number.over(groups1))
                         .filter("rn_oc = 1").drop("rn_oc")

      val ocd = table("order_compliance_detail").repartition(500)
      val groups2 = Window.partitionBy(ocd("ORDER_COMPLIANCE_DETAIL_ID")).orderBy(ocd("UPDT_DT_TM").desc_nulls_last)
      val ocd_tbl = ocd.withColumn("rn_ocd", row_number.over(groups2))
                       .filter("rn_ocd = 1").drop("rn_ocd")

      val ord = table("orders").repartition(500)
      val groups3 = Window.partitionBy(ord("ORDER_ID")).orderBy(ord("UPDT_DT_TM").desc_nulls_last)
      val ord_tbl = ord.withColumn("rn_ord", row_number.over(groups3))
                       .filter("rn_ord = 1 and PERSON_ID is not null").drop("rn_ord")

      val joined = oc_tbl.join(ocd_tbl, Seq("ORDER_COMPLIANCE_ID"),"inner")
                         .join(ord_tbl, ocd_tbl("ORDER_NBR") === ord_tbl("ORDER_ID"),"inner")
      joined.filter("ENCNTR_COMPLIANCE_STATUS_FLAG = 0 and ORDER_COMPLIANCE_DETAIL_ID is not null")
            .withColumnRenamed("PERSON_ID","PATIENT_ID")
            .withColumnRenamed("ORDER_COMPLIANCE_DETAIL_ID","REPORTEDMEDID")
            .withColumnRenamed("ENCNTR_ID","ENCOUNTER_ID")
            .withColumnRenamed("PERFORMED_DT_TM","MEDREPORTED_TIME")
            //.withColumnRenamed("ORDER_NBR","OC_ORDER_NBR")
            .withColumnRenamed("CATALOG_CD","LOCALMED_CODE")
            .withColumnRenamed("ORDER_ID","ORD_ORDER_ID")
            .select("PATIENT_ID","REPORTEDMEDID","ENCOUNTER_ID","MEDREPORTED_TIME","LOCALMED_CODE","ORD_ORDER_ID")
    }),
    "order_action" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ORDER_ID"),df("ACTION_SEQUENCE")).orderBy(df("UPDT_DT_TM").desc_nulls_last)
      df.withColumn("rn_oa", row_number.over(groups))
                        .filter("rn_oa = 1").drop("rn_oa")
                        .withColumnRenamed("ORDER_ID","OA_ORDER_ID")
                        .withColumnRenamed("ACTION_SEQUENCE","OA_ACTION_SEQUENCE")
                        .select("OA_ORDER_ID","ACTION_TYPE_CD","OA_ACTION_SEQUENCE")
    }),
    "order_detail" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ORDER_ID"),df("ACTION_SEQUENCE"),df("OE_FIELD_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last)
      df.withColumn("rn_od", row_number.over(groups))
        .filter("rn_od = 1").drop("rn_od")
        .withColumnRenamed("ORDER_ID","OD_ORDER_ID")
        .withColumnRenamed("ACTION_SEQUENCE","OD_ACTION_SEQUENCE")
        .select("OD_ORDER_ID","OE_FIELD_ID","OE_FIELD_DISPLAY_VALUE","OE_FIELD_VALUE","OD_ACTION_SEQUENCE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("order_compliance")
      .join(dfs("order_action"), dfs("order_compliance")("ORD_ORDER_ID") === dfs("order_action")("OA_ORDER_ID"),"left_outer")
      .join(dfs("order_detail"), (dfs("order_action")("OA_ORDER_ID") === dfs("order_detail")("OD_ORDER_ID")) && (dfs("order_action")("OA_ACTION_SEQUENCE") === dfs("order_detail")("OD_ACTION_SEQUENCE")),"left_outer")
      .join(dfs("zh_code_value"), dfs("order_detail")("OE_FIELD_VALUE") === dfs("zh_code_value")("CODE_VALUE"),"left_outer")
  }

  afterJoin = (df: DataFrame) => {

    val list_rxroute_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "RXROUTE", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_sd_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "STRENGTHDOSE", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_sdunit_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "STRENGTHDOSEUNIT", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_vd_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "VOLUMEDOSE", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_vdunit_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "VOLUMEDOSEUNIT", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_no_action_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "NEW_ORDER_CD", "RX_PAT_REP", "ORDER_ACTION", "ACTION_TYPE_CD")
    val list_df_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DRUGFORM", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")

    val df1 = df.groupBy("PATIENT_ID","REPORTEDMEDID","ENCOUNTER_ID","LOCALMED_CODE","MEDREPORTED_TIME","OE_FIELD_VALUE","DESCRIPTION")
                .agg(
                  max(when(df("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && df("OE_FIELD_ID").isin(list_vdunit_oe_field_id: _*), df("DESCRIPTION")).otherwise(null)).as("LOCAL_DOSE_UNIT"),
                  max(when(df("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && df("OE_FIELD_ID").isin(list_df_oe_field_id: _*), df("DESCRIPTION")).otherwise(null)).as("LOCAL_FORM"),
                  max(when(df("OE_FIELD_ID").isin(list_vd_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("LOCAL_QTY_OF_DOSE_UNIT"),
                  max(when(df("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && df("OE_FIELD_ID").isin(list_rxroute_oe_field_id: _*),
                        when(df("OE_FIELD_VALUE").isNotNull, concat_ws(".", lit(config(CLIENT_DS_ID)),df("OE_FIELD_VALUE")))).otherwise(null)).as("LOCAL_ROUTE"),
                  max(when(df("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && df("OE_FIELD_ID").isin(list_sd_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("LOCAL_STRENGTHPERDOSEUNIT"),
                  max(when(df("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && df("OE_FIELD_ID").isin(list_sdunit_oe_field_id: _*), df("DESCRIPTION")).otherwise(null)).as("LOCAL_STRENGTHUNIT")
                )

    val med = table("tempmed").withColumnRenamed("LOCALNDC","MED_LOCALNDC")
    val zc = table("zh_code_value").withColumnRenamed("DISPLAY","ZH_DISPLAY").withColumnRenamed("CODE_VALUE","ZH_CODE_VAL")

    val join1 = df1.join(med, df1("LOCALMED_CODE") === med("CATALOG_CD"),"left_outer")
                   .join(zc, df1("LOCALMED_CODE") === zc("ZH_CODE_VAL"), "left_outer")
                   .select("PATIENT_ID","ENCOUNTER_ID","MEDREPORTED_TIME","LOCALMED_CODE","LOCAL_DOSE_UNIT","LOCAL_FORM",
                          "LOCAL_QTY_OF_DOSE_UNIT","LOCAL_ROUTE","LOCAL_STRENGTHPERDOSEUNIT","LOCAL_STRENGTHUNIT",
                          "ZH_DISPLAY","MED_LOCALNDC","REPORTEDMEDID")

    join1.groupBy("REPORTEDMEDID")
         .agg(
           max("PATIENT_ID").as("PATIENTID"),max("ENCOUNTER_ID").as("ENCOUNTERID"),max("MEDREPORTED_TIME").as("MEDREPORTEDTIME"),
           max("LOCALMED_CODE").as("LOCALMEDCODE"),max("LOCAL_DOSE_UNIT").as("LOCALDOSEUNIT"),max("LOCAL_FORM").as("LOCALFORM"),
           max("LOCAL_QTY_OF_DOSE_UNIT").as("LOCALQTYOFDOSEUNIT"),max("LOCAL_ROUTE").as("LOCALROUTE"),
           max("LOCAL_STRENGTHPERDOSEUNIT").as("LOCALSTRENGTHPERDOSEUNIT"),max("LOCAL_STRENGTHUNIT").as("LOCALSTRENGTHUNIT"),
           max("ZH_DISPLAY").as("LOCALDRUGDESCRIPTION"),max("MED_LOCALNDC").as("LOCALNDC")
         )
  }

  map = Map(
    "DATASRC" -> literal("order_compliance"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
    "LOCALDRUGDESCRIPTION" -> mapFrom("LOCALDRUGDESCRIPTION"),
    "LOCALQTYOFDOSEUNIT" -> mapFrom("LOCALQTYOFDOSEUNIT"),
    "LOCALFORM" -> mapFrom("LOCALFORM"),
    "LOCALNDC" -> mapFrom("LOCALNDC"),
    "LOCALROUTE" -> mapFrom("LOCALROUTE"),
    "LOCALSTRENGTHPERDOSEUNIT" -> mapFrom("LOCALSTRENGTHPERDOSEUNIT"),
    "LOCALSTRENGTHUNIT" -> mapFrom("LOCALSTRENGTHUNIT"),
    "LOCALDOSEUNIT" -> mapFrom("LOCALDOSEUNIT"),
    "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("LOCALSTRENGTHPERDOSEUNIT").rlike("^[+-]?\\d+\\.?\\d*$"), df("LOCALSTRENGTHPERDOSEUNIT")).otherwise(null)
        .multiply(when(df("LOCALQTYOFDOSEUNIT").rlike("^[+-]?\\d+\\.?\\d*$"), df("LOCALQTYOFDOSEUNIT")).otherwise(null)))}),
    "REPORTEDMEDID" -> mapFrom("REPORTEDMEDID"),
    "MEDREPORTEDTIME" -> mapFrom("MEDREPORTEDTIME"),
    "LOCALMEDCODE" -> mapFrom("LOCALMEDCODE")
  )

}