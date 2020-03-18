package com.humedica.mercury.etl.cerner_v2.patientreportedmeds

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
 * Auto-generated on 08/09/2018
 */


class PatientreportedmedsOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("orders",
                "order_action",
                "tempmed:cerner_v2.rxordersandprescriptions.RxordersandprescriptionsTempmed",
                "order_detail",
                "zh_code_value",
                "cdr.map_predicate_values"
  )

  columnSelect = Map(
    "orders" -> List("ORDER_ID","PERSON_ID", "ENCNTR_ID", "CATALOG_CD","UPDT_DT_TM","TEMPLATE_ORDER_ID","ORIG_ORD_AS_FLAG","ACTIVE_IND"),
    "order_action" -> List("ORDER_ID","ACTION_SEQUENCE","ACTION_DT_TM","ACTION_TYPE_CD","ORDER_PROVIDER_ID","STOP_TYPE_CD"
          ,"UPDT_DT_TM","ORDER_DT_TM"),
    "tempmed" -> List("LOCALNDC","CATALOG_CD"),
    "order_detail" -> List("ORDER_ID","OE_FIELD_DISPLAY_VALUE","OE_FIELD_VALUE","OE_FIELD_ID","ACTION_SEQUENCE","UPDT_DT_TM","OE_FIELD_DT_TM_VALUE"),
    "zh_code_value" -> List("DISPLAY","DESCRIPTION","CODE_VALUE","CODE_SET")
  )

  beforeJoin = Map(
    "orders" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val list_activity_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "RX", "ORDERS", "ACTIVITY_TYPE_CD")
      val groups1 = Window.partitionBy(df1("ORDER_ID")).orderBy(df1("UPDT_DT_TM").desc_nulls_last)
      val ord_tbl = df1.withColumn("rn_ord", row_number.over(groups1))
        .filter("TEMPLATE_ORDER_ID = '0' and ORIG_ORD_AS_FLAG = '2' and ACTIVITY_TYPE_CD in (" + list_activity_type_cd + ")")

      val zc = table("zh_code_value").withColumnRenamed("DISPLAY","ZC_DISPLAY")
      val joined = ord_tbl.join(zc, ord_tbl("CATALOG_CD") === zc("CODE_VALUE") && zc("CODE_SET") === lit("200"), "left_outer")
      joined.filter("rn_ord = 1").drop("rn_ord")
            .select("ORDER_ID","PERSON_ID", "ENCNTR_ID", "ZC_DISPLAY", "CATALOG_CD","ACTIVE_IND")
    }),
    "order_action" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val list_dl_action_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DELETE_ORDER_CD", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD")
      val zc1 = table("zh_code_value").withColumnRenamed("DESCRIPTION","ZC1_DESCRIPTION")
      val joined = df1.join(zc1, df1("ACTION_TYPE_CD") === zc1("CODE_VALUE"), "left_outer")
      val groups = Window.partitionBy(joined("ORDER_ID"),joined("ACTION_SEQUENCE")).orderBy(joined("UPDT_DT_TM").desc_nulls_last)
      joined.withColumn("rn_oa", row_number.over(groups))
        .filter("rn_oa = 1").drop("rn_oa")
        .withColumnRenamed("ORDER_ID","OA_ORDER_ID")
        .withColumnRenamed("ACTION_SEQUENCE","OA_ACTION_SEQUENCE")
        .withColumn("OA_DELETE_FLAG", when(joined("ACTION_TYPE_CD").isin(list_dl_action_type_cd: _*), lit("Y")).otherwise(null))
        .select("OA_ORDER_ID","ACTION_TYPE_CD","OA_ACTION_SEQUENCE","OA_DELETE_FLAG","ORDER_PROVIDER_ID","STOP_TYPE_CD"
          ,"ACTION_DT_TM","ZC1_DESCRIPTION","ORDER_DT_TM")
    }),
    "order_detail" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val zc2 = table("zh_code_value").withColumnRenamed("DESCRIPTION","ZC2_DESCRIPTION")
      val joined = df1.join(zc2, df1("OE_FIELD_VALUE") === zc2("CODE_VALUE"), "left_outer")
      val groups = Window.partitionBy(joined("ORDER_ID"),joined("ACTION_SEQUENCE"),joined("OE_FIELD_ID")).orderBy(joined("UPDT_DT_TM").desc_nulls_last)
      joined.withColumn("rn_od", row_number.over(groups))
        .filter("rn_od = 1").drop("rn_od")
        .withColumnRenamed("ORDER_ID","OD_ORDER_ID")
        .withColumnRenamed("ACTION_SEQUENCE","OD_ACTION_SEQUENCE")
        .select("OD_ORDER_ID","OE_FIELD_ID","OE_FIELD_DISPLAY_VALUE","OE_FIELD_VALUE","OD_ACTION_SEQUENCE","ZC2_DESCRIPTION"
            ,"OE_FIELD_DT_TM_VALUE")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("orders")
      .join(dfs("order_action"), dfs("orders")("ORDER_ID") === dfs("order_action")("OA_ORDER_ID"), "left_outer")
      .join(dfs("order_detail"), (dfs("order_action")("OA_ORDER_ID") === dfs("order_detail")("OD_ORDER_ID")) && (dfs("order_action")("OA_ACTION_SEQUENCE") === dfs("order_detail")("OD_ACTION_SEQUENCE")),"left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val list_rxroute_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "RXROUTE", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_sd_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "STRENGTHDOSE", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_sdunit_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "STRENGTHDOSEUNIT", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_vd_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "VOLUMEDOSE", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_vdunit_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "VOLUMEDOSEUNIT", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_no_action_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "NEW_ORDER_CD", "RX_PAT_REP", "ORDER_ACTION", "ACTION_TYPE_CD")
    val list_df_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DRUGFORM", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_rqstartdt_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "REQSTARTDTTM", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_stopdt_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "STOPDTTM", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_cancel_action_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CANCEL_ACTION_TYPE", "RX_PAT_REP", "ORDER_ACTION", "ACTION_TYPE_CD")
    val list_cr_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CANCELREASON", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_dis_action_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DISCONTINUE_ACTION_TYPE", "RX_PAT_REP", "ORDER_ACTION", "ACTION_TYPE_CD")
    val list_dcreason_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DCREASON", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_susreason_oe_field_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SUSPENDREASON", "RX_PAT_REP", "ORDER_DETAIL", "OE_FIELD_ID")
    val list_action_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ACTION_TYPE_CD", "RX_PAT_REP", "ORDERS", "ACTION_TYPE_CD")
    val list_suspend_action_type_cd = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SUSPEND_ACTION_TYPE", "RX_PAT_REP", "ORDER_ACTION", "ACTION_TYPE_CD")

    val fil = df.withColumn("LOCALMEDCODE", when(df("CATALOG_CD") === lit("0"), null).otherwise(df("CATALOG_CD")))
                .withColumnRenamed("PERSON_ID", "PATIENTID")
                .withColumnRenamed("ZC_DISPLAY", "LOCALDRUGDESCRIPTION")
                .withColumnRenamed("ENCNTR_ID", "ENCOUNTERID")
                .withColumnRenamed("ORDER_ID", "REPORTEDMEDID")
                .filter("ACTIVE_IND != '0' and PATIENTID is not null and REPORTEDMEDID is not null")

    val df1 = fil.groupBy("PATIENTID","ENCOUNTERID","REPORTEDMEDID","LOCALMEDCODE","LOCALDRUGDESCRIPTION")
                  .agg(
                    max(fil("ACTION_DT_TM")).as("ACTIONTIME"),
                    max(fil("ACTION_TYPE_CD")).as("LOCALCATEGORYCD"),
                    max(when(fil("ORDER_PROVIDER_ID") =!= lit("0"), fil("ORDER_PROVIDER_ID"))).as("LOCALPROVIDERID"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*), fil("ORDER_DT_TM"))).as("MEDREPORTEDTIME"),
                    max(fil("OA_DELETE_FLAG")).as("DELETE_FLG"),
                    max(fil("ZC1_DESCRIPTION")).as("LOCALACTIONCODE"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_vdunit_oe_field_id: _*), fil("ZC2_DESCRIPTION")).otherwise(null)).as("LOCAL_DOSE_UNIT"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_df_oe_field_id: _*), fil("ZC2_DESCRIPTION")).otherwise(null)).as("LOCAL_FORM"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_vd_oe_field_id: _*), translate(fil("OE_FIELD_DISPLAY_VALUE"), ",", "")).otherwise(null)).as("LOCAL_QTY_OF_DOSE_UNIT"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_rxroute_oe_field_id: _*), fil("OE_FIELD_VALUE")).otherwise(null)).as("LOCAL_ROUTE"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_sd_oe_field_id: _*), translate(fil("OE_FIELD_DISPLAY_VALUE"), ",", "")).otherwise(null)).as("LOCAL_STRENGTHPERDOSEUNIT"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_no_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_sdunit_oe_field_id: _*), fil("ZC2_DESCRIPTION")).otherwise(null)).as("LOCAL_STRENGTHUNIT"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_rqstartdt_oe_field_id: _*), fil("OE_FIELD_DT_TM_VALUE"))
                       .when(!fil("ACTION_TYPE_CD").isin(list_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_stopdt_oe_field_id: _*), fil("OE_FIELD_DT_TM_VALUE"))
                       .otherwise(null)
                    ).as("DISCONTINUEDATE"),
                    max(when(fil("ACTION_TYPE_CD").isin(list_cancel_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_cr_oe_field_id: _*), fil("ZC2_DESCRIPTION"))
                       .when(fil("ACTION_TYPE_CD").isin(list_dis_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_dcreason_oe_field_id: _*), fil("ZC2_DESCRIPTION"))
                       .when(fil("ACTION_TYPE_CD").isin(list_suspend_action_type_cd: _*) && fil("OE_FIELD_ID").isin(list_susreason_oe_field_id: _*), fil("ZC2_DESCRIPTION"))
                       .otherwise(when(fil("STOP_TYPE_CD") === lit("0"), null).otherwise(fil("STOP_TYPE_CD")))
                    ).as("DISCONTINUEREASON")
                  )

    val med = table("tempmed").withColumnRenamed("CATALOG_CD", "MED_CATALOG_CD")

    val medtbl = df1.join(med, df1("LOCALMEDCODE") === med("MED_CATALOG_CD"),"left_outer")
                    .filter("LOCALMEDCODE is not null")

    val groups = Window.partitionBy(medtbl("REPORTEDMEDID"))

    medtbl.withColumn("LOCALCATEGORYCODE", when(medtbl("LOCALCATEGORYCD").isNotNull, concat_ws(".", lit(config(CLIENT_DS_ID)),medtbl("LOCALCATEGORYCD"))).otherwise(null))
          .withColumn("DEL_FLG", first("DELETE_FLG").over(groups))
          .withColumn("LOCALDOSEUNIT", first("LOCAL_DOSE_UNIT").over(groups))
          .withColumn("LOCALFORM", first("LOCAL_FORM").over(groups))
          .withColumn("LOCALQTYOFDOSEUNIT", first("LOCAL_QTY_OF_DOSE_UNIT").over(groups))
          .withColumn("LOCALROUTE", first("LOCAL_ROUTE").over(groups))
          .withColumn("LOCALSTRENGTHPERDOSEUNIT", first("LOCAL_STRENGTHPERDOSEUNIT").over(groups))
          .withColumn("LOCALSTRENGTHUNIT", first("LOCAL_STRENGTHUNIT").over(groups))
          .filter("DEL_FLG is null")
  }

  map = Map(
    "DATASRC" -> literal("orders"),
    "LOCALROUTE" -> mapFrom("LOCALROUTE", prefix = config(CLIENT_DS_ID) + "."),
    "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("LOCALSTRENGTHPERDOSEUNIT").rlike("^[+-]?\\d+\\.?\\d*$"), df("LOCALSTRENGTHPERDOSEUNIT")).otherwise(null)
        .multiply(when(df("LOCALQTYOFDOSEUNIT").rlike("^[+-]?\\d+\\.?\\d*$"), df("LOCALQTYOFDOSEUNIT")).otherwise(null)))})
  )

}