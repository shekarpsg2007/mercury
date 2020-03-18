package com.humedica.mercury.etl.cerner_v2.rxordersandprescriptions

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 08/09/2018
 */


class RxordersandprescriptionsOrders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  cacheMe = true

  tables = List("orders", "order_action", "order_detail", "zh_code_value",
    "clinicalencounter:cerner_v2.clinicalencounter.ClinicalencounterEncounter",
    "tempmed:cerner_v2.rxordersandprescriptions.RxordersandprescriptionsTempmed",
    "cdr.map_predicate_values", "cdr.map_patient_type", "cdr.map_ordertype")

  columnSelect = Map(
    "orders" -> List("CATALOG_CD", "ORIG_ORD_AS_FLAG", "PERSON_ID", "ORDER_ID", "ENCNTR_ID", "CATALOG_CD",
      "ORDER_DETAIL_DISPLAY_LINE", "UPDT_DT_TM", "ACTIVE_IND", "ACTIVITY_TYPE_CD", "ORIG_ORDER_DT_TM"),
    "order_action" -> List("ORDER_DT_TM", "STOP_TYPE_CD", "ORDER_PROVIDER_ID", "ORDER_STATUS_CD", "ORDER_ID",
      "ACTION_SEQUENCE", "UPDT_DT_TM", "ACTION_TYPE_CD"),
    "order_detail" -> List("ORDER_ID", "ACTION_SEQUENCE", "OE_FIELD_ID", "OE_FIELD_DISPLAY_VALUE", "OE_FIELD_VALUE",
      "UPDT_DT_TM", "OE_FIELD_DT_TM_VALUE"),
    "zh_code_value" -> List("CODE_VALUE", "DESCRIPTION", "DISPLAY"),
    "clinicalencounter" -> List("ENCOUNTERID", "LOCALPATIENTTYPE"),
    "tempmed" -> List("CATALOG_CD", "LOCALNDC", "LOCALGENERICDESC"),
    "cdr.map_patient_type" -> List("GROUPID", "LOCAL_CODE", "CUI"),
    "cdr.map_ordertype" -> List("GROUPID", "LOCALCODE", "CUI")
  )

  beforeJoin = Map(
    "orders" -> ((df: DataFrame) => {
      val list_activity_type = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ORDERS", "RX", "ORDERS", "ACTIVITY_TYPE_CD")
      val zh = table("zh_code_value")
        .filter("code_set = '200'")
      val joined = df.join(zh, df("CATALOG_CD") === zh("CODE_VALUE"), "left_outer")
      val groups = Window.partitionBy(joined("ORDER_ID")).orderBy(joined("UPDT_DT_TM").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups))
        .filter("rn = 1 and active_ind <> '0' and orig_ord_as_flag in ('0','1','4','5') and activity_type_cd in (" + list_activity_type + ")")
        .drop("rn", "UPDT_DT_TM", "CODE_VALUE", "DESCRIPTION")
    }),
    "order_action" -> ((df: DataFrame) => {
      val list_join_action_type = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "JOINCRITERIA", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD")
      val list_delete_order_id = mpvList1(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "DELETE_ORDER_CD", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD")
      val df2 = df.filter("action_type_cd in (" + list_join_action_type + ")")
      val groups = Window.partitionBy(df2("ORDER_ID"), df2("ACTION_SEQUENCE"))
        .orderBy(df2("UPDT_DT_TM").desc_nulls_last)
      val df3 = df2.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn")
      val groups2 = Window.partitionBy(df3("ORDER_ID")).orderBy(df2("ACTION_SEQUENCE").desc_nulls_last)
      df3.withColumn("LATEST_ACTION_SEQUENCE_FLG", row_number.over(groups2))
        .withColumn("DELETE_FLG", when(df2("ACTION_TYPE_CD").isin(list_delete_order_id: _*), lit("Y")).otherwise(null))
        .drop("UPDT_DT_TM")
    }),
    "order_detail" -> ((df: DataFrame) => {
      val zh = table("zh_code_value")
      val joined = df.join(zh, df("OE_FIELD_VALUE") === zh("CODE_VALUE"), "left_outer")
      val groups = Window.partitionBy(joined("ORDER_ID"), joined("ACTION_SEQUENCE"), joined("OE_FIELD_ID"))
        .orderBy(joined("UPDT_DT_TM").desc_nulls_last)
      joined.withColumn("rn", row_number.over(groups))
        .filter("rn = 1")
        .drop("rn", "UPDT_DT_TM", "CODE_VALUE", "DISPLAY")
    }),
    "clinicalencounter" -> ((df: DataFrame) => {
      val mpt = table("cdr.map_patient_type")
        .filter("groupid = '" + config(GROUP) + "'")
      df.join(mpt, df("LOCALPATIENTTYPE") === mpt("LOCAL_CODE"), "left_outer")
        .select("ENCOUNTERID", "CUI")
        .withColumnRenamed("CUI", "CUI_pt")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("orders")
      .join(dfs("order_action"), Seq("ORDER_ID"), "left_outer")
      .join(dfs("order_detail"), Seq("ORDER_ID", "ACTION_SEQUENCE"), "left_outer")
  }

  afterJoin = (df: DataFrame) => {
    val mpv = table("cdr.map_predicate_values")
    val list_no_action_type = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "NEW_ORDER_CD","RXORDER","ORDER_ACTION","ACTION_TYPE_CD")
    val list_io_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "INFUSEOVER","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_iounit_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "INFUSEOVERUNIT","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_rate_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "RATE","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_runit_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "RATEUNIT","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_tv_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "TOTALVOLUME","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_daw_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "DAW","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_freq_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "FREQ","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_vdunit_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "VOLUMEDOSEUNIT","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_dur_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "DURATION","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_durunit_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "DURATIONUNIT","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_df_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "DRUGFORM","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_vd_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "VOLUMEDOSE","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_rxroute_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "RXROUTE","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_sd_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "STRENGTHDOSE","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_sdunit_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "STRENGTHDOSEUNIT","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_nbr_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "NBRREFILLS","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_lds_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "ORDERS", "RXORDER_LOCALDAYSUPPLIED", "ORDER_DETAIL","OE_FIELD_ID")
    val list_qty_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "QTYPERFILL","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_rqstartdt_act_type_cd = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "REQSTART_ACTION_TYPE_CD","RXORDER","ORDERS","ACTION_TYPE_CD")
    val list_rqstartdt_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "REQSTARTDTTM","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_stop_act_type_cd = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "STOP_ACTION_TYPE_CD","RXORDER","ORDERS","ACTION_TYPE_CD")
    val list_stopdt_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "STOPDTTM","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_action_type_cd = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "ACTION_TYPE_CD","RXORDER","ORDERS","ACTION_TYPE_CD")
    val list_cancel_action_type_cd = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "CANCEL","RXORDER","ORDERS","ACTION_TYPE_CD")
    val list_cr_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "CANCELREASON","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_discont_act_type_cd = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "DISCONTINUE","RXORDER","ORDERS","ACTION_TYPE_CD")
    val list_dcreason_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "DCREASON","RXORDER","ORDER_DETAIL","OE_FIELD_ID")
    val list_suspend_action_type_cd = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "SUSPEND","RXORDER","ORDERS","ACTION_TYPE_CD")
    val list_susreason_oe_field_id = mpvList1(mpv, config(GROUP), config(CLIENT_DS_ID), "SUSPENDREASON","RXORDER","ORDER_DETAIL","OE_FIELD_ID")

    val df2 = df.withColumnRenamed("ORDER_ID", "RXID")
      .withColumnRenamed("PERSON_ID", "PATIENTID")
      .withColumnRenamed("ENCNTR_ID", "ENCOUNTERID")
      .withColumn("LOCALMEDCODE", when(df("CATALOG_CD") === lit("0"), null).otherwise(df("CATALOG_CD")))
      .withColumnRenamed("ORDER_DETAIL_DISPLAY_LINE", "SIGNATURE")
      .withColumn("ORDERVSPRESCRIPTION",
        when(expr("orig_ord_as_flag in ('0','4')"), lit("O"))
        .when(expr("orig_ord_as_flag in ('1','5')"), lit("P"))
        .otherwise(null))
      .withColumnRenamed("DISPLAY", "LOCALDESCRIPTION")
      .groupBy("RXID", "PATIENTID", "ENCOUNTERID", "LOCALMEDCODE", "SIGNATURE", "ORDERVSPRESCRIPTION", "LOCALDESCRIPTION",
        "ORIG_ORD_AS_FLAG", "ORIG_ORDER_DT_TM")
      .agg(
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*), df("ORDER_DT_TM")).otherwise(null)).as("ORDER_DT"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("ORDER_PROVIDER_ID") =!= lit("0"), df("ORDER_PROVIDER_ID")).otherwise(null)).as("LOCALPROVIDERID"),
        max(when(df("LATEST_ACTION_SEQUENCE_FLG") === lit("1"), concat_ws(".", lit(config(CLIENT_DS_ID)), df("ORDER_STATUS_CD"))).otherwise(null)).as("ORDERSTATUS"),
        max(df("DELETE_FLG")).as("DELETE_FLG"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_io_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("INFUSEOVER"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_iounit_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("INFUSEOVERUNIT"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_rate_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("RATE"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_runit_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("RATEUNIT"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_tv_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("LOCALINFUSIONVOLUME"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_daw_oe_field_id: _*), concat_ws(".", lit(config(CLIENT_DS_ID)), df("OE_FIELD_DISPLAY_VALUE"))).otherwise(null)).as("LOCALDAW"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_freq_oe_field_id: _*), df("DESCRIPTION")).otherwise(null)).as("LOCALDOSEFREQ"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_vdunit_oe_field_id: _*), df("DESCRIPTION")).otherwise(null)).as("LOCALDOSEUNIT"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_dur_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("DURATION"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_durunit_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("DURATIONUNIT"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_df_oe_field_id: _*), df("DESCRIPTION")).otherwise(null)).as("LOCALFORM"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_vd_oe_field_id: _*), translate(df("OE_FIELD_DISPLAY_VALUE"), ",", "")).otherwise(null)).as("LOCALQTYOFDOSEUNIT"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_rxroute_oe_field_id: _*), concat_ws(".", lit(config(CLIENT_DS_ID)), df("OE_FIELD_VALUE"))).otherwise(null)).as("LOCALROUTE"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_sd_oe_field_id: _*), translate(df("OE_FIELD_DISPLAY_VALUE"), ",", "")).otherwise(null)).as("LOCALSTRENGTHPERDOSEUNIT"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_sdunit_oe_field_id: _*), df("DESCRIPTION")).otherwise(null)).as("LOCALSTRENGTHUNIT"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_nbr_oe_field_id: _*) && df("OE_FIELD_DISPLAY_VALUE").rlike("^[0-9]{1,4}$"), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("FILLNUM"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_lds_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("LOCALDAYSUPPLIED"),
        max(when(df("ACTION_TYPE_CD").isin(list_no_action_type: _*) && df("OE_FIELD_ID").isin(list_qty_oe_field_id: _*), df("OE_FIELD_DISPLAY_VALUE")).otherwise(null)).as("QUANTITYPERFILL"),
        max(when(df("LATEST_ACTION_SEQUENCE_FLG") === lit("1"),
          when(df("ACTION_TYPE_CD").isin(list_action_type_cd: _*) && df("STOP_TYPE_CD") =!= lit("0"), concat_ws(".", lit(config(CLIENT_DS_ID)), df("STOP_TYPE_CD")))
            .when(df("ACTION_TYPE_CD").isin(list_cancel_action_type_cd: _*) && df("OE_FIELD_ID").isin(list_cr_oe_field_id: _*), concat_ws(".", lit(config(CLIENT_DS_ID)), df("DESCRIPTION")))
            .when(df("ACTION_TYPE_CD").isin(list_discont_act_type_cd: _*) && df("OE_FIELD_ID").isin(list_dcreason_oe_field_id: _*), concat_ws(".", lit(config(CLIENT_DS_ID)), df("DESCRIPTION")))
            .when(df("ACTION_TYPE_CD").isin(list_suspend_action_type_cd: _*) && df("OE_FIELD_ID").isin(list_susreason_oe_field_id: _*), concat_ws(".", lit(config(CLIENT_DS_ID)), df("DESCRIPTION")))
            .otherwise(null)))
          .as("DISCONTINUEREASON"),
        max(when(df("LATEST_ACTION_SEQUENCE_FLG") === lit("1"),
          when(df("ACTION_TYPE_CD").isin(list_rqstartdt_act_type_cd: _*) && df("OE_FIELD_ID").isin(list_rqstartdt_oe_field_id: _*), df("OE_FIELD_DT_TM_VALUE"))
          .when(df("ACTION_TYPE_CD").isin(list_stop_act_type_cd: _*) && df("OE_FIELD_ID").isin(list_stopdt_oe_field_id: _*), df("OE_FIELD_DT_TM_VALUE"))
          .otherwise(null)))
          .as("DISCONTINUEDATE")
      )

    val mo = table("cdr.map_ordertype")
      .filter("groupid = '" + config(GROUP) + "'")
      .withColumnRenamed("CUI", "CUI_mo")
      .drop("GROUPID")
    val tempmeds = table("tempmed")
    val ce = table("clinicalencounter")
    df2.join(mo, df2("ORIG_ORD_AS_FLAG") === mo("LOCALCODE"), "left_outer")
      .join(tempmeds, df2("LOCALMEDCODE") === tempmeds("CATALOG_CD"), "left_outer")
      .join(ce, Seq("ENCOUNTERID"), "inner")
      .withColumn("ISSUEDATE", coalesce(df2("ORDER_DT"), df2("ORIG_ORDER_DT_TM")))
      .filter("delete_flg is null and localmedcode is not null")
      .drop("LOCALCODE")
  }

  map = Map(
    "DATASRC" -> literal("orders"),
    "LOCALINFUSIONDURATION" -> concatFrom(Seq("INFUSEOVER", "INFUSEOVERUNIT"), delim = " "),
    "LOCALINFUSIONRATE" -> concatFrom(Seq("RATE", "RATEUNIT"), delim = " "),
    "LOCALDURATION" -> concatFrom(Seq("DURATION", "DURATIONUNIT"), delim = " "),
    "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, bround(df("LOCALSTRENGTHPERDOSEUNIT") * df("LOCALQTYOFDOSEUNIT"), 2))
    }),
    "VENUE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ORIG_ORD_AS_FLAG") === lit("1") ||
          df("CUI_pt").isin("CH000110", "CH000113", "CH000924", "CH000935", "CH000936", "CH000937")
        ,lit("1"))
        .otherwise(lit("0")))
    }),
    "ORDERTYPE" -> mapFrom("CUI_mo")
  )

}