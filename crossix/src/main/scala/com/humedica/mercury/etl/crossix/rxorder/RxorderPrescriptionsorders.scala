package com.humedica.mercury.etl.crossix.rxorder


import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderPrescriptionsorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("GROUPID", "DATASRC", "FACILITYID", "RXID",
        "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID",
        "ISSUEDATE", "DISCONTINUEDATE", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE",
        "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE",
        "ORDERSTATUS", "ALTMEDCODE", "MAPPEDNDC", "MAPPEDGPI", "MAPPEDNDC_CONF",
        "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED",
        "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ", "LOCALDURATION",
        "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11",
        "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",  "LOCALDAW",
        "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID",
        "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON",
        "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE",
        "ACTIVE_MED_FLAG", "ROW_SOURCE", "MODIFIED_DATE")

    tables = List("orders", "order_action", "order_detail", "zh_code_value", "zh_med_identifier",
        "cdr.map_predicate_values", "cdr.map_patient_type", "temptable:cr2clinicalencounter", "cdr.map_ordertype",
        "cdr.zh_order_catalog_item_r")

    columnSelect = Map(
        "orders" -> List("FILE_ID", "ORDER_DETAIL_DISPLAY_LINE", "ENCNTR_ID", "PERSON_ID", "ACTIVE_IND", "CATALOG_CD", "ACTIVITY_TYPE_CD", "ORDER_ID", "UPDT_DT_TM", "ORIG_ORD_AS_FLAG", "ORIG_ORDER_DT_TM"),
        "order_action" -> List("FILE_ID",   "ACTION_TYPE_CD", "OA_ORDER_ID", "ACTION_SEQUENCE", "UPDT_DT_TM", "ORDER_DT_TM", "ORDER_PROVIDER_ID", "ORDER_STATUS_CD", "STOP_TYPE_CD"),
        "order_detail" -> List("FILE_ID",  "OE_FIELD_VALUE", "OD_ORDER_ID", "ACTION_SEQUENCE", "OE_FIELD_ID", "UPDT_DT_TM", "OE_FIELD_DISPLAY_VALUE", "OE_FIELD_DT_TM_VALUE"),
        "zh_code_value" -> List("FILE_ID",   "ACTION_TYPE_CD", "OA_ORDER_ID", "ACTION_SEQUENCE", "UPDT_DT_TM", "ORDER_DT_TM", "ORDER_PROVIDER_ID", "ORDER_STATUS_CD", "STOP_TYPE_CD"),
        "zh_med_identifier" -> List("FILE_ID",  "VALUE_KEY", "ITEM_ID", "MED_IDENTIFIER_TYPE_CD", "ACTIVE_IND", "VALUE"),
        "cdr.map_predicate_values" -> List("DATA_SRC",   "ENTITY", "TABLE_NAME", "COLUMN_NAME", "GROUPID", "CLIENT_DS_ID", "DTS_VERSION"),
        "cdr.map_patient_type" -> List("GROUPID", "LOCAL_CODE", "CUI")
    )




    def safe_to_number(value:Column)={
        try {
            value.cast("Integer")
            value
        }
        catch {
            case ex:Exception => null
        }
    }


    def predicate_value_listlike(p_mpv: DataFrame, dataSrc: String, entity: String, table: String, column: String, colName: String): DataFrame = {
        var mpv1 = p_mpv.filter(p_mpv("DATA_SRC").equalTo(dataSrc).and(p_mpv("ENTITY").rlike(entity)).and(p_mpv("TABLE_NAME").equalTo(table)).and(p_mpv("COLUMN_NAME").equalTo(column)))
        mpv1=mpv1.withColumn(colName, mpv1("COLUMN_VALUE"))
        mpv1.select("GROUPID", "CLIENT_DS_ID", colName).distinct()
    }

    def safe_to_number_with_default(value:Column, default:Object)={
        when(safe_to_number(value).isNull, lit(default)).otherwise(value)
    }


    def predicate_value_list(p_mpv: DataFrame, dataSrc: String, entity: String, table: String, column: String, colName: String): DataFrame = {
        var mpv1 = p_mpv.filter(p_mpv("DATA_SRC").equalTo(dataSrc).and(p_mpv("ENTITY").equalTo(entity)).and(p_mpv("TABLE_NAME").equalTo(table)).and(p_mpv("COLUMN_NAME").equalTo(column)))
        mpv1=mpv1.withColumn(colName, mpv1("COLUMN_VALUE"))
        mpv1.select("GROUPID", "CLIENT_DS_ID", colName).orderBy(mpv1("DTS_VERSION").desc).distinct()
    }

    join = (dfs: Map[String, DataFrame])  => {

        var orders = dfs("orders")
        var zh_code_value = dfs("zh_code_value")
        var mpv = dfs("cdr.map_predicate_values")
        var OJ1 = orders.join(zh_code_value, orders("GROUPID_oms") === zh_code_value("GROUPID_zms") && orders("CLIENT_DS_ID_oms") === zh_code_value("CLIENT_DS_ID_zms")  && orders("CATALOG_CD") === zh_code_value("CODE_VALUE")  && lit("200") === zh_code_value("CODE_SET"), "left_outer").drop("CLIENT_DS_ID_zms").drop("GROUPID_zms")
        var LIST_ACTIVITY_TYPE_CD = predicate_value_list(mpv, "ORDERS", "RX", "ORDERS", "ACTIVITY_TYPE_CD", "ACTIVITY_TYPE_CD_VAL")

        var OJ2 = OJ1.join(LIST_ACTIVITY_TYPE_CD, OJ1("ACTIVITY_TYPE_CD") === LIST_ACTIVITY_TYPE_CD("ACTIVITY_TYPE_CD_VAL") && OJ1("GROUPID_oms") === LIST_ACTIVITY_TYPE_CD("GROUPID") && OJ1("CLIENT_DS_ID_oms") === LIST_ACTIVITY_TYPE_CD("CLIENT_DS_ID"), "inner").drop("CLIENT_DS_ID").drop("GROUPID")
        val group1 = Window.partitionBy(OJ2("ORDER_ID"), OJ2("GROUPID_oms"), OJ2("CLIENT_DS_ID_oms")).orderBy(OJ2("UPDT_DT_TM").desc)
        var OJ3 = OJ2.withColumn("ROW_NUMBER", row_number().over(group1)) //dedupe orders
        OJ3 = OJ3.filter("ROW_NUMBER == 1")
        var OJ4 = OJ3.filter(OJ3("ACTIVITY_TYPE_CD_VAL").isNotNull.and((OJ3("ORIG_ORD_AS_FLAG") === "0" || OJ3("ORIG_ORD_AS_FLAG") === "1" || OJ3("ORIG_ORD_AS_FLAG") === "4" || OJ3("ORIG_ORD_AS_FLAG") === "5")))
        var OA = dfs("order_detail")

        var LIST_JOIN_ACTION_TYPE_CD = predicate_value_list(mpv, "JOINCRITERIA", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD", "LIST_JOIN_ACTION_TYPE_CD_VAL")
        var LIST_DELETE_ORDER_ID = predicate_value_list(mpv, "DELETE_ORDER_CD", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD", "DELETE_ORDER_CD_VAL")

        var OA1 = OA.join(LIST_JOIN_ACTION_TYPE_CD, OA("CLIENT_DS_ID_oa") === LIST_JOIN_ACTION_TYPE_CD("CLIENT_DS_ID") && OA("GROUPID_oa") === LIST_JOIN_ACTION_TYPE_CD("GROUPID") && OA("ACTION_TYPE_CD") === LIST_JOIN_ACTION_TYPE_CD("LIST_JOIN_ACTION_TYPE_CD_VAL") && OA("ACTION_TYPE_CD") === LIST_JOIN_ACTION_TYPE_CD("LIST_JOIN_ACTION_TYPE_CD_VAL") && OA("ACTION_TYPE_CD") === LIST_JOIN_ACTION_TYPE_CD("LIST_JOIN_ACTION_TYPE_CD_VAL"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OA2 = OA1.join(LIST_DELETE_ORDER_ID, OA("CLIENT_DS_ID_oa") === LIST_JOIN_ACTION_TYPE_CD("CLIENT_DS_ID") && OA("GROUPID_oa") === LIST_JOIN_ACTION_TYPE_CD("GROUPID") && OA("ACTION_TYPE_CD") === LIST_DELETE_ORDER_ID("DELETE_ORDER_CD_VAL"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        val group2 = Window.partitionBy(OA2("OA_ORDER_ID"), OA2("ACTION_SEQUENCE"),OA2("GROUPID_oa"), OA2("CLIENT_DS_ID_oa")).orderBy(OA2("UPDT_DT_TM").desc)
        var OA3 = OA2.withColumn("ROW_NUMBER1", row_number().over(group2))
        OA3 = OA3.filter(OA3("ROW_NUMBER1")===1)
        var OA4 = OA3.filter(OA3("LIST_JOIN_ACTION_TYPE_CD_VAL").isNotNull)

        val group3 = Window.partitionBy(OA4("OA_ORDER_ID"), OA4("GROUPID_oa"), OA4("CLIENT_DS_ID_oa")).orderBy(OA4("ACTION_SEQUENCE").desc)
        var OA5 = OA4.withColumn("LATEST_ACTION_SEQUENCE_FLG", row_number().over(group3))
        // var OA52 = OA5.filter(OA5("LATEST_ACTION_SEQUENCE_FLG") === 1)
        var OA7 = OA5.withColumn("DELETE_FLG", when(OA5("DELETE_ORDER_CD_VAL").isNotNull, "Y").otherwise("N"))
        var OOAJ1 = OJ4.join(OA7, OJ4("ORDER_ID")===OA7("OA_ORDER_ID") &&  OJ4("GROUPID_oms")===OA7("GROUPID_oa") &&  OJ4("CLIENT_DS_ID_oms")===OA7("CLIENT_DS_ID_oa"), "left_outer").drop("CLIENT_DS_ID_oa").drop("GROUPID_oa")

        var OD = dfs("order_detail")
        OD=OD.withColumnRenamed("ORDER_ID","OD_ORDER_ID").select("FILE_ID",  "OE_FIELD_VALUE", "OD_ORDER_ID", "ACTION_SEQUENCE", "OE_FIELD_ID", "UPDT_DT_TM", "OE_FIELD_DISPLAY_VALUE", "OE_FIELD_DT_TM_VALUE")


        var ODZ = OD.join(zh_code_value, OD("OE_FIELD_VALUE") === zh_code_value("CODE_VALUE") &&  OD("CLIENT_DS_ID_od") === zh_code_value("CLIENT_DS_ID_zfv") &&  OD("GROUPID_od") === zh_code_value("GROUPID_zfv"), "left_outer").drop("CLIENT_DS_ID_zfv").drop("GROUPID_zfv")
        val group4 = Window.partitionBy(ODZ("OD_ORDER_ID"), ODZ("ACTION_SEQUENCE"), ODZ("OE_FIELD_ID"), ODZ("GROUPID_od"), ODZ("CLIENT_DS_ID_od")).orderBy(ODZ("UPDT_DT_TM").desc)
        var ODZ1 = ODZ.withColumn("RESULT", row_number().over(group4))
        ODZ1 = ODZ1.filter(ODZ1("RESULT") === 1)

        var OOAJ21 = OOAJ1.join(ODZ1, OOAJ1("ORDER_ID") === ODZ1("OD_ORDER_ID") && OOAJ1("ACTION_SEQUENCE") === ODZ1("ACTION_SEQUENCE") && lit(1) === ODZ1("RESULT") && OOAJ1("GROUPID_oms") === ODZ1("GROUPID_od")  && OOAJ1("CLIENT_DS_ID_oms") === ODZ1("CLIENT_DS_ID_od") , "left_outer").drop("CLIENT_DS_ID_od").drop("GROUPID_od")
        var OOAJ2 =OOAJ21.filter("ACTIVE_IND != '0'")


        var LIST_NO_ACTION_TYPE_CD = predicate_value_list(mpv, "NEW_ORDER_CD", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD", "LIST_NO_ACTION_TYPE_CD_VAL");
        var LIST_IO_OE_FIELD_ID = predicate_value_list(mpv, "INFUSEOVER", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_IO_OE_FIELD_ID_VAL");
        var LIST_IOUNIT_OE_FIELD_ID = predicate_value_list(mpv, "INFUSEOVERUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_IOUNIT_OE_FIELD_ID_VAL");
        var LIST_RATE_OE_FIELD_ID = predicate_value_list(mpv, "RATE", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_RATE_OE_FIELD_ID_VAL");
        var LIST_RUNIT_OE_FIELD_ID = predicate_value_list(mpv, "RATEUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_RUNIT_OE_FIELD_ID_VAL");
        var LIST_TV_OE_FIELD_ID = predicate_value_list(mpv, "TOTALVOLUME", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_TV_OE_FIELD_ID_VAL");
        var LIST_DAW_OE_FIELD_ID = predicate_value_list(mpv, "DAW", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DAW_OE_FIELD_ID_VAL");
        var LIST_FREQ_OE_FIELD_ID = predicate_value_list(mpv, "FREQ", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_FREQ_OE_FIELD_ID_VAL");
        var LIST_VDUNIT_OE_FIELD_ID = predicate_value_list(mpv, "VOLUMEDOSEUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_VDUNIT_OE_FIELD_ID_VAL");
        var LIST_DUR_OE_FIELD_ID = predicate_value_list(mpv, "DURATION", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DUR_OE_FIELD_ID_VAL");
        var LIST_DURUNIT_OE_FIELD_ID = predicate_value_list(mpv, "DURATIONUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DURUNIT_OE_FIELD_ID_VAL");
        var LIST_DF_OE_FIELD_ID = predicate_value_list(mpv, "DRUGFORM", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DF_OE_FIELD_ID_VAL");
        var LIST_VD_OE_FIELD_ID = predicate_value_list(mpv, "VOLUMEDOSE", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_VD_OE_FIELD_ID_VAL");
        var LIST_RXROUTE_OE_FIELD_ID = predicate_value_list(mpv, "RXROUTE", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_RXROUTE_OE_FIELD_ID_VAL");
        var LIST_SD_OE_FIELD_ID = predicate_value_list(mpv, "STRENGTHDOSE", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_SD_OE_FIELD_ID_VAL");
        var LIST_SDUNIT_OE_FIELD_ID = predicate_value_list(mpv, "STRENGTHDOSEUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_SDUNIT_OE_FIELD_ID_VAL");
        var LIST_LDS_OE_FIELD_ID = predicate_value_list(mpv, "ORDERS", "RXORDER_LOCALDAYSUPPLIED", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_LDS_OE_FIELD_ID_VAL");
        var LIST_QTY_OE_FIELD_ID = predicate_value_list(mpv, "QTYPERFILL", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_QTY_OE_FIELD_ID_VAL");
        var LIST_NBR_OE_FIELD_ID = predicate_value_list(mpv, "NBRREFILLS", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_NBR_OE_FIELD_ID_VAL");
        var LIST_RQSTARTDT_ACT_TYPE_CD = predicate_value_list(mpv, "REQSTART_ACTION_TYPE_CD", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_RQSTARTDT_ACT_TYPE_CD_VAL");
        var LIST_RQSTARTDT_OE_FIELD_ID = predicate_value_list(mpv, "REQSTARTDTTM", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_RQSTARTDT_OE_FIELD_ID_VAL");
        var LIST_STOP_ACT_TYPE_CD = predicate_value_list(mpv, "STOP_ACTION_TYPE_CD", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_STOP_ACT_TYPE_CD_VAL");
        var LIST_STOPDT_OE_FIELD_ID = predicate_value_list(mpv, "STOPDTTM", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_STOPDT_OE_FIELD_ID_VAL");
        var LIST_ACTION_TYPE_CD = predicate_value_list(mpv, "ACTION_TYPE_CD", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_ACTION_TYPE_CD_VAL");
        var LIST_CANCEL_ACTION_TYPE_CD = predicate_value_list(mpv, "CANCEL", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_CANCEL_ACTION_TYPE_CD_VAL");
        var LIST_CR_OE_FIELD_ID = predicate_value_list(mpv, "CANCELREASON", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_CR_OE_FIELD_ID_VAL");
        var LIST_DISCONT_ACT_TYPE_CD = predicate_value_list(mpv, "DISCONTINUE", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_DISCONT_ACT_TYPE_CD_VAL");
        var LIST_DCREASON_OE_FIELD_ID = predicate_value_list(mpv, "DCREASON", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DCREASON_OE_FIELD_ID_VAL");
        var LIST_SUSPEND_ACTION_TYPE_CD = predicate_value_list(mpv, "SUSPEND", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_SUSPEND_ACTION_TYPE_CD_VAL");
        var LIST_SUSREASON_OE_FIELD_ID = predicate_value_list(mpv, "SUSPENDREASON", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_SUSREASON_OE_FIELD_ID_VAL");

        var OOAJ22 = OOAJ2.join(LIST_NO_ACTION_TYPE_CD, OOAJ2("ACTION_TYPE_CD") === LIST_NO_ACTION_TYPE_CD("LIST_NO_ACTION_TYPE_CD_VAL") && OOAJ2("GROUPID_oms") === LIST_NO_ACTION_TYPE_CD("GROUPID") && OOAJ2("CLIENT_DS_ID_oms") === LIST_NO_ACTION_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")

        var OOAJ23 = OOAJ22.join(LIST_IO_OE_FIELD_ID, OOAJ22("OE_FIELD_ID") === LIST_IO_OE_FIELD_ID("LIST_IO_OE_FIELD_ID_VAL") && OOAJ22("GROUPID_oms") === LIST_IO_OE_FIELD_ID("GROUPID") && OOAJ22("CLIENT_DS_ID_oms") === LIST_IO_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ24 = OOAJ23.join(LIST_IOUNIT_OE_FIELD_ID, OOAJ23("OE_FIELD_ID") === LIST_IOUNIT_OE_FIELD_ID("LIST_IOUNIT_OE_FIELD_ID_VAL") && OOAJ23("GROUPID_oms") === LIST_IOUNIT_OE_FIELD_ID("GROUPID") && OOAJ23("CLIENT_DS_ID_oms") === LIST_IOUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ25 = OOAJ24.join(LIST_RATE_OE_FIELD_ID, OOAJ24("OE_FIELD_ID") === LIST_RATE_OE_FIELD_ID("LIST_RATE_OE_FIELD_ID_VAL") && OOAJ24("GROUPID_oms") === LIST_RATE_OE_FIELD_ID("GROUPID") && OOAJ24("CLIENT_DS_ID_oms") === LIST_RATE_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ26 = OOAJ25.join(LIST_RUNIT_OE_FIELD_ID, OOAJ25("OE_FIELD_ID") === LIST_RUNIT_OE_FIELD_ID("LIST_RUNIT_OE_FIELD_ID_VAL") && OOAJ25("GROUPID_oms") === LIST_RUNIT_OE_FIELD_ID("GROUPID") && OOAJ25("CLIENT_DS_ID_oms") === LIST_RUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ27 = OOAJ26.join(LIST_TV_OE_FIELD_ID, OOAJ26("OE_FIELD_ID") === LIST_TV_OE_FIELD_ID("LIST_TV_OE_FIELD_ID_VAL") && OOAJ26("GROUPID_oms") === LIST_TV_OE_FIELD_ID("GROUPID") && OOAJ26("CLIENT_DS_ID_oms") === LIST_TV_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ28 = OOAJ27.join(LIST_DAW_OE_FIELD_ID, OOAJ27("OE_FIELD_ID") === LIST_DAW_OE_FIELD_ID("LIST_DAW_OE_FIELD_ID_VAL") && OOAJ27("GROUPID_oms") === LIST_DAW_OE_FIELD_ID("GROUPID") && OOAJ27("CLIENT_DS_ID_oms") === LIST_DAW_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ29 = OOAJ28.join(LIST_FREQ_OE_FIELD_ID, OOAJ28("OE_FIELD_ID") === LIST_FREQ_OE_FIELD_ID("LIST_FREQ_OE_FIELD_ID_VAL") && OOAJ28("GROUPID_oms") === LIST_FREQ_OE_FIELD_ID("GROUPID") && OOAJ28("CLIENT_DS_ID_oms") === LIST_FREQ_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ30 = OOAJ29.join(LIST_VDUNIT_OE_FIELD_ID, OOAJ29("OE_FIELD_ID") === LIST_VDUNIT_OE_FIELD_ID("LIST_VDUNIT_OE_FIELD_ID_VAL") && OOAJ29("GROUPID_oms") === LIST_VDUNIT_OE_FIELD_ID("GROUPID") && OOAJ29("CLIENT_DS_ID_oms") === LIST_VDUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ31 = OOAJ30.join(LIST_DUR_OE_FIELD_ID, OOAJ30("OE_FIELD_ID") === LIST_DUR_OE_FIELD_ID("LIST_DUR_OE_FIELD_ID_VAL") && OOAJ30("GROUPID_oms") === LIST_DUR_OE_FIELD_ID("GROUPID") && OOAJ30("CLIENT_DS_ID_oms") === LIST_DUR_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ32 = OOAJ31.join(LIST_DURUNIT_OE_FIELD_ID, OOAJ31("OE_FIELD_ID") === LIST_DURUNIT_OE_FIELD_ID("LIST_DURUNIT_OE_FIELD_ID_VAL") && OOAJ31("GROUPID_oms") === LIST_DURUNIT_OE_FIELD_ID("GROUPID") && OOAJ31("CLIENT_DS_ID_oms") === LIST_DURUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ33 = OOAJ32.join(LIST_DF_OE_FIELD_ID, OOAJ32("OE_FIELD_ID") === LIST_DF_OE_FIELD_ID("LIST_DF_OE_FIELD_ID_VAL") && OOAJ32("GROUPID_oms") === LIST_DF_OE_FIELD_ID("GROUPID") && OOAJ32("CLIENT_DS_ID_oms") === LIST_DF_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ34 = OOAJ33.join(LIST_VD_OE_FIELD_ID, OOAJ33("OE_FIELD_ID") === LIST_VD_OE_FIELD_ID("LIST_VD_OE_FIELD_ID_VAL") && OOAJ33("GROUPID_oms") === LIST_VD_OE_FIELD_ID("GROUPID") && OOAJ33("CLIENT_DS_ID_oms") === LIST_VD_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ35 = OOAJ34.join(LIST_RXROUTE_OE_FIELD_ID, OOAJ34("OE_FIELD_ID") === LIST_RXROUTE_OE_FIELD_ID("LIST_RXROUTE_OE_FIELD_ID_VAL") && OOAJ34("GROUPID_oms") === LIST_RXROUTE_OE_FIELD_ID("GROUPID") && OOAJ34("CLIENT_DS_ID_oms") === LIST_RXROUTE_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ36 = OOAJ35.join(LIST_SD_OE_FIELD_ID, OOAJ35("OE_FIELD_ID") === LIST_SD_OE_FIELD_ID("LIST_SD_OE_FIELD_ID_VAL") && OOAJ35("GROUPID_oms") === LIST_SD_OE_FIELD_ID("GROUPID") && OOAJ35("CLIENT_DS_ID_oms") === LIST_SD_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ37 = OOAJ36.join(LIST_SDUNIT_OE_FIELD_ID, OOAJ36("OE_FIELD_ID") === LIST_SDUNIT_OE_FIELD_ID("LIST_SDUNIT_OE_FIELD_ID_VAL") && OOAJ36("GROUPID_oms") === LIST_SDUNIT_OE_FIELD_ID("GROUPID") && OOAJ36("CLIENT_DS_ID_oms") === LIST_SDUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ38 = OOAJ37.join(LIST_NBR_OE_FIELD_ID, OOAJ37("OE_FIELD_ID") === LIST_NBR_OE_FIELD_ID("LIST_NBR_OE_FIELD_ID_VAL") && OOAJ37("GROUPID_oms") === LIST_NBR_OE_FIELD_ID("GROUPID") && OOAJ37("CLIENT_DS_ID_oms") === LIST_NBR_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ39 = OOAJ38.join(LIST_LDS_OE_FIELD_ID, OOAJ38("OE_FIELD_ID") === LIST_LDS_OE_FIELD_ID("LIST_LDS_OE_FIELD_ID_VAL") && OOAJ38("GROUPID_oms") === LIST_LDS_OE_FIELD_ID("GROUPID") && OOAJ38("CLIENT_DS_ID_oms") === LIST_LDS_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ40 = OOAJ39.join(LIST_QTY_OE_FIELD_ID, OOAJ39("OE_FIELD_ID") === LIST_QTY_OE_FIELD_ID("LIST_QTY_OE_FIELD_ID_VAL") && OOAJ39("GROUPID_oms") === LIST_QTY_OE_FIELD_ID("GROUPID") && OOAJ39("CLIENT_DS_ID_oms") === LIST_QTY_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ41 = OOAJ40.join(LIST_RQSTARTDT_ACT_TYPE_CD, OOAJ40("ACTION_TYPE_CD") === LIST_RQSTARTDT_ACT_TYPE_CD("LIST_RQSTARTDT_ACT_TYPE_CD_VAL") && OOAJ40("GROUPID_oms") === LIST_RQSTARTDT_ACT_TYPE_CD("GROUPID") && OOAJ40("CLIENT_DS_ID_oms") === LIST_RQSTARTDT_ACT_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ42 = OOAJ41.join(LIST_RQSTARTDT_OE_FIELD_ID, OOAJ41("OE_FIELD_ID") === LIST_RQSTARTDT_OE_FIELD_ID("LIST_RQSTARTDT_OE_FIELD_ID_VAL") && OOAJ41("GROUPID_oms") === LIST_RQSTARTDT_OE_FIELD_ID("GROUPID") && OOAJ41("CLIENT_DS_ID_oms") === LIST_RQSTARTDT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ43 = OOAJ42.join(LIST_STOP_ACT_TYPE_CD, OOAJ42("ACTION_TYPE_CD") === LIST_STOP_ACT_TYPE_CD("LIST_STOP_ACT_TYPE_CD_VAL") && OOAJ42("GROUPID_oms") === LIST_STOP_ACT_TYPE_CD("GROUPID") && OOAJ42("CLIENT_DS_ID_oms") === LIST_STOP_ACT_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ44 = OOAJ43.join(LIST_STOPDT_OE_FIELD_ID, OOAJ43("OE_FIELD_ID") === LIST_STOPDT_OE_FIELD_ID("LIST_STOPDT_OE_FIELD_ID_VAL") && OOAJ43("GROUPID_oms") === LIST_STOPDT_OE_FIELD_ID("GROUPID") && OOAJ43("CLIENT_DS_ID_oms") === LIST_STOPDT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ45 = OOAJ44.join(LIST_ACTION_TYPE_CD, OOAJ44("ACTION_TYPE_CD") === LIST_ACTION_TYPE_CD("LIST_ACTION_TYPE_CD_VAL") && OOAJ44("GROUPID_oms") === LIST_ACTION_TYPE_CD("GROUPID") && OOAJ44("CLIENT_DS_ID_oms") === LIST_ACTION_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ46 = OOAJ45.join(LIST_CANCEL_ACTION_TYPE_CD, OOAJ45("ACTION_TYPE_CD") === LIST_CANCEL_ACTION_TYPE_CD("LIST_CANCEL_ACTION_TYPE_CD_VAL") && OOAJ45("GROUPID_oms") === LIST_CANCEL_ACTION_TYPE_CD("GROUPID") && OOAJ45("CLIENT_DS_ID_oms") === LIST_CANCEL_ACTION_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ47 = OOAJ46.join(LIST_CR_OE_FIELD_ID, OOAJ46("OE_FIELD_ID") === LIST_CR_OE_FIELD_ID("LIST_CR_OE_FIELD_ID_VAL") && OOAJ46("GROUPID_oms") === LIST_CR_OE_FIELD_ID("GROUPID") && OOAJ46("CLIENT_DS_ID_oms") === LIST_CR_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ48 = OOAJ47.join(LIST_DISCONT_ACT_TYPE_CD, OOAJ47("ACTION_TYPE_CD") === LIST_DISCONT_ACT_TYPE_CD("LIST_DISCONT_ACT_TYPE_CD_VAL") && OOAJ47("GROUPID_oms") === LIST_DISCONT_ACT_TYPE_CD("GROUPID") && OOAJ47("CLIENT_DS_ID_oms") === LIST_DISCONT_ACT_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ49 = OOAJ48.join(LIST_DCREASON_OE_FIELD_ID, OOAJ48("OE_FIELD_ID") === LIST_DCREASON_OE_FIELD_ID("LIST_DCREASON_OE_FIELD_ID_VAL") && OOAJ48("GROUPID_oms") === LIST_DCREASON_OE_FIELD_ID("GROUPID") && OOAJ48("CLIENT_DS_ID_oms") === LIST_DCREASON_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ50 = OOAJ49.join(LIST_SUSPEND_ACTION_TYPE_CD, OOAJ49("ACTION_TYPE_CD") === LIST_SUSPEND_ACTION_TYPE_CD("LIST_SUSPEND_ACTION_TYPE_CD_VAL") && OOAJ49("GROUPID_oms") === LIST_SUSPEND_ACTION_TYPE_CD("GROUPID") && OOAJ49("CLIENT_DS_ID_oms") === LIST_SUSPEND_ACTION_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
        var OOAJ51 = OOAJ50.join(LIST_SUSREASON_OE_FIELD_ID, OOAJ50("OE_FIELD_ID") === LIST_SUSREASON_OE_FIELD_ID("LIST_SUSREASON_OE_FIELD_ID_VAL") && OOAJ50("GROUPID_oms") === LIST_SUSREASON_OE_FIELD_ID("GROUPID") && OOAJ50("CLIENT_DS_ID_oms") === LIST_SUSREASON_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")

        var OOAJ52=OOAJ51.withColumn("LOCALMEDCODE", when(OOAJ51("CATALOG_CD").isNull, "0").otherwise(OOAJ51("CATALOG_CD")))
        OOAJ52=OOAJ52.withColumnRenamed("ORDER_DETAIL_DISPLAY_LINE", "SIGNATURE")
        OOAJ52=OOAJ52.withColumn("ORDERVSPRESCRIPTION", when(OOAJ52("ORIG_ORD_AS_FLAG") === "0", "O")
                .when(OOAJ52("ORIG_ORD_AS_FLAG") === "4", "O")
                .when(OOAJ52("ORIG_ORD_AS_FLAG") === "1", "P")
                .when(OOAJ52("ORIG_ORD_AS_FLAG") === "5", "P").otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALDESCRIPTION", OOAJ52("DISPLAY_zh"))

        OOAJ52=OOAJ52.withColumn("ISSUEDATE", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull, OOAJ52("ORDER_DT_TM")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALPROVIDERID", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull and (OOAJ52("ORDER_PROVIDER_ID") !== "0"), OOAJ52("ORDER_PROVIDER_ID")))

        //STOPPED here
        OOAJ52=OOAJ52.withColumn("ORDERSTATUS", when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG") === "1", concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("ORDER_STATUS_CD"))).otherwise(null))
        OOAJ52=OOAJ52.withColumn("INFUSEOVER", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_IO_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("INFUSEOVERUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_IOUNIT_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("RATE", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_RATE_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("RATEUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_RUNIT_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALINFUSIONVOLUME", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_TV_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALDAW", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_DAW_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"),  OOAJ52("OE_FIELD_DISPLAY_VALUE"))).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALDOSEFREQ", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_FREQ_OE_FIELD_ID_VAL").isNotNull && OOAJ52("DESCRIPTION").isNotNull, OOAJ52("DESCRIPTION")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALDOSEUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_VDUNIT_OE_FIELD_ID_VAL").isNotNull && OOAJ52("DESCRIPTION").isNotNull, OOAJ52("DESCRIPTION")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("DURATION", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_DUR_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("DURATIONUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_DURUNIT_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALFORM", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_DF_OE_FIELD_ID_VAL").isNotNull, OOAJ52("DESCRIPTION")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALQTYOFDOSEUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_VD_OE_FIELD_ID_VAL").isNotNull, safe_to_number(OOAJ52("OE_FIELD_DISPLAY_VALUE"))).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALROUTE", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_RXROUTE_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("OE_FIELD_VALUE"))).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALSTRENGTHPERDOSEUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_SD_OE_FIELD_ID_VAL").isNotNull,  safe_to_number(OOAJ52("OE_FIELD_DISPLAY_VALUE"))).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALSTRENGTHUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_SDUNIT_OE_FIELD_ID_VAL").isNotNull, OOAJ52("DESCRIPTION")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("FILLNUM", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_NBR_OE_FIELD_ID_VAL").isNotNull,  safe_to_number_with_default(OOAJ52("OE_FIELD_DISPLAY_VALUE"),"9999")).otherwise(null))
        OOAJ52=OOAJ52.withColumn("LOCALDAYSUPPLIED", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_LDS_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")))
        OOAJ52=OOAJ52.withColumn("QUANTITYPERFILL", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_QTY_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")))
        OOAJ52=OOAJ52.withColumn("DISCONTINUEDATE", when((OOAJ52("LATEST_ACTION_SEQUENCE_FLG") === 1 && OOAJ52("LIST_RQSTARTDT_ACT_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_RQSTARTDT_OE_FIELD_ID_VAL").isNotNull).or(
            OOAJ52("LATEST_ACTION_SEQUENCE_FLG") === 1 && OOAJ52("LIST_STOP_ACT_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_STOPDT_OE_FIELD_ID_VAL").isNotNull), OOAJ52("OE_FIELD_DT_TM_VALUE")).otherwise(null))

        OOAJ52=OOAJ52.withColumn("DISCONTINUEREASON",
            when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG")===1 and OOAJ52("LIST_ACTION_TYPE_CD_VAL").isNotNull, when(OOAJ52("STOP_TYPE_CD") === "0", null).otherwise(concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("STOP_TYPE_CD"))))
                    .when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG")===1 and OOAJ52("LIST_CANCEL_ACTION_TYPE_CD_VAL").isNotNull and OOAJ52("LIST_CR_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("DESCRIPTION")))
                    .when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG")===1 and OOAJ52("LIST_DISCONT_ACT_TYPE_CD_VAL").isNotNull and OOAJ52("LIST_DCREASON_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("DESCRIPTION")))
                    .when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG")===1 and OOAJ52("LIST_SUSPEND_ACTION_TYPE_CD_VAL").isNotNull and OOAJ52("LIST_SUSREASON_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("DESCRIPTION")))
                    .otherwise(null))

        //OOAJ52=OOAJ52.withColumn("DISCONTINUEREASON", when(cdn1.or(cdn2).or(cdn3), str).when(cdn4,  str2))
        OOAJ52=OOAJ52.withColumnRenamed("ORDER_ID", "RXID1").withColumn("ODDLS", when(OOAJ52("SIGNATURE").isin("0", "4"), "O").when(OOAJ52("SIGNATURE").isin("1", "5"), "P"))

        //        var OOAJ53 = OOAJ52.withColumnRenamed("PERSON_ID","PATIENTID").groupBy("RXID1", "GROUPID_oms", "CLIENT_DS_ID_oms", "PATIENTID", "ENCNTR_ID", "LOCALMEDCODE", "SIGNATURE", "ORDERVSPRESCRIPTION", "LOCALDESCRIPTION", "ORIG_ORD_AS_FLAG")
        var OOAJ53 = OOAJ52.withColumnRenamed("PERSON_ID","PATIENTID").groupBy("RXID1", "PATIENTID", "ENCNTR_ID",  "GROUPID_oms", "CLIENT_DS_ID_oms","CATALOG_CD", "ODDLS", "LOCALDESCRIPTION",  "ORIG_ORD_AS_FLAG", "ORIG_ORDER_DT_TM")
                .agg(max(OOAJ52("ORDERSTATUS")).as("ORDERSTATUS"), max(OOAJ52("DELETE_FLG")).as("DELETE_FLG"), max(OOAJ52("INFUSEOVER")).as("INFUSEOVER"), max(OOAJ52("INFUSEOVERUNIT")).as("INFUSEOVERUNIT"), max(OOAJ52("RATE")).as("RATE"), max(OOAJ52("RATEUNIT")).as("RATEUNIT"), max(OOAJ52("LOCALINFUSIONVOLUME")).as("LOCALINFUSIONVOLUME"),
                    max(OOAJ52("LOCALDAW")).as("LOCALDAW"), max(OOAJ52("LOCALDOSEFREQ")).as("LOCALDOSEFREQ"), max(OOAJ52("LOCALDOSEUNIT")).as("LOCALDOSEUNIT"), max(OOAJ52("DURATION")).as("DURATION"), max(OOAJ52("DURATIONUNIT")).as("DURATIONUNIT"),
                    max(OOAJ52("LOCALFORM")).as("LOCALFORM"), max(OOAJ52("LOCALQTYOFDOSEUNIT")).as("LOCALQTYOFDOSEUNIT"), max(OOAJ52("LOCALROUTE")).as("LOCALROUTE"), max(OOAJ52("LOCALSTRENGTHPERDOSEUNIT")).as("LOCALSTRENGTHPERDOSEUNIT"), max(OOAJ52("LOCALSTRENGTHUNIT")).as("LOCALSTRENGTHUNIT"),
                    max(OOAJ52("FILLNUM")).as("FILLNUM"), max(OOAJ52("LOCALDAYSUPPLIED")).as("LOCALDAYSUPPLIED"), max(OOAJ52("QUANTITYPERFILL")).as("QUANTITYPERFILL"), max(OOAJ52("DISCONTINUEDATE")).as("DISCONTINUEDATE"), max(OOAJ52("DISCONTINUEREASON")).as("DISCONTINUEREASON"), max(OOAJ52("LOCALPROVIDERID")).as("LOCALPROVIDERID"),
                    max(OOAJ52("ISSUEDATE")).as("ISSUEDATE"), max(OOAJ52("LOCALMEDCODE")).as("LOCALMEDCODE"), max(OOAJ52("SIGNATURE")).as("SIGNATURE"), max(OOAJ52("ORDERVSPRESCRIPTION")).as("ORDERVSPRESCRIPTION")).withColumnRenamed("GROUPID_oms", "GRPID2").withColumnRenamed("CLIENT_DS_ID_oms", "CLSID2")


        var OOAJ55 = OOAJ53.filter(OOAJ53("LOCALMEDCODE").isNotNull)
        var OOAJ56 = OOAJ55.withColumn("LOCALINFUSIONDURATION", concat_ws(" ", OOAJ55("INFUSEOVER"), OOAJ55("INFUSEOVERUNIT")))
                .withColumn("LOCALINFUSIONRATE", concat_ws(" ",OOAJ55("RATE"), OOAJ55("RATEUNIT")))
                .withColumn("LOCALDURATION", concat_ws(" ", OOAJ55("DURATION"), OOAJ55("DURATIONUNIT")))
                .withColumnRenamed("LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHPERDOSEUNIT1")
                .withColumnRenamed("LOCALQTYOFDOSEUNIT", "LOCALQTYOFDOSEUNIT1")

        var ENC = dfs("temptable").withColumnRenamed("GROUPID", "ENC_GROUPID").withColumnRenamed("ENCOUNTERID", "ENC_ENCOUNTERID")
                .withColumnRenamed("CLIENT_DS_ID", "ENC_CLIENT_DS_ID")

        var MPT = dfs("cdr.map_patient_type").withColumnRenamed("CUI", "MPT_CUI")

        var TEMP_H416989_ENCOUNTER_PT = ENC.join(MPT, ENC("LOCALPATIENTTYPE") === MPT("LOCAL_CODE") and ENC("ENC_GROUPID") === MPT("GROUPID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID").withColumnRenamed("PATIENTID", "PATIENTID_TEMP")
        var OOAJ57 = OOAJ56.join(TEMP_H416989_ENCOUNTER_PT, OOAJ56("PATIENTID") === TEMP_H416989_ENCOUNTER_PT("PATIENTID_TEMP") &&
                OOAJ56("ENCNTR_ID") === TEMP_H416989_ENCOUNTER_PT("ENC_ENCOUNTERID") &&
                OOAJ56("GRPID2") === TEMP_H416989_ENCOUNTER_PT("ENC_GROUPID") &&
                OOAJ56("CLSID2") === TEMP_H416989_ENCOUNTER_PT("ENC_CLIENT_DS_ID"), "left_outer").drop("ENC_GROUPID").drop("ENC_CLIENT_DS_ID").drop("PATIENTID_TEMP")

        var MOT = dfs("map_ordertype").withColumnRenamed("CUI", "MOT_CUI").withColumnRenamed("LOCALCODE", "MOT_LOCALCODE")
        OOAJ57 = OOAJ57.join(MOT,OOAJ57("GRPID2")===MOT("GROUPID") and OOAJ57("ORIG_ORD_AS_FLAG")===MOT("MOT_LOCALCODE"), "left_outer")

        OOAJ57 = OOAJ57.withColumn("ORDERTYPE", OOAJ57("MOT_CUI"))
        var ZHOCIR = dfs("zh_order_catalog_item_r")
        ZHOCIR=ZHOCIR.withColumn("FILE_ID",ZHOCIR("FILEID").cast("String"))     .withColumnRenamed("ITEM_ID","ZHOCIR_ITEM_ID").select("FILE_ID",  "ZHOCIR_ITEM_ID", "CATALOG_CD")

        var MID  = dfs("zh_med_identifier")
        MID=MID.withColumn("FILE_ID", MID("FILEID").cast("String"))    .select("FILE_ID",  "VALUE_KEY", "ITEM_ID", "MED_IDENTIFIER_TYPE_CD", "ACTIVE_IND", "VALUE")


        var LIST_LOCALDRUGDESC = predicate_value_listlike(mpv, "LOCALDRUGDESC", "TEMP_MEDS_", "ZH_MED_IDENTIFIER", "MED_IDENTIFIER_TYPE_CD", "LIST_LOCALDRUGDESC_VAL");
        var LIST_LOCALNDC = predicate_value_listlike(mpv, "LOCALNDC", "TEMP_MEDS", "ZH_MED_IDENTIFIER", "MED_IDENTIFIER_TYPE_CD", "LIST_LOCALNDC_VAL");

        var TMJ0 = ZHOCIR.join(MID, ZHOCIR("ZHOCIR_ITEM_ID") === MID("ITEM_ID") && ZHOCIR("GROUPID_zc") === MID("GROUPID_mid") && ZHOCIR("CLIENT_DS_ID_zc") === MID("CLIENT_DS_ID_mid"), "left_outer").drop("GROUPID_mid").drop("CLIENT_DS_ID_mid")
        var TMJ01 = TMJ0.join(LIST_LOCALDRUGDESC, TMJ0("MED_IDENTIFIER_TYPE_CD") === LIST_LOCALDRUGDESC("LIST_LOCALDRUGDESC_VAL") && TMJ0("GROUPID_zc") === LIST_LOCALDRUGDESC("GROUPID") && TMJ0("CLIENT_DS_ID_zc") === LIST_LOCALDRUGDESC("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
        var TMJ02 = TMJ01.join(LIST_LOCALNDC, TMJ01("MED_IDENTIFIER_TYPE_CD") === LIST_LOCALNDC("LIST_LOCALNDC_VAL") && TMJ01("GROUPID_zc") === LIST_LOCALNDC("GROUPID") && TMJ01("CLIENT_DS_ID_zc") === LIST_LOCALNDC("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
        var TMJ1 = TMJ02.filter("ACTIVE_IND == '1' AND ( LIST_LOCALDRUGDESC_VAL IS NOT NULL OR LIST_LOCALNDC_VAL IS NOT NULL)")
        var TMJ2 = TMJ1.withColumn("LEN", length(TMJ1("VALUE"))).withColumn("II",  TMJ1("ITEM_ID"))
        val group5 = Window.partitionBy(TMJ2("CATALOG_CD") , TMJ2("MED_IDENTIFIER_TYPE_CD"), TMJ2("GROUPID_zc"), TMJ2("CLIENT_DS_ID_zc")).orderBy(TMJ2("LEN"), TMJ2("II"))
        var TMJ3 = TMJ2.withColumn("ROWNUMBER", row_number().over(group5))

        val group6 = Window.partitionBy(TMJ3("CATALOG_CD"), TMJ3("MED_IDENTIFIER_TYPE_CD"), TMJ3("GROUPID_zc"), TMJ3("CLIENT_DS_ID_zc"))
        var TMJ6 = TMJ3.withColumn("CNT_NDC", count("VALUE_KEY").over(group6))

        var TMJ7 = TMJ6.withColumn("LOCALGENERICDESC", when(TMJ6("ROWNUMBER") === "1" and TMJ6("LIST_LOCALDRUGDESC_VAL").isNotNull, TMJ6("VALUE"))).withColumn("LOCALNDC", when(TMJ6("CNT_NDC") === "1" and TMJ6("LIST_LOCALNDC_VAL").isNotNull, TMJ6("VALUE_KEY")))
        var TMJ9 = TMJ7.groupBy("CATALOG_CD", "GROUPID_zc", "CLIENT_DS_ID_zc").agg(max(TMJ7("LOCALGENERICDESC")).as("LOCALGENERICDESC"), max(TMJ7("LOCALNDC")).as("LOCALNDC"))

        OOAJ57.join(TMJ9, OOAJ57("LOCALMEDCODE") === TMJ9("CATALOG_CD") && OOAJ57("GRPID2") === TMJ9("GROUPID_zc") && OOAJ57("CLSID2") === TMJ9("CLIENT_DS_ID_zc"), "left_outer")
    }


    afterJoin = (df: DataFrame) => {
        df.withColumn("FACILITYID", lit("NULL"))
                .withColumn("EXPIREDATE", lit("NULL"))
                .withColumn("ALTMEDCODE", lit("NULL"))
                .withColumn("MAPPEDNDC", lit("NULL"))
                .withColumn("MAPPEDGPI", lit("NULL"))
                .withColumn("MAPPEDNDC_CONF", lit("NULL"))
                .withColumn("MAPPEDGPI_CONF", lit("NULL"))
                .withColumn("MAP_USED", lit("NULL"))
                .withColumn("HUM_MED_KEY", lit("NULL"))
                .withColumn("NDC11", lit("NULL"))
                .withColumn("LOCALGPI", lit("NULL"))
                .withColumn("LOCALDISCHARGEMEDFLG", lit("NULL"))
                .withColumn("HUM_GEN_MED_KEY", lit("NULL"))
                .withColumn("HTS_GENERIC", lit("NULL"))
                .withColumn("HTS_GENERIC_EXT", lit("NULL"))
                .withColumn("HGPID", lit("NULL"))
                .withColumn("GRP_MPI", lit("NULL"))
                .withColumn("MSTRPROVID", lit("NULL"))
                .withColumn("ALLOWEDAMOUNT", lit("NULL"))
                .withColumn("PAIDAMOUNT", lit("NULL"))
                .withColumn("CHARGE", lit("NULL"))
                .withColumn("DCC", lit("NULL"))
                .withColumn("RXNORM_CODE", lit("NULL"))
                .withColumn("ACTIVE_MED_FLAG", lit("NULL"))
                .withColumn("ROW_SOURCE", lit("NULL"))
                .withColumn("MODIFIED_DATE", lit("NULL"))
    }


    map = Map(
        "GROUPID" -> mapFrom("GRPID2"),
        "CLIENT_DS_ID" -> mapFrom("CLSID2"),
        "VENUE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("ORIG_ORD_AS_FLAG") === "1" or df("MPT_CUI").isin("CH000110", "CH000113", "CH000924", "CH000935", "CH000936", "CH000937"), "1").otherwise("0"))}),
        "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  df("LOCALSTRENGTHPERDOSEUNIT1").multiply(df("LOCALQTYOFDOSEUNIT1")))}),
        "DATASRC" -> literal("orders"),
        "LOCALQTYOFDOSEUNIT" -> mapFrom("LOCALQTYOFDOSEUNIT1"),
        "LOCALSTRENGTHPERDOSEUNIT" -> mapFrom("LOCALSTRENGTHPERDOSEUNIT1"),
        "RXID" -> mapFrom("RXID1"),
        "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
        "FACILITYID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("FACILITYID")==="NULL", null).otherwise(df("FACILITYID")))}),
        "EXPIREDATE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("EXPIREDATE")==="NULL", null).otherwise(df("EXPIREDATE")))}),
        "ALTMEDCODE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("ALTMEDCODE")==="NULL", null).otherwise(df("ALTMEDCODE")))}),
        "MAPPEDNDC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("MAPPEDNDC")==="NULL", null).otherwise(df("MAPPEDNDC")))}),
        "MAPPEDGPI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("MAPPEDGPI")==="NULL", null).otherwise(df("MAPPEDGPI")))}),
        "MAPPEDNDC_CONF" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("MAPPEDNDC_CONF")==="NULL", null).otherwise(df("MAPPEDNDC_CONF")))}),
        "MAPPEDGPI_CONF" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("MAPPEDGPI_CONF")==="NULL", null).otherwise(df("MAPPEDGPI_CONF")))}),
        "MAP_USED" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("MAP_USED")==="NULL", null).otherwise(df("MAP_USED")))}),
        "HUM_MED_KEY" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("HUM_MED_KEY")==="NULL", null).otherwise(df("HUM_MED_KEY")))}),
        "NDC11" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("NDC11")==="NULL", null).otherwise(df("NDC11")))}),
        "LOCALGPI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("LOCALGPI")==="NULL", null).otherwise(df("LOCALGPI")))}),
        "LOCALDISCHARGEMEDFLG" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("LOCALDISCHARGEMEDFLG")==="NULL", null).otherwise(df("LOCALDISCHARGEMEDFLG")))}),
        "HUM_GEN_MED_KEY" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("HUM_GEN_MED_KEY")==="NULL", null).otherwise(df("HUM_GEN_MED_KEY")))}),
        "HTS_GENERIC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("HTS_GENERIC")==="NULL", null).otherwise(df("HTS_GENERIC")))}),
        "HTS_GENERIC_EXT" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("HTS_GENERIC_EXT")==="NULL", null).otherwise(df("HTS_GENERIC_EXT")))}),
        "HGPID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("HGPID")==="NULL", null).otherwise(df("HGPID")))}),
        "GRP_MPI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("GRP_MPI")==="NULL", null).otherwise(df("GRP_MPI")))}),
        "MSTRPROVID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("MSTRPROVID")==="NULL", null).otherwise(df("MSTRPROVID")))}),
        "ALLOWEDAMOUNT" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("ALLOWEDAMOUNT")==="NULL", null).otherwise(df("ALLOWEDAMOUNT")))}),
        "PAIDAMOUNT" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("PAIDAMOUNT")==="NULL", null).otherwise(df("PAIDAMOUNT")))}),
        "CHARGE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("CHARGE")==="NULL", null).otherwise(df("CHARGE")))}),
        "DCC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("DCC")==="NULL", null).otherwise(df("DCC")))}),
        "RXNORM_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("RXNORM_CODE")==="NULL", null).otherwise(df("RXNORM_CODE")))}),
        "ACTIVE_MED_FLAG" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("ACTIVE_MED_FLAG")==="NULL", null).otherwise(df("ACTIVE_MED_FLAG")))}),
        "ROW_SOURCE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("ROW_SOURCE")==="NULL", null).otherwise(df("ROW_SOURCE")))}),
        "MODIFIED_DATE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("MODIFIED_DATE")==="NULL", null).otherwise(df("MODIFIED_DATE")))})
    )
}

//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")