package com.humedica.mercury.etl.crossix.rxorder

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
/**
  * Created by rbabu on 1/31/18.
  */
class RxorderDcdrexport (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List()

    tables = List("consolidated_cdr", "cdr.map_med_disc_reason", "cdr.map_med_route", "cdr.map_med_schedule", "cdr.map_med_order_status",
        "cdr.zcm_dcdr_exclude", "cdr.zh_med_map_prefndc", "cdr.client_data_src", "cdr.facility_xref", "cdr.provider_xref")

    columnSelect = Map(
        "cdr.map_med_disc_reason" -> List(),
        "cdr.map_med_route" -> List("GROUPID", "LOCAL_CODE","CUI"),
        "cdr.map_med_schedule" -> List("GROUPID", "LOCALCODE","CUI"),
        "cdr.map_med_order_status" -> List("GROUPID", "LOCALCODE","CUI"),
        "cdr.zcm_dcdr_exclude" -> List("CLIENT_DS_ID"),
        "cdr.zh_med_map_prefndc" -> List(),
        "cdr.client_data_src" -> List("CLIENT_ID", "CLIENT_DATA_SRC_ID", "DCDR_EXCLUDE", "FINAL_RELEASE"),
        "cdr.facility_xref" -> List("GROUPID", "CLIENT_DS_ID", "FACILITYID", "HGFACID"),
        "cdr.provider_xref" -> List("GROUPID", "CLIENT_DS_ID", "PROVIDERID", "HGPROVID")
    )


    join = (dfs: Map[String, DataFrame])  => {
        var MAP_MED_DISC_REASON_D = dfs("cdr.map_med_disc_reason").withColumnRenamed("LOCALCODE","MDR_LOCALCODE").withColumnRenamed("CUI","MDR_CUI")

        var MAP_MED_ROUTE_D = dfs("cdr.map_med_route").withColumnRenamed("LOCAL_CODE", "MMR_LOCAL_CODE").withColumnRenamed("CUI","MMR_CUI")
        MAP_MED_ROUTE_D=MAP_MED_ROUTE_D.withColumn("MMR_LOCAL_CODE", upper(MAP_MED_ROUTE_D("MMR_LOCAL_CODE"))).filter(MAP_MED_ROUTE_D("MMR_LOCAL_CODE").isin("TA", "NA") === false).distinct()

        var MAP_MED_SCHEDULE_D = dfs("cdr.map_med_schedule").withColumnRenamed("LOCALCODE", "MMS_LOCALCODE").withColumnRenamed("CUI","MMS_CUI")
        MAP_MED_SCHEDULE_D=MAP_MED_SCHEDULE_D.withColumn("MMS_LOCALCODE", upper(MAP_MED_SCHEDULE_D("MMS_LOCALCODE"))).distinct()

        var MAP_MED_ORDER_STATUS_D = dfs("cdr.map_med_order_status").withColumnRenamed("GROUPID", "MMOS_GROUPID") .withColumnRenamed("LOCALCODE", "MMOS_LOCALCODE").withColumnRenamed("CUI","MMOS_CUI")
        MAP_MED_ORDER_STATUS_D=MAP_MED_ORDER_STATUS_D.withColumn("MMOS_LOCALCODE", upper(MAP_MED_ORDER_STATUS_D("MMOS_LOCALCODE"))).distinct()

        var ZCM_DCDR_EXCLUDE = dfs("cdr.zcm_dcdr_exclude")

        var ZH_MED_MAP_PREFNDC = dfs("cdr.zh_med_map_prefndc").withColumnRenamed("LOCALROUTE", "ZMMP_LOCALROUTE").withColumnRenamed("PREFERREDNDC", "ZMMP_PREFERREDNDC")
                .withColumnRenamed("LOCALSTRENGTHPERDOSEUNIT", "ZMMP_LOCALSTRENGTHPERDOSEUNIT").withColumnRenamed("LOCALSTRENGTHUNIT", "ZMMP_LOCALSTRENGTHUNIT")
                .withColumnRenamed("LOCALFORM", "ZMMP_LOCALFORM").withColumnRenamed("DCC", "ZMMP_DCC")

        var V_RXORDER_PREFNDC = dfs("consolidated_cdr")

        var clientDataSrc = dfs("cdr.client_data_src")
        clientDataSrc = clientDataSrc.filter("DCDR_EXCLUDE=='N' AND FINAL_RELEASE IS NULL")
        V_RXORDER_PREFNDC = V_RXORDER_PREFNDC.join(clientDataSrc, V_RXORDER_PREFNDC("GROUPID")===clientDataSrc("CLIENT_ID") and V_RXORDER_PREFNDC("CLIENT_DS_ID")===clientDataSrc("CLIENT_DATA_SRC_ID"), "inner")


        V_RXORDER_PREFNDC = V_RXORDER_PREFNDC.join(ZH_MED_MAP_PREFNDC,
            V_RXORDER_PREFNDC("GROUPID")===ZH_MED_MAP_PREFNDC("ZMMP_GROUPID") and V_RXORDER_PREFNDC("DCC")===ZH_MED_MAP_PREFNDC("ZMMP_DCC") and V_RXORDER_PREFNDC("LOCALROUTE").contains(ZH_MED_MAP_PREFNDC("ZMMP_LOCALROUTE")) and
                    V_RXORDER_PREFNDC("LOCALFORM")===ZH_MED_MAP_PREFNDC("ZMMP_LOCALFORM") and V_RXORDER_PREFNDC("LOCALSTRENGTHUNIT")===ZH_MED_MAP_PREFNDC("ZMMP_LOCALSTRENGTHPERDOSEUNIT") and
                    V_RXORDER_PREFNDC("LOCALSTRENGTHPERDOSEUNIT")===ZH_MED_MAP_PREFNDC("ZMMP_LOCALSTRENGTHPERDOSEUNIT"), "left_outer")

        var FACILITY_XREF = dfs("cdr.facility_xref").withColumnRenamed("FACILITYID", "FX_FACILITYID").withColumnRenamed("HGFACID", "FX_HGFACID")
        var PROVIDER_XREF = dfs("cdr.provider_xref").withColumnRenamed("PROVIDERID", "PPX_PROVIDERID").withColumnRenamed("HGPROVID", "PPX_HGPROVID")

        var J1 = V_RXORDER_PREFNDC.join(ZCM_DCDR_EXCLUDE, V_RXORDER_PREFNDC("CLIENT_DS_ID")===ZCM_DCDR_EXCLUDE("EXC_CLIENT_DS_ID"), "left_outer")
        var J2 = J1.join(MAP_MED_DISC_REASON_D, upper(J1("DISCONTINUEREASON"))===MAP_MED_DISC_REASON_D("MDR_LOCALCODE"), "left_outer")
        var J3 = J2.join(MAP_MED_ROUTE_D, upper(J2("LOCALROUTE"))===MAP_MED_ROUTE_D("MMR_LOCAL_CODE"), "left_outer")
        var J4 = J3.join(MAP_MED_SCHEDULE_D, upper(J3("LOCALDOSEFREQ"))===MAP_MED_SCHEDULE_D("MMS_LOCALCODE"), "left_outer")
        var J5 = J4.join(MAP_MED_ORDER_STATUS_D, upper(J4("ORDERSTATUS"))===MAP_MED_ORDER_STATUS_D("MMOS_LOCALCODE"), "left_outer")
        var J6 = J5.join(FACILITY_XREF, J5("FACILITYID")===FACILITY_XREF("FX_FACILITYID"), "left_outer")
        J6.join(PROVIDER_XREF, J6("LOCALPROVIDERID")===PROVIDER_XREF("PPX_PROVIDERID"), "left_outer")
    }

    afterJoin = (df: DataFrame) => {
        df.filter("EXC_CLIENT_DS_ID is null")
    }

    map = Map(
        "PREFERREDNDC_SOURCE" -> ((col: String, df: DataFrame) => {df.withColumn(col,   when(df("LOCALNDC").isNotNull, "logicndc").when(df("ZMMP_PREFERREDNDC").isNotNull, "logicndc").otherwise("mappedndc"))}),
        "FACILITYID" -> mapFrom("FX_HGFACID"),
        "LOCALPROVIDERID" -> mapFrom("PPX_HGPROVID"),
        "LOCALDESCRIPTION_PHI" -> mapFrom("LOCALDESCRIPTION"),
        "ROUTE_CUI" -> mapFrom("MMR_CUI"),
        "DOSEFREQ_CUI" -> mapFrom("MMS_CUI"),
        "MAPPED_ORDERSTATUS" -> mapFrom("MMOS_CUI"),
        "FACILITYID" -> mapFrom("FX_HGFACID"),
        "ORDERTYPE" -> ((col: String, df: DataFrame) => {df.withColumn(col,   when(df("ORDERTYPE").isin("CH002045", "CH002046", "CH002047"), df("ORDERTYPE"))
                .when(((df("ORDERTYPE").isin("CH002045", "CH002046", "CH002047")===false) or (df("ORDERTYPE").isNull)) and (df("ORDERVSPRESCRIPTION")==="P"), "CH002047")
                .when(((df("ORDERTYPE").isin("CH002045", "CH002046", "CH002047")===false) or (df("ORDERTYPE").isNull)) and (df("ORDERVSPRESCRIPTION")==="O"), "CH002045"))}),
        "PREFERREDNDC" -> ((col: String, df: DataFrame) => {df.withColumn(col,   when(df("LOCALNDC").isNotNull, df("LOCALNDC")).when(df("ZMMP_PREFERREDNDC").isNotNull, df("ZMMP_PREFERREDNDC")).otherwise(df("MAPPEDNDC")))})
    )
}
