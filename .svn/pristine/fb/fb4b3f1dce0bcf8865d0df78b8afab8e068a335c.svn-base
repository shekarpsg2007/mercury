package com.humedica.mercury.etl.crossix.rxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderBackendmedmapping(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    tables = List("rxorder", "cdr.zh_med_map_dcc", "cdr.map_med_order_status")

    columnSelect = Map(
        "rxorder" -> List(),
        "cdr.zh_med_map_dcc" -> List("CLIENT_DS_ID","PATIENTID","HGPID","GRP_MPI"),
        "cdr.map_med_order_status" -> List("MASTER_HGPROVID", "LOCALPROVIDERID", "CLIENT_DS_ID")
    )


    def normalizeNDC(df:DataFrame, col:String) ={
        var srcNDC = df(col)
        when(length(srcNDC) > 13, lit("NULL"))
                .otherwise( {
                    var normalNDC = regexp_replace(srcNDC, "[^-0-9]", "")
                    when(normalNDC === srcNDC && normalNDC.contains("-") === false && length(normalNDC)===11, regexp_replace(normalNDC, "-", ""))
                            .otherwise(when(length(regexp_replace(normalNDC, "[^-]", ""))!==2, lit("NULL"))
                                    .otherwise({
                                        var v_normal_ndc = concat_ws("", lpad(regexp_extract(normalNDC, "^[0-9]{4,5}", 0), 5, "0"),
                                            lpad(regexp_extract(normalNDC, "^[0-9]{4,5}", 0), 4, "0"),
                                            lpad(regexp_extract(normalNDC, "^[0-9]{4,5}", 0), 2, "0"))
                                        when(length(v_normal_ndc).between(10, 11),  lpad(v_normal_ndc, 11, "0"))
                                    }))
                })
    }


    join = (dfs: Map[String, DataFrame])  => {
        var rxo = dfs("rxorder")
        var RXORDER = rxo.drop("MAP_USED").drop("HUM_MED_KEY").drop("HUM_GEN_MED_KEY").drop("HUM_GEN_MED_KEY").drop("HTS_GENERIC").drop("HTS_GENERIC_EXT").drop("DCC").drop("RXNORM_CODE")

        var MED_MAP_DCC = dfs("zh_med_map_dcc").select("DATASRC", "LOCALMEDCODE", "MAP_USED", "HUM_MED_KEY", "HUM_GEN_MED_KEY", "HTS_GENERIC",
            "HTS_GENERIC_EXT", "HTS_GPI", "TOP_NDC", "DCC", "DTS_VERSION", "RXNORM_CODE")
                .withColumnRenamed("DATASRC", "DATASRC_mmd").withColumnRenamed("LOCALMEDCODE", "LOCALMEDCODE_mmd")

        var RXMED_MAP = RXORDER.join(MED_MAP_DCC, RXORDER("DATASRC") === MED_MAP_DCC("DATASRC_mmd") and RXORDER("LOCALMEDCODE") === MED_MAP_DCC("LOCALMEDCODE_mmd"), "left_outer")
        RXMED_MAP =RXMED_MAP.withColumn("NDC11", normalizeNDC(RXMED_MAP, "LOCALNDC"))
        RXMED_MAP = RXMED_MAP.withColumn("NDC11", when(RXMED_MAP("NDC11")==="NULL", null).otherwise(RXMED_MAP("NDC11")))

        var RXMED_MAP_FIN = RXMED_MAP.select("DATASRC", "FACILITYID", "RXID", "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID", "ISSUEDATE", "DISCONTINUEDATE",
            "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE", "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE", "ORDERSTATUS", "ALTMEDCODE",
            "TOP_NDC", "HTS_GPI", "MAPPEDNDC_CONF", "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED", "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ",
            "LOCALDURATION", "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",
            "LOCALDAW", "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON",
            "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE", "ACTIVE_MED_FLAG").withColumn("MAPPEDNDC", RXMED_MAP("TOP_NDC")).withColumn("MAPPEDGPI", RXMED_MAP("HTS_GPI"))
        var MAP_MED_ORDER_STATUS = dfs("map_med_order_status").select("LOCALCODE", "CUI", "DTS_VERSION")

        var UPDATE_MEDS_1 = RXMED_MAP_FIN.join(MAP_MED_ORDER_STATUS, RXMED_MAP_FIN("LOCALMEDCODE") === MAP_MED_ORDER_STATUS("LOCALCODE"), "left_outer")
        var ORDERVSPRESCRIPTION = UPDATE_MEDS_1("ORDERVSPRESCRIPTION")
        var DISCONTINUEDATE =from_unixtime(UPDATE_MEDS_1("DISCONTINUEDATE").divide(1000))
        var ISSUEDATE = UPDATE_MEDS_1("ISSUEDATE")
        var EXPIREDATE = UPDATE_MEDS_1("EXPIREDATE")
        var UPDATE_ACTIVE_MEDS_RXORDER = UPDATE_MEDS_1.withColumn("ACTIVE_MED_FLAG", when((ORDERVSPRESCRIPTION === "P").and(DISCONTINUEDATE.isNotNull).and(
            ISSUEDATE.lt(DISCONTINUEDATE)).and(DISCONTINUEDATE < current_timestamp()), "N")
                .when((ORDERVSPRESCRIPTION === "P").and(DISCONTINUEDATE.isNotNull), "N")
                .when((ORDERVSPRESCRIPTION === "P").and(ISSUEDATE.lt(EXPIREDATE)).and(EXPIREDATE.lt(current_timestamp())), "N")
                .when((ORDERVSPRESCRIPTION === "P").and(UPDATE_MEDS_1("CUI").isin("CH001593", "CH001594", "CH001595", "CH001596")), "N").otherwise("Y"))
        UPDATE_ACTIVE_MEDS_RXORDER
    }
}
