package com.humedica.mercury.etl.crossix.rxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderDcdrimport(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI", "ORDERVSPRESCRIPTION", "ENCOUNTERID",
        "GROUPID", "FACILITYID", "LOCALMEDCODE", "LOCALPROVIDERID", "ISSUEDATE", "ALTMEDCODE", "ORDERTYPE", "ORDERSTATUS", "QUANTITYPERFILL", "FILLNUM", "RXID", "LOCALSTRENGTHPERDOSEUNIT",
        "LOCALSTRENGTHUNIT", "LOCALROUTE", "MAPPEDNDC", "LOCALDESCRIPTION_PHI","DATASRC", "VENUE", "MAPPEDNDC_CONF", "MAPPEDGPI","MAPPEDGPI_CONF", "HUM_MED_KEY",
        "MAP_USED", "LOCALDAYSUPPLIED","LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ","LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION",
        "LOCALNDC", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG","LOCALDAW", "HUM_GEN_MED_KEY","MSTRPROVID", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID",
        "DISCONTINUEDATE", "EXPIREDATE","DISCONTINUEREASON", "DCC", "NDC11", "ROUTE_CUI","DOSEFREQ_CUI", "MAPPED_ORDERSTATUS","RXNORM_CODE", "PREFERREDNDC", "PREFERREDNDC_SOURCE", "ORIGIN")

    tables = List("temptable:crossix.parient.RxOrderDcdrWhiteList", "common.all_objectids")

    columnSelect = Map(
        "temptable:crossix.parient.RxOrderDcdrWhiteList" -> List("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI", "ORDERVSPRESCRIPTION", "ENCOUNTERID",
            "GROUPID", "FACILITYID", "LOCALMEDCODE", "LOCALPROVIDERID", "ISSUEDATE", "ALTMEDCODE", "ORDERTYPE", "ORDERSTATUS", "QUANTITYPERFILL", "FILLNUM", "RXID", "LOCALSTRENGTHPERDOSEUNIT",
            "LOCALSTRENGTHUNIT", "LOCALROUTE", "MAPPEDNDC", "LOCALDESCRIPTION_PHI","DATASRC", "VENUE", "MAPPEDNDC_CONF", "MAPPEDGPI","MAPPEDGPI_CONF", "HUM_MED_KEY",
            "MAP_USED", "LOCALDAYSUPPLIED","LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ","LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION",
            "LOCALNDC", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG","LOCALDAW", "HUM_GEN_MED_KEY","MSTRPROVID", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID",
            "DISCONTINUEDATE", "EXPIREDATE","DISCONTINUEREASON", "DCC", "NDC11", "ROUTE_CUI","DOSEFREQ_CUI", "MAPPED_ORDERSTATUS","RXNORM_CODE", "PREFERREDNDC", "PREFERREDNDC_SOURCE", "ORIGIN"),
        "common.all_objectids" -> List("GROUPID")
    )


    beforeJoin = Map(
        "common.all_objectids" -> ((df: DataFrame) => {df.distinct()})
    )

    join = (dfs: Map[String, DataFrame])  => {
        dfs("temptable:crossix.parient.RxOrderDcdrImport").join(dfs("common.all_objectids"),Seq("GROUPID"), "inner")
    }

    map = Map(
        "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("DATASRC")!=="medorders", df("LOCALTOTALDOSE")))})
    )
}

//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")