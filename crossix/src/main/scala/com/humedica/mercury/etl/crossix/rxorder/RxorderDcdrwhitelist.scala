package com.humedica.mercury.etl.crossix.rxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderDcdrwhitelist(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI", "ORDERVSPRESCRIPTION",
        "ENCOUNTERID", "GROUPID", "FACILITYID", "LOCALMEDCODE", "LOCALPROVIDERID", "ISSUEDATE",
        "ALTMEDCODE", "ORDERTYPE", "ORDERSTATUS", "QUANTITYPERFILL", "FILLNUM", "RXID", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT",
        "LOCALROUTE", "MAPPEDNDC", "LOCALDESCRIPTION_PHI", "DATASRC", "VENUE", "MAPPEDNDC_CONF", "MAPPEDGPI", "MAPPEDGPI_CONF",
        "HUM_MED_KEY", "MAP_USED", "LOCALDAYSUPPLIED", "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ",
        "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",
        "LOCALDAW", "HUM_GEN_MED_KEY", "MSTRPROVID", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID", "DISCONTINUEDATE", "EXPIREDATE",
        "DISCONTINUEREASON", "DCC", "NDC11", "ROUTE_CUI", "DOSEFREQ_CUI", "MAPPED_ORDERSTATUS", "RXNORM_CODE", "PREFERREDNDC",
        "PREFERREDNDC_SOURCE", "ORIGIN")

    tables = List("temptable:crossix.parient.RxOrderDcdrExport", "common.grp_mpi_xref_iot", "common.encounter_xref_iot",
        "common.rxorder_xref_iot", "common.localmedcode_xref_iot", "common.w_rxorder_localdosefreq", "common.w_rxorder_localdescription",
        "common.w_rxorder_localdoseunit", "common.w_rxorder_localform", "common.w_rxorder_localqtyofdoseunit",
        "common.w_rxorder_localstrengthperdo", "common.w_rxorder_localstrengthunit", "common.w_rxorder_quantiryperfill",
        "common.w_rxorder_localroute", "common.restricted_meds", "common.zh_med_map_cdr_next")

    columnSelect = Map(
        "temptable:crossix.parient.RxOrderDcdrExport" -> List("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI", "ORDERVSPRESCRIPTION",
            "ENCOUNTERID", "GROUPID", "FACILITYID", "LOCALMEDCODE", "LOCALPROVIDERID", "ISSUEDATE",
            "ALTMEDCODE", "ORDERTYPE", "ORDERSTATUS", "QUANTITYPERFILL", "FILLNUM", "RXID", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT",
            "LOCALROUTE", "MAPPEDNDC", "LOCALDESCRIPTION_PHI", "DATASRC", "VENUE", "MAPPEDNDC_CONF", "MAPPEDGPI", "MAPPEDGPI_CONF",
            "HUM_MED_KEY", "MAP_USED", "LOCALDAYSUPPLIED", "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ",
            "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",
            "LOCALDAW", "HUM_GEN_MED_KEY", "MSTRPROVID", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID", "DISCONTINUEDATE", "EXPIREDATE",
            "DISCONTINUEREASON", "DCC", "NDC11", "ROUTE_CUI", "DOSEFREQ_CUI", "MAPPED_ORDERSTATUS", "RXNORM_CODE", "PREFERREDNDC",
            "PREFERREDNDC_SOURCE", "ORIGIN"),
        "common.grp_mpi_xref_iot" -> List("GROUPID", "GRP_MPI", "NEW_ID"),
        "common.encounter_xref_iot" -> List("GROUPID", "GRP_MPI", "ENCOUNTERID", "CLIENT_DS_ID", "HGENCID"),
        "common.rxorder_xref_iot" -> List("GROUPID", "CLIENT_DS_ID", "RXID", "NEW_ID"),
        "common.localmedcode_xref_iot" -> List("GROUPID", "CLIENT_DS_ID", "LOCALMEDCODE", "MAPPED_VAL"),
        "common.w_rxorder_localdosefreq" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.w_rxorder_localdescription" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.w_rxorder_localdoseunit" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.w_rxorder_localform" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.w_rxorder_localqtyofdoseunit" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.w_rxorder_localstrengthperdo" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.w_rxorder_localstrengthunit" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.w_rxorder_quantiryperfill" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.w_rxorder_localroute" -> List("GROUPID", "RAWVAL", "CLEANVAL"),
        "common.restricted_meds" -> List("GROUPID", "SIMPLE_GENERIC_C"),
        "common.zh_med_map_cdr_next" -> List("GROUPID", "CLIENT_DS_ID", "DATASRC", "MAPPEDLOCALMEDCODE", "HTS_GPI")
    )


    beforeJoin = Map(
        "temptable:crossix.parient.RxOrderDcdrExport" -> ((df: DataFrame) => {

            df.withColumn("LOCALDOSEFREQ", ltrim(rtrim(regexp_replace(regexp_replace(df("LOCALDOSEFREQ"), "\\n", " "), "\\r", " "))))
                    .withColumn("LOCALDESCRIPTION_PHI", ltrim(rtrim(regexp_replace(regexp_replace(df("LOCALDESCRIPTION_PHI"), "\\n", " "), "\\r", " "))))
                    .withColumn("LOCALDOSEUNIT", ltrim(rtrim(regexp_replace(regexp_replace(df("LOCALDOSEUNIT"), "\\n", " "), "\\r", " "))))
                    .withColumn("LOCALFORM", ltrim(rtrim(regexp_replace(regexp_replace(df("LOCALFORM"), "\\n", " "), "\\r", " "))))
                    .withColumn("LOCALQTYOFDOSEUNIT", ltrim(rtrim(regexp_replace(regexp_replace(df("LOCALQTYOFDOSEUNIT"), "\\n", " "), "\\r", " "))))
                    .withColumn("LOCALSTRENGTHUNIT", ltrim(rtrim(regexp_replace(regexp_replace(df("LOCALSTRENGTHUNIT"), "\\n", " "), "\\r", " "))))
                    .withColumn("QUANTITYPERFILL", ltrim(rtrim(regexp_replace(regexp_replace(df("QUANTITYPERFILL"), "\\n", " "), "\\r", " "))))
                    .withColumn("LOCALROUTE", ltrim(rtrim(regexp_replace(regexp_replace(df("LOCALROUTE"), "\\n", " "), "\\r", " "))))
                    .withColumn("LOCALSTRENGTHPERDOSEUNIT", ltrim(rtrim(regexp_replace(regexp_replace(df("LOCALSTRENGTHPERDOSEUNIT"), "\\n", " "), "\\r", " "))))}),

        "common.grp_mpi_xref_iot" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "G_GROUPID").withColumnRenamed("GRP_MPI", "G_GRP_MPI").withColumnRenamed("NEW_ID", "G_NEW_ID")}),
        "common.encounter_xref_iot" -> ((df: DataFrame) => {df.withColumnRenamed("HGENCID", "E_HGENCID").withColumnRenamed("GROUPID", "E_GROUPID").withColumnRenamed("GRP_MPI", "E_GRP_MPI").withColumnRenamed("ENCOUNTERID", "E_ENCOUNTERID").withColumnRenamed("CLIENT_DS_ID", "E_CLIENT_DS_ID")}),
        "common.rxorder_xref_iot" -> ((df: DataFrame) => {df.withColumnRenamed("NEW_ID", "T1_NEW_ID").withColumnRenamed("GROUPID", "T1_GROUPID").withColumnRenamed("CLIENT_DS_ID", "T1_CLIENT_DS_ID").withColumnRenamed("RXID", "T1_RXID")}),
        "common.localmedcode_xref_iot" -> ((df: DataFrame) => {df.withColumnRenamed("MAPPED_VAL", "LMC_MAPPED_VAL").withColumnRenamed("GROUPID", "LMC_GROUPID").withColumnRenamed("CLEANVAL", "LMC_CLEANVAL").withColumnRenamed("LOCALMEDCODE", "LMC_LOCALMEDCODE").withColumnRenamed("CLIENT_DS_ID", "LMC_CLIENT_DS_ID")}),
        "common.w_rxorder_localdosefreq" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "LDF_GROUPID").withColumnRenamed("RAWVAL", "LDF_RAWVAL").withColumnRenamed("CLEANVAL", "LDF_CLEANVAL")}),
        "common.w_rxorder_localdescription" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "LD_GROUPID").withColumnRenamed("RAWVAL", "LD_RAWVAL").withColumnRenamed("CLEANVAL", "LD_CLEANVAL")}),
        "common.w_rxorder_localdoseunit" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "LDU_GROUPID").withColumnRenamed("RAWVAL", "LDU_RAWVAL").withColumnRenamed("CLEANVAL", "LDU_CLEANVAL")}),
        "common.w_rxorder_localform" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "LF_GROUPID").withColumnRenamed("RAWVAL", "LF_RAWVAL").withColumnRenamed("CLEANVAL", "LF_CLEANVAL")}),
        "common.w_rxorder_localqtyofdoseunit" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "LQU_GROUPID").withColumnRenamed("RAWVAL", "LQU_RAWVAL").withColumnRenamed("CLEANVAL", "LQU_CLEANVAL")}),
        "common.w_rxorder_localstrengthperdo" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "LPU_GROUPID").withColumnRenamed("RAWVAL", "LPU_RAWVAL").withColumnRenamed("CLEANVAL", "LPU_CLEANVAL")}),
        "common.w_rxorder_localstrengthunit" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "LSU_GROUPID").withColumnRenamed("RAWVAL", "LSU_RAWVAL").withColumnRenamed("CLEANVAL", "LSU_CLEANVAL")}),
        "common.w_rxorder_quantityperfill" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "QPF_GROUPID").withColumnRenamed("RAWVAL", "QPF_RAWVAL").withColumnRenamed("CLEANVAL", "QPF_CLEANVAL")}),
        "common.w_rxorder_localroute" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "RTE_GROUPID").withColumnRenamed("RAWVAL", "RTE_RAWVAL").withColumnRenamed("CLEANVAL", "RTE_CLEANVAL")}),
        "common.restricted_meds" -> ((df: DataFrame) => {df.withColumnRenamed("GROUPID", "RM_GROUPID").withColumnRenamed("SIMPLE_GENERIC_C", "RM_SIMPLE_GENERIC_C")}),
        "common.zh_med_map_cdr_next" -> ((df: DataFrame) => {df.withColumnRenamed("HTS_GPI", "ZMM_HTS_GPI").withColumnRenamed("GROUPID", "ZMM_GROUPID").withColumnRenamed("CLIENT_DS_ID", "ZMM_CLIENT_DS_ID").withColumnRenamed("DATASRC", "ZMM_DATASRC").withColumnRenamed("MAPPEDLOCALMEDCODE", "ZMM_MAPPEDLOCALMEDCODE")})
    )

    join = (dfs: Map[String, DataFrame])  => {
        var rxOrderDcdrExport = dfs("temptable:crossix.parient.RxOrderDcdrExport")
        var grpmpi = dfs("common.grp_mpi_xref_iot")
        var encxref = dfs("common.encounter_xref_iot")
        var rxoref = dfs("common.rxorder_xref_iot")
        var lmcref = dfs("common.localmedcode_xref_iot")
        var ldcxref = dfs("common.w_rxorder_localdosefreq")
        var ldxref = dfs("common.w_rxorder_localdescription")

        var lduxref = dfs("common.w_rxorder_localdoseunit")
        var lfxref = dfs("common.w_rxorder_localform")
        var lqduxref = dfs("common.w_rxorder_localqtyofdoseunit")
        var lspuxref = dfs("common.w_rxorder_localstrengthperdo")
        var lsuxref = dfs("common.w_rxorder_localstrengthunit")
        var qpfxref = dfs("common.w_rxorder_quantityperfill")
        var lrxref = dfs("common.w_rxorder_localroute")
        var rmxref = dfs("common.restricted_meds")
        var zmmref = dfs("common.zh_med_map_cdr_next")

        rxOrderDcdrExport.join(grpmpi, rxOrderDcdrExport("GROUPID")===grpmpi("G_GROUPID") and rxOrderDcdrExport("GRP_MPI")===grpmpi("G_GRP_MPI"), "left_outer")
                .join(encxref, rxOrderDcdrExport("GROUPID")===encxref("E_GROUPID") and rxOrderDcdrExport("CLIENT_DS_ID")===encxref("E_CLIENT_DS_ID") and rxOrderDcdrExport("ENCOUNTERID")===encxref("E_ENCOUNTERID"), "left_outer")
                .join(rxoref, rxOrderDcdrExport("GROUPID")===rxoref("T1_GROUPID") and rxOrderDcdrExport("CLIENT_DS_ID")===rxoref("T1_CLIENT_DS_ID") and rxOrderDcdrExport("RXID")===rxoref("T1_RXID"), "left_outer")
                .join(lmcref, rxOrderDcdrExport("GROUPID")===lmcref("LMC_GROUPID") and rxOrderDcdrExport("CLIENT_DS_ID")===lmcref("LMC_CLIENT_DS_ID") and rxOrderDcdrExport("LOCALMEDCODE")===lmcref("LMC_LOCALMEDCODE"), "left_outer")
                .join(ldcxref, rxOrderDcdrExport("GROUPID")===ldcxref("LDF_GROUPID") and trim(rxOrderDcdrExport("LOCALDOSEFREQ"))===ldcxref("LDF_RAWVAL"), "left_outer")
                .join(ldxref, rxOrderDcdrExport("GROUPID")===ldxref("LD_GROUPID") and trim(rxOrderDcdrExport("LOCALDESCRIPTION_PHI"))===ldxref("LD_RAWVAL"), "left_outer")
                .join(lduxref, rxOrderDcdrExport("GROUPID")===lduxref("LDU_GROUPID") and trim(rxOrderDcdrExport("LOCALDOSEUNIT"))===lduxref("LDU_RAWVAL"), "left_outer")
                .join(lfxref, rxOrderDcdrExport("GROUPID")===lfxref("LF_GROUPID") and trim(rxOrderDcdrExport("LOCALFORM"))===lfxref("LF_RAWVAL"), "left_outer")
                .join(lqduxref, rxOrderDcdrExport("GROUPID")===lqduxref("LQU_GROUPID") and rxOrderDcdrExport("LOCALQTYOFDOSEUNIT")===lqduxref("LQU_RAWVAL"), "left_outer")
                .join(lspuxref, rxOrderDcdrExport("GROUPID")===lspuxref("LPU_GROUPID") and rxOrderDcdrExport("LOCALSTRENGTHPERDOSEUNIT")===lspuxref("LPU_RAWVAL"), "left_outer")
                .join(lsuxref, rxOrderDcdrExport("GROUPID")===lsuxref("LSU_GROUPID") and rxOrderDcdrExport("LOCALSTRENGTHUNIT")===lsuxref("LSU_RAWVAL"), "left_outer")
                .join(qpfxref, rxOrderDcdrExport("GROUPID")===qpfxref("QPF_GROUPID") and rxOrderDcdrExport("QUANTITYPERFILL")===qpfxref("QPF_RAWVAL"), "left_outer")
                .join(lrxref, rxOrderDcdrExport("GROUPID")===lrxref("RTE_GROUPID") and rxOrderDcdrExport("LOCALROUTE")===lrxref("RTE_RAWVAL"), "left_outer")
                .join(rmxref, rxOrderDcdrExport("GROUPID")===rmxref("RM_GROUPID") and rxOrderDcdrExport("DCC")===rmxref("RM_SIMPLE_GENERIC_C"), "left_outer")
                .join(zmmref, rxOrderDcdrExport("GROUPID")===zmmref("ZMM_GROUPID") and rxOrderDcdrExport("CLIENT_DS_ID")===zmmref("ZMM_CLIENT_DS_ID")and rxOrderDcdrExport("DATASRC")===zmmref("ZMM_DATASRC") and rxOrderDcdrExport("LMC_MAPPED_VAL")===zmmref("ZMM_MAPPEDLOCALMEDCODE"), "left_outer")
    }


    afterJoin = (df: DataFrame) => {
        df.filter("RM_SIMPLE_GENERIC_C IS NULL")
    }

    map = Map(
        "ORDERVSPRESCRIPTION" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("ORDERVSPRESCRIPTION")))}),
        "ENCOUNTERID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("E_HGENCID")))}),
        "GROUPID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("GROUPID")))}),
        "FACILITYID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("FACILITYID")))}),
        "LOCALMEDCODE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LMC_MAPPED_VAL")))}),
        "LOCALPROVIDERID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LOCALPROVIDERID")))}),
        "ALTMEDCODE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("ALTMEDCODE")))}),
        "ORDERTYPE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("ORDERTYPE")))}),
        "ORDERSTATUS" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("ORDERSTATUS")))}),
        "QUANTITYPERFILL" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("QPF_CLEANVAL")))}),
        "RXID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("T1_NEW_ID")))}),
        "LOCALSTRENGTHPERDOSEUNIT" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LPU_CLEANVAL")))}),
        "LOCALSTRENGTHUNIT" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LSU_CLEANVAL")))}),
        "LOCALROUTE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("RTE_CLEANVAL")))}),
        "MAPPEDNDC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("MAPPEDNDC")))}),
        "LOCALDESCRIPTION_PHI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LD_CLEANVAL")))}),
        "DATASRC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("DATASRC")))}),
        "VENUE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("VENUE")))}),
        "MAPPEDGPI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("MAPPEDGPI")))}),
        "HUM_MED_KEY" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("HUM_MED_KEY")))}),
        "MAP_USED" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("MAP_USED")))}),
        "LOCALQTYOFDOSEUNIT" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LQU_CLEANVAL")))}),
        "LOCALFORM" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LF_CLEANVAL")))}),
        "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  fIsPhoneNumber(df, "LOCALTOTALDOSE"))}),
        "LOCALINFUSIONVOLUME" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LOCALINFUSIONVOLUME")))}),
        "LOCALINFUSIONDURATION" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LOCALINFUSIONDURATION")))}),
        "LOCALNDC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LOCALNDC")))}),
        "LOCALGPI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LOCALGPI")))}),
        "LOCALDOSEUNIT" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LDU_CLEANVAL")))}),
        "LOCALDISCHARGEMEDFLG" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LOCALDISCHARGEMEDFLG")))}),
        "LOCALDAW" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LOCALDAW")))}),
        "HUM_GEN_MED_KEY" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("HUM_GEN_MED_KEY")))}),
        "MSTRPROVID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("MSTRPROVID")))}),
        "HTS_GENERIC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("HTS_GENERIC")))}),
        "HTS_GENERIC_EXT" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("HTS_GENERIC_EXT")))}),
        "DISCONTINUEREASON" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("DISCONTINUEREASON")))}),
        "DCC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("DCC")))}),
        "NDC11" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("NDC11")))}),
        "ROUTE_CUI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("ROUTE_CUI")))}),
        "DOSEFREQ_CUI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("DOSEFREQ_CUI")))}),
        "MAPPED_ORDERSTATUS" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("MAPPED_ORDERSTATUS")))}),
        "RXNORM_CODE" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("RXNORM_CODE")))}),
        "PREFERREDNDC" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("PREFERREDNDC")))}),
        "LOCALDOSEFREQ" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("LDF_CLEANVAL")))}),
        "CDR_GRP_MPI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("GRP_MPI")))}),
        "DCDR_GRP_MPI" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("G_NEW_ID")))}),
        "ORIGIN" -> ((col: String, df: DataFrame) => {df.withColumn(col,  trim(df("ORDERVSPRESCRIPTION")))}),
        "ORDERVSPRESCRIPTION" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("G_NEW_ID").isNotNull, "DCDR").when(df("GRP_MPI").isNotNull, "CDR")otherwise("SOURCE"))})
    )


    def fIsPhoneNumber(df:DataFrame, col:String ) :Column = {
        var column = df(col)
        var res = regexp_extract(column, "(^|[^0-9])1?([2-9][0-9]{2}(\\W|_){0,2}){2}[0-9]{4}($|[^0-9])", 0)
        when(trim(res)==="", column)
    }


}


//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")