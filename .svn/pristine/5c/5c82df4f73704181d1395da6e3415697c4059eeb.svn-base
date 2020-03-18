package com.humedica.mercury.etl.crossix.asrxorder

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
  * Created by bhenriksen on 5/17/17.
  */
class RxordersandprescriptionsErx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

//Rename GRPID1 --> GROUPID and DATA_STREAM_ID--> CLIENT_DS_ID

    columns = List("GRPID1","DATASRC", "FACILITYID", "RXID",
        "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID",
        "ISSUEDATE", "DISCONTINUEDATE", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE",
        "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE",
        "ORDERSTATUS", "ALTMEDCODE", "MAPPEDNDC", "MAPPEDGPI", "MAPPEDNDC_CONF",
        "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED",
        "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ", "LOCALDURATION",
        "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11",
        "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",  "LOCALDAW",
        "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLSID",
        "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON",
        "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE",
        "ACTIVE_MED_FLAG", "ROW_SOURCE", "MODIFIED_DATE")


    tables = List("as_erx/*",
        "as_zc_medication_de", "shelf_manifest_entry_wclsid")


    columnSelect = Map(
        "as_erx/*" -> List("THERAPY_END_DATE", "PRESCRIBEDBYID", "FREQUENCY_UNITS_ID", "ORDER_DATE",
            "PATIENT_MRN", "CHILD_MEDICATION_ID", "PARENT_MEDICATION_ID", "MEDICATION_NDC", "DAYS_SUPPLY", "THERAPY_START_DATE",
            "ENCOUNTER_ID", "EXP_DATE", "SITE_ID", "DAW_FLAG", "FREQUENCY_UNITS_ID",
            "DRUG_FORM", "DRUG_DESCRIPTION", "DAYS_TO_TAKE", "DRUG_FORM", "MED_DICT_DE",
            "ORDERED_BY_ID", "DOSE", "DRUG_STRENGTH", "UNITS_OF_MEASURE", "DOSE",
            "REFILLS", "ORDER_STATUS_ID", "QTY_TO_DISPENSE", "FREE_TEXT_SIG", "LAST_UPDATED_DATE", "PRESCRIBE_ACTION_ID", "MANAGED_BY_ID", "FILE_ID"),
        "as_zc_medication_de" -> List("GPI_TC3", "NDC", "ROUTEOFADMINDE", "ID", "FILEID"),
        "shelf_manifest_entry_wclsid" -> List("GROUPID", "CLIENT_DATA_SRC_ID", "FILE_ID")
    )


    join = (dfs: Map[String, DataFrame])  => {
        var sme = dfs("shelf_manifest_entry_wclsid").withColumnRenamed("GROUPID","GRPID1").withColumnRenamed("CLIENT_DATA_SRC_ID","CLSID")
        var erx = dfs("as_erx/*").withColumnRenamed("FILEID", "FILE_ID")
        var as_zc_medication_de = dfs("as_zc_medication_de").withColumnRenamed("FILEID", "FILE_ID")
        erx = erx.join(sme, Seq("FILE_ID"), "inner").drop("FILE_ID").withColumnRenamed("GRPID1","GRPID1_erx").withColumnRenamed("CLSID","CLSID1_erx")
        as_zc_medication_de = as_zc_medication_de.join(sme , Seq("FILE_ID"), "inner").withColumnRenamed("GRPID1","GRPID1_AS").withColumnRenamed("CLSID","CLSID_AS").drop("FILE_ID")

        erx.join(as_zc_medication_de, as_zc_medication_de("ID") === erx("MED_DICT_DE") and as_zc_medication_de("GRPID1_AS") === erx("GRPID1_erx") and as_zc_medication_de("CLSID_AS") === erx("CLSID1_erx"), "left_outer").distinct()
    }

    afterJoin = (df: DataFrame) => {
        val fil = df.filter("ORDER_STATUS_ID not in ('5', '10') or ORDER_STATUS_ID is null")
        val groups = Window.partitionBy(fil("CHILD_MEDICATION_ID")).orderBy(fil("LAST_UPDATED_DATE").desc)
        val groups1 = Window.partitionBy(fil("CHILD_MEDICATION_ID")).orderBy(fil("ORDER_STATUS_ID").desc, fil("LAST_UPDATED_DATE").desc)
        val groups2 = Window.partitionBy(fil("PARENT_MEDICATION_ID"))
        val addcolumn = fil.withColumn("rn", row_number.over(groups))
                .withColumn("LASTKNOWNSTATUS", first("ORDER_STATUS_ID").over(groups1))
                .withColumn("ENTERED_IN_ERROR", sum(when(df("ORDER_STATUS_ID") === "5", 1).otherwise(0)).over(groups2))
                .withColumn("PRESCRIBE_ACTION_FLAG", sum(when(df("PRESCRIBE_ACTION_ID") === "9", 0).otherwise(1)).over(groups2))
                .withColumn("ORDER_STATUS_FLAG", min(when(df("ORDER_STATUS_ID").isin("18","19"), 0).otherwise(1)).over(groups2))
        addcolumn.filter("rn = '1'")
    }



    map = Map(
        "DATASRC" -> literal("erx"),
        "PATIENTID" -> mapFrom("PATIENT_MRN"),
        "ENCOUNTERID" -> mapFrom("ENCOUNTER_ID"),
        "FACILITYID" -> mapFrom("SITE_ID"),
        "RXID" -> mapFrom("CHILD_MEDICATION_ID"),
        "LOCALMEDCODE" -> mapFrom("MED_DICT_DE"),
        "LOCALDESCRIPTION" -> mapFrom("DRUG_DESCRIPTION"),
        "ALTMEDCODE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, trim(df("MEDICATION_NDC")))
        }),
        "LOCALSTRENGTHPERDOSEUNIT" -> mapFrom("DRUG_STRENGTH"),
        "LOCALSTRENGTHUNIT" -> mapFrom("UNITS_OF_MEASURE"),
        "LOCALROUTE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("ROUTEOFADMINDE")!=="0", df("ROUTEOFADMINDE")))
        }),
        "FILLNUM" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("REFILLS").rlike("^[+-]?\\d+\\.?\\d*$"), df("REFILLS")).otherwise(9999))
        }),
        "QUANTITYPERFILL" -> mapFrom("QTY_TO_DISPENSE"),
        "LOCALPROVIDERID" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, coalesce(when(df("ORDERED_BY_ID") === 0, null).otherwise(df("ORDERED_BY_ID")),
                when(df("MANAGED_BY_ID") === 0, null).otherwise(df("MANAGED_BY_ID"))))
        }),
        "ISSUEDATE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(coalesce(df("ORDER_STATUS_ID"), df("LASTKNOWNSTATUS"), lit("15")) === "15",
                when(df("PRESCRIBE_ACTION_ID").isin("2", "4", "12", "8", "15", "19", "9"), df("ORDER_DATE"))
                        .otherwise(coalesce(df("THERAPY_START_DATE"), df("ORDER_DATE"))))
                    .otherwise(df("THERAPY_START_DATE")))
        }),
        "SIGNATURE" -> mapFrom("FREE_TEXT_SIG"),
        "ORDERTYPE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col,
                when(df("PRESCRIBE_ACTION_FLAG") === "0", "CH002046")
                        .when(df("ORDER_STATUS_FLAG") === "0", "CH002045")
                        .otherwise("CH002047"))
        }),
        "ORDERSTATUS" -> mapFrom("ORDER_STATUS_ID"),
        "VENUE" -> literal("1"),
        "LOCALDAYSUPPLIED" -> mapFrom("DAYS_SUPPLY"),
        "LOCALFORM" -> mapFrom("DRUG_FORM"),
        "LOCALQTYOFDOSEUNIT" -> mapFrom("DOSE"),
        "LOCALTOTALDOSE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("DOSE").rlike("^[+-]?\\d+\\.?\\d*$"), df("DOSE")).otherwise(null)
                    .multiply(when(df("DRUG_STRENGTH").rlike("^[+-]?\\d+\\.?\\d*$"), df("DRUG_STRENGTH")).otherwise(null)))}
                ),
        "LOCALDOSEFREQ" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("FREQUENCY_UNITS_ID")!=="0", concat_ws(".", df("CLSID1_erx"), df("FREQUENCY_UNITS_ID"))))
        }),
    "LOCALDURATION" -> mapFrom("DAYS_TO_TAKE"),
        "LOCALNDC" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, trim(when(df("NDC").isNotNull, substring(regexp_replace(df("NDC"), "-", ""),1,11))
                    .otherwise(substring(regexp_replace(df("MEDICATION_NDC"), "-", ""),1,11))))
        }),
        "LOCALGPI" -> mapFrom("GPI_TC3"),
        "LOCALDAW" -> mapFrom("DAW_FLAG"),
        "ORDERVSPRESCRIPTION" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("ORDER_STATUS_ID").isin("18","19"), "O").otherwise("P"))
        }),
        "LOCALDOSEUNIT" -> mapFrom("DRUG_FORM"),
        "GRPID1" -> mapFrom("GRPID1_erx"),
        "CLSID" -> mapFrom("CLSID1_erx")
    )



    afterMap = (df: DataFrame) => {
        val addColumn = df.withColumn("DISCONTINUEDATE", when(df("THERAPY_END_DATE") lt df("ISSUEDATE"), null).otherwise(df("THERAPY_END_DATE")))
                .withColumn("EXPIREDATE", when(df("EXP_DATE") lt df("ISSUEDATE"), null).otherwise(df("EXP_DATE")))
        addColumn.filter("ISSUEDATE is not null")
    }


    mapExceptions = Map(
        ("H232796_AS ENT", "DISCONTINUEDATE") -> cascadeFrom(Seq("THERAPY_END_DATE", "EXP_DATE")),
        ("H232796_AS ENT", "LOCALPROVIDERID") -> mapFrom("PRESCRIBEDBYID"),
        ("H984474_AS_ENT_ALMOB", "LOCALDOSEFREQ") -> todo("FREQUENCY_UNITS_ID")      //TODO - to be coded
    )

}


//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")