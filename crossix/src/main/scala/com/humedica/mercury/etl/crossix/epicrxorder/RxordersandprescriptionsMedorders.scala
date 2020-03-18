package com.humedica.mercury.etl.crossix.epicrxorder

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Auto-generated on 02/01/2017
  */


class RxordersandprescriptionsMedorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("GRPID1","DATASRC", "FACILITYID", "RXID",
        "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID",
        "ISSUEDATE", "DISCONTINUEDATE", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE",
        "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE",
        "ORDERSTATUS", "ALTMEDCODE", "MAPPEDNDC", "MAPPEDGPI", "MAPPEDNDC_CONF",
        "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED",
        "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ", "LOCALDURATION",
        "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11",
        "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG", "LOCALDAW",
        "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT","CLSID1",
        "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON",
        "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE",
        "ACTIVE_MED_FLAG", "ROW_SOURCE", "MODIFIED_DATE", "UPDATE_DATE")


    tables = List("zh_med_unit",
        "medorders/*",
        "cdr.map_ordertype",
        "cdr.map_predicate_values",
        "shelf_manifest_entry_wclsid")

    columnSelect = Map(
        "zh_med_unit" -> List("DISP_QTYUNIT_C", "FILEID", "NAME"),
        "medorders/*" -> List("ORDER_STATUS_C", "AUTHRZING_PROV_ID", "MED_ROUTE_C", "DISCON_TIME",
            "RSN_FOR_DISCON_C", "ORDER_END_TIME", "ORDER_INST", "ORDERING_MODE_C",
            "PAT_ID", "ORDER_MED_ID", "PAT_ENC_CSN_ID",
            "DISP_AS_WRITTEN_YN", "HV_DISCR_FREQ_ID", "NAME", "FORM", "GENERIC_NAME",
            "GPI", "MEDICATION_ID", "MED_PRESC_PROV_ID", "STRENGTH",
            "REFILLS", "QUANTITY", "SIG", "UPDATE_DATE", "ORDER_CLASS_C", "ORDER_START_TIME", "ORDERING_DATE", "END_DATE", "ROUTE",
            "ORDERING_MODE", "HV_DOSE_UNIT_C","FILE_ID"),
        "shelf_manifest_entry_wclsid" -> List("GROUPID", "CLIENT_DATA_SRC_ID", "FILE_ID")

    )


    beforeJoin =
            Map("medorders/*" -> ((df: DataFrame) =>
            {
                val groups = Window.partitionBy(df("ORDER_MED_ID")).orderBy(df("UPDATE_DATE").desc)
                val addColumn = df.withColumn("rn", row_number.over(groups))
                val dd = addColumn.filter("rn = 1 AND (ORDER_STATUS_C <> '4' OR ORDER_STATUS_C is null) AND MEDICATION_ID <> '-1' AND MEDICATION_ID IS NOT NULL and PAT_ID is not null").drop("rn")
                dd.withColumn("LOCALORDERTYPECODE",
                    when(dd("ORDER_CLASS_C") === "3", dd("ORDER_CLASS_C"))
                            .when(isnull(dd("ORDERING_MODE_C")) && isnull(dd("ORDERING_MODE")),null)
                            .when(dd("ORDERING_MODE_C").isNotNull, dd("ORDERING_MODE_C"))
                            .when(dd("ORDERING_MODE").isNotNull, dd("ORDERING_MODE"))
                            .otherwise(null))
            }
                    ),
                "cdr.map_predicate_values" -> ((df: DataFrame) => {
                    val fil = df.filter(" DATA_SRC = 'MEDORDERS' " +
                            "AND entity = 'RXORDER' AND TABLE_NAME = 'MEDORDERS' AND COLUMN_NAME = 'ORDER_CLASS_C'")
                    fil.withColumnRenamed("GROUPID", "GRPID1_mpv").withColumnRenamed("CLIENT_DS_ID", "CLSID1_mpv").drop("DTS_VERSION")
                })
                //,
                //  "cdr.map_ordertype" -> includeIf("GROUPID='"+config(GROUP)+"'")
            )

    join = (dfs: Map[String, DataFrame]) => {
        var sme = dfs("shelf_manifest_entry_wclsid")
        var medorders = dfs("medorders/*")
        medorders = medorders.join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GROUPID","GRPID1_med").withColumnRenamed("CLIENT_DATA_SRC_ID","CLSID1_med")

        var zh_med_unit = dfs("zh_med_unit").withColumnRenamed("FILEID", "FILE_ID").withColumnRenamed("NAME", "ZHMU_NAME")
        zh_med_unit = zh_med_unit.join(sme, Seq("FILE_ID"), "inner").withColumnRenamed("GROUPID","GRPID1_zmu").withColumnRenamed("CLIENT_DATA_SRC_ID","CLSID1_zmu")

        var map_predicate_values = dfs("cdr.map_predicate_values")
        var map_ordertype = dfs("cdr.map_ordertype").withColumnRenamed("GROUPID","GRPID1_mot")
        medorders
                .join(zh_med_unit, medorders("GRPID1_med")===zh_med_unit("GRPID1_zmu") and medorders("CLSID1_med")===zh_med_unit("CLSID1_zmu")  and medorders("HV_DOSE_UNIT_C") === zh_med_unit("DISP_QTYUNIT_C"), "left_outer")
                .join(map_predicate_values, medorders("GRPID1_med")===map_predicate_values("GRPID1_mpv") and medorders("CLSID1_med")===map_predicate_values("CLSID1_mpv")  and medorders("ORDER_CLASS_C") === map_predicate_values("column_value"), "left_outer")
                .join(map_ordertype, medorders("GRPID1_med")===map_ordertype("GRPID1_mot") and map_ordertype("LOCALCODE") === medorders("LOCALORDERTYPECODE"), "left_outer")
    }


    map = Map(
        "DATASRC" -> literal("medorders"),
        "ISSUEDATE" -> cascadeFrom(Seq("ORDER_INST", "ORDER_START_TIME", "ORDERING_DATE")),
        "ORDERVSPRESCRIPTION" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("ORDERING_MODE_C") === "1", "P")
                    .when(df("ORDERING_MODE_C") === "2", "O").otherwise(null))
        }),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "RXID" -> mapFrom("ORDER_MED_ID"),
        "DISCONTINUEDATE" -> cascadeFrom(Seq("DISCON_TIME", "END_DATE")),
        "LOCALDOSEUNIT" -> ((col: String, df: DataFrame) => { df.withColumn(col, when(df("HV_DOSE_UNIT_C")!== "-1", df("ZHMU_NAME")))}),
        "DISCONTINUEREASON" -> ((col: String, df: DataFrame) => { df.withColumn(col, when(df("RSN_FOR_DISCON_C")!== "-1", concat_ws(".", df("CLSID1_med"),df("RSN_FOR_DISCON_C"))))}),
        "LOCALDAW" -> mapFrom("DISP_AS_WRITTEN_YN"),
        "LOCALDOSEFREQ" -> ((col: String, df: DataFrame) => { df.withColumn(col, concat_ws(".",df("CLSID1_med") ,  df("HV_DISCR_FREQ_ID")))}),
        "LOCALDESCRIPTION" -> mapFrom("NAME"),
        "LOCALFORM" -> mapFrom("FORM"),
        "LOCALGENERICDESC" -> mapFrom("GENERIC_NAME"),
        "LOCALGPI" -> mapFrom("GPI"),
        "LOCALMEDCODE" -> mapFrom("MEDICATION_ID"),
        "LOCALPROVIDERID" -> mapFrom("MED_PRESC_PROV_ID", nullIf = Seq("-1")),
        //   "LOCALTOTALDOSE" -> mapFrom("SIG"), - removed as EP does not support
        "LOCALROUTE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col,
                when(df("MED_ROUTE_C").isNotNull, concat_ws(".", df("CLSID1_med") ,df("MED_ROUTE_C"))).when(df("ROUTE").isNotNull, concat_ws(".", df("CLSID1_med") ,df("ROUTE"))))
        }),

        "LOCALSTRENGTHPERDOSEUNIT" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, regexp_extract(df("STRENGTH"), "[-\\.0-9]*", 0)
            )
        }),
        "LOCALSTRENGTHUNIT" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, regexp_extract(df("STRENGTH"), "[^-\\.0-9]+", 0)
            )
        }),
        "FILLNUM" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(safe_to_number(df("REFILLS")) leq 100, safe_to_number(df("REFILLS"))).otherwise(null))
        }),
        "ORDERSTATUS" -> ((col: String, df: DataFrame) => { df.withColumn(col, concat_ws(".", df("CLSID1_med") , df("ORDER_STATUS_C")))}),

        "QUANTITYPERFILL" ->  ((col: String, df: DataFrame) => {
            df.withColumn(col, substring(df("QUANTITY"), 1, 200))
        }),
        "SIGNATURE" -> mapFrom("SIG"),
        "VENUE" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("ORDERING_MODE_C") === "1", "1")
                    .when(df("ORDERING_MODE_C") === "2", "0").otherwise(null))
        }),
        "ORDERTYPE" -> mapFrom("CUI"),
        "GRPID1" -> mapFrom("GRPID1_med"),
        "CLSID1" -> mapFrom("CLSID1_med"),
        "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID")

    )


    afterMap = includeIf("COLUMN_VALUE is null AND COALESCE(ISSUEDATE,DISCONTINUEDATE) is not null and RXID is not null")

    def safe_to_number(value:Column)={
        try {
            value.cast("Integer")
            value
        }
        catch {
            case ex:Exception => null
        }
    }


    mapExceptions = Map(
        ("H458934_EP2", "LOCALPROVIDERID") -> mapFrom("AUTHRZING_PROV_ID", nullIf=Seq("-1")),
        ("H846629_EP2", "DISCONTINUEDATE") -> cascadeFrom(Seq("ORDER_END_TIME", "DISCON_TIME"))
    )


    afterMapExceptions = Map(
        ("H262866_EP2", (df: DataFrame) => {
            includeIf("COALESCE(ISSUEDATE,DISCONTINUEDATE) is not null and RXID is not null")(df)
        })
    )

}


//  build(new RxordersandprescriptionsMedorders(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID1", "CLIENT_DS_ID").distinct.write.parquet(cfg("EMR_DATA_ROOT")+"/EPRXORDER")
//        writeGenerated(Toolkit.build(new RxordersandprescriptionsMedorders(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID1", "CLIENT_DS_ID"), dateStr, "EPRXORDER", cfg)

