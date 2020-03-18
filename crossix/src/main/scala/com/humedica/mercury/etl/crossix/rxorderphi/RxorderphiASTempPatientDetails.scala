package com.humedica.mercury.etl.crossix.rxorderphi

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderphiASTempPatientDetails(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")

    tables = List("as_patdem", "cdr.patient_phi")

    columnSelect = Map(
        "as_patdem" -> List(),
        "cdr.patient_mpi" -> List("PATIENTID")
    )


    beforeJoin = Map(
        "as_patdem" -> ((df: DataFrame) => {
            df.filter(((df("PATIENT_LAST_NAME").rlike(".*%.*") and df("PATIENT_FIRST_NAME").isNull) or
                    (df("PATIENT_LAST_NAME").rlike("^TEST |TEST$|ZZTEST") or df("PATIENT_FIRST_NAME").rlike("TEST")
                            or upper(df("PATIENT_FIRST_NAME")) === "TEST") === false) and length(df("PATIENT_MRN")) > 2)
                    .withColumn("ETH_COL",
                        when(df("ETHNICITY").isNotNull.and(df("ETHNICITYDE") !== 0), concat_ws("", df("CLIENT_DS_ID"), df("ETHNICITYDE")))
                                .when(df("RACE_ID").isNotNull.and(df("RACE_ID") !== "0"), concat_ws("", df("CLIENT_DS_ID"), df("RACE_ID"))).otherwise("NULL")
                    )
        })

    )


    def tempgrp(FILTER_PATDEM1: DataFrame, LOCALVALUE: Column, PATIENTDETAILTYPE: String) = {
        var PATIENT_DETAIL_ZIP = FILTER_PATDEM1.withColumn("FACILITYID", lit("NULL")).withColumn("ENCOUNTERID", lit("NULL"))
                .withColumn("PATIENTID", FILTER_PATDEM1("PATIENT_MRN")).withColumn("DATEOFBIRTH", FILTER_PATDEM1("PATIENT_DATE_OF_BIRTH"))
                .withColumn("LOCALVALUE", LOCALVALUE).withColumn("PATIENTDETAILTYPE", lit(PATIENTDETAILTYPE))
                .withColumn("DATASRC", lit("patdem")).withColumnRenamed("LAST_UPDATED_DATE", "PATDETAIL_TIMESTAMP")
        PATIENT_DETAIL_ZIP = PATIENT_DETAIL_ZIP.select("FACILITYID", "ENCOUNTERID", "PATIENTID", "DATEOFBIRTH", "LOCALVALUE",
            "PATIENTDETAILTYPE", "PATDETAIL_TIMESTAMP", "DATASRC")
        val group1 = Window.partitionBy(PATIENT_DETAIL_ZIP("PATIENTID"), PATIENT_DETAIL_ZIP("LOCALVALUE")).orderBy(PATIENT_DETAIL_ZIP("PATDETAIL_TIMESTAMP").desc)
        var TEMP_PATIENT_3 = PATIENT_DETAIL_ZIP.withColumn("ROW_NUMBER", row_number().over(group1))
        TEMP_PATIENT_3 = TEMP_PATIENT_3.filter((TEMP_PATIENT_3("ROW_NUMBER") === 1).and(TEMP_PATIENT_3("LOCALVALUE").isNotNull))
        TEMP_PATIENT_3 = TEMP_PATIENT_3.withColumn("PATIENTDETAILQUAL", lit("NULL")).withColumn("HGPID", lit("NULL")).withColumn("GRP_MPI", lit("NULL"))
        TEMP_PATIENT_3.select("DATASRC", "FACILITYID", "LOCALVALUE", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATDETAIL_TIMESTAMP", "DATEOFBIRTH")
    }

    join = (dfs: Map[String, DataFrame]) => {

        var PATDEM = dfs("as_patdem")
        var PATIENT_MPI1 = dfs("cdr.patient_phi")

        var FILTER_PATDEM = PATDEM.filter((PATDEM("PATIENT_LAST_NAME").rlike(".*%.*") and PATDEM("PATIENT_FIRST_NAME").isNull) or
                (PATDEM("PATIENT_LAST_NAME").rlike("^TEST |TEST$|ZZTEST") or PATDEM("PATIENT_FIRST_NAME").rlike("TEST")
                        or upper(PATDEM("PATIENT_FIRST_NAME")) === "TEST")===false)

        var FILTER_PATDEM1 = PATDEM.filter(( (PATDEM("PATIENT_LAST_NAME").rlike(".*%.*") and PATDEM("PATIENT_FIRST_NAME").isNull) or
                (PATDEM("PATIENT_LAST_NAME").rlike("^TEST |TEST$|ZZTEST") or PATDEM("PATIENT_FIRST_NAME").rlike("TEST")
                        or upper(PATDEM("PATIENT_FIRST_NAME")) === "TEST")===false) and length(PATDEM("PATIENT_MRN")) > 2)
        FILTER_PATDEM1 = FILTER_PATDEM1.withColumn("COLUMN_VALUE", concat_ws("",FILTER_PATDEM1("PATIENT_SSN"), FILTER_PATDEM1("PATIENT_NOTE2")))

        FILTER_PATDEM1 = FILTER_PATDEM1.join(PATIENT_MPI1, FILTER_PATDEM1("PATIENT_MRN")===PATIENT_MPI1("PATIENTID"), "left_outer")

        var TEMP_PAT_DET_FST_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_FIRST_NAME"), "FIRST_NAME")
        var TEMP_PAT_DET_LST_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_LAST_NAME"), "LAST_NAME")
        var TEMP_PAT_DET_CITY = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_CITY"), "CITY")
        var TEMP_PAT_DET_STATE = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_STATE"), "STATE")
        var TEMP_PAT_DET_ZIP = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_ZIP"), "ZIPCODE")
        var TEMP_PAT_DET_GENDER = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("GENDER_ID"), "GENDER")

        var TEMP_PAT_DET_RACE = tempgrp(FILTER_PATDEM1, concat_ws("", FILTER_PATDEM1("CLIENT_DS_ID"), FILTER_PATDEM1("RACE_ID")), "RACE")

        var ETH_COL = when(FILTER_PATDEM1("ETHNICITY").isNotNull.and(FILTER_PATDEM1("ETHNICITYDE") !== 0), concat_ws("",FILTER_PATDEM1("CLIENT_DS_ID_pat"), FILTER_PATDEM1("ETHNICITYDE")))
                .when(FILTER_PATDEM1("RACE_ID").isNotNull.and(FILTER_PATDEM1("RACE_ID") !== "0"), concat_ws("",FILTER_PATDEM1("CLIENT_DS_ID_pat"), FILTER_PATDEM1("RACE_ID"))).otherwise("NULL")
        var TEMP_PAT_DET_ETHNICITY = tempgrp(FILTER_PATDEM1, ETH_COL, "ETHNICITY")
        var TEMP_PAT_DET_DECEASED = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("DEATH_FLAG_INDICATOR"), "DECEASED")
        var TEMP_PAT_DET_LANGUAGE = tempgrp(FILTER_PATDEM1, concat_ws("", FILTER_PATDEM1("CLIENT_DS_ID_pat"),FILTER_PATDEM1("PRIMARY_LANGUAGE_ID")), "LANGUAGE")
        var TEMP_PAT_DET_MID_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_MIDDLE_NAME"), "MIDDLE_NAME")
        var TEMP_PAT_DET_MARITAL = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("MARITAL_STATUS_ID"), "MARITAL")
        FILTER_PATDEM1 = FILTER_PATDEM1.withColumnRenamed("PATIENTID", "PATIENTID_pat")
        FILTER_PATDEM1 = FILTER_PATDEM1.join(PATIENT_MPI1,Seq("PATIENTID"), "left_outer")

        var TEMP_PATIENTDETAIL = TEMP_PAT_DET_MARITAL.unionAll(TEMP_PAT_DET_MID_NAME).distinct().unionAll(TEMP_PAT_DET_LANGUAGE).distinct().unionAll(TEMP_PAT_DET_DECEASED).distinct()
                .unionAll(TEMP_PAT_DET_ETHNICITY).distinct().unionAll(TEMP_PAT_DET_RACE).distinct().unionAll(TEMP_PAT_DET_GENDER).distinct()
                .unionAll(TEMP_PAT_DET_ZIP).distinct().unionAll(TEMP_PAT_DET_STATE).distinct().unionAll(TEMP_PAT_DET_CITY).distinct()
                .unionAll(TEMP_PAT_DET_LST_NAME).distinct().unionAll(TEMP_PAT_DET_FST_NAME).distinct()
        TEMP_PATIENTDETAIL.join(PATIENT_MPI1, Seq("PATIENTID"), "left_outer")
    }

    map = Map(
        "PATIENTDETAILQUAL" -> literal("NULL"),
        "FACILITYID" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("FACILITYID") !== "NULL", df("FACILITYID")))
        }),
        "DATEOFBIRTH" -> ((col: String, df: DataFrame) => {
            df.withColumn(col, when(df("DATEOFBIRTH") !== "NULL", df("DATEOFBIRTH")))
        }),
        "HGPID" -> mapFrom("HGPID_pmi"),
        "GRP_MPI" -> mapFrom("GRP_MPI_pmi"),
        "GROUPID" -> mapFrom("GROUPID"),
        "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID")
    )
}


//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")