package com.humedica.mercury.etl.crossix.rxorderphi

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}


/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderphiASTempPatientId(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "HGPID", "GRP_MPI", "ID_SUBTYPE")

    tables = List("as_patdem", "cdr.patient_phi")

    columnSelect = Map(
        "as_patdem" -> List("GROUPID"),
        "cdr.patient_mpi" -> List("GROUPID","CLIENT_DS_ID","PATIENTID","HGPID","GRP_MPI")
    )

    def tempgrp(FILTER_PATDEM1:DataFrame, LOCALVALUE:Column, PATIENTDETAILTYPE:String) = {
        var PATIENT_DETAIL_ZIP = FILTER_PATDEM1.withColumn("FACILITYID", lit("NULL")).withColumn("ENCOUNTERID", lit("NULL"))
                .withColumn("PATIENTID", FILTER_PATDEM1("PATIENT_MRN")).withColumn("DATEOFBIRTH", FILTER_PATDEM1("PATIENT_DATE_OF_BIRTH"))
                .withColumn("LOCALVALUE",LOCALVALUE).withColumn("PATIENTDETAILTYPE", lit(PATIENTDETAILTYPE))
                .withColumn("DATASRC", lit("patdem")).withColumnRenamed("LAST_UPDATED_DATE", "PATDETAIL_TIMESTAMP")
        PATIENT_DETAIL_ZIP = PATIENT_DETAIL_ZIP.select("FACILITYID", "ENCOUNTERID", "PATIENTID", "DATEOFBIRTH","LOCALVALUE",
            "PATIENTDETAILTYPE", "PATDETAIL_TIMESTAMP", "DATASRC")
        val group1 = Window.partitionBy(PATIENT_DETAIL_ZIP("PATIENTID"), PATIENT_DETAIL_ZIP("LOCALVALUE")).orderBy(PATIENT_DETAIL_ZIP("PATDETAIL_TIMESTAMP").desc)
        var TEMP_PATIENT_3 = PATIENT_DETAIL_ZIP.withColumn("ROW_NUMBER", row_number().over(group1))
        TEMP_PATIENT_3 = TEMP_PATIENT_3.filter((TEMP_PATIENT_3("ROW_NUMBER")===1).and(TEMP_PATIENT_3("LOCALVALUE").isNotNull))
        TEMP_PATIENT_3 = TEMP_PATIENT_3.withColumn("PATIENTDETAILQUAL", lit("NULL")).withColumn("HGPID", lit("NULL")).withColumn("GRP_MPI", lit("NULL"))
        TEMP_PATIENT_3.select("DATASRC", "FACILITYID", "LOCALVALUE", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATDETAIL_TIMESTAMP","DATEOFBIRTH")
    }


    join = (dfs: Map[String, DataFrame])  => {

        var PATDEM = dfs("as_patdem")
        var PATIENT_MPI1 = dfs("cdr.patient_phi")

        var FILTER_PATDEM = PATDEM.filter((PATDEM("PATIENT_LAST_NAME").rlike(".*%.*") and PATDEM("PATIENT_FIRST_NAME").isNull) or
                (PATDEM("PATIENT_LAST_NAME").rlike("^TEST |TEST$|ZZTEST") or PATDEM("PATIENT_FIRST_NAME").rlike("TEST")
                        or upper(PATDEM("PATIENT_FIRST_NAME")) === "TEST")===false)

        var FILTER_PATDEM1 = PATDEM.filter(( (PATDEM("PATIENT_LAST_NAME").rlike(".*%.*") and PATDEM("PATIENT_FIRST_NAME").isNull) or
                (PATDEM("PATIENT_LAST_NAME").rlike("^TEST |TEST$|ZZTEST") or PATDEM("PATIENT_FIRST_NAME").rlike("TEST")
                        or upper(PATDEM("PATIENT_FIRST_NAME")) === "TEST")===false) and length(PATDEM("PATIENT_MRN")) > 2)
        FILTER_PATDEM1 = FILTER_PATDEM1.withColumn("COLUMN_VALUE", concat_ws("",FILTER_PATDEM1("PATIENT_SSN"), FILTER_PATDEM1("PATIENT_NOTE2")))

        FILTER_PATDEM1 = FILTER_PATDEM1.join(PATIENT_MPI1, Seq("PATIENTID"), "left_outer")

        var TEMP_PAT_DET_FST_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_FIRST_NAME"), "FIRST_NAME")
        var TEMP_PAT_DET_LST_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_LAST_NAME"), "LAST_NAME")
        var TEMP_PAT_DET_CITY = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_CITY"), "CITY")
        var TEMP_PAT_DET_STATE = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_STATE"), "STATE")
        var TEMP_PAT_DET_ZIP = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_ZIP"), "ZIPCODE")
        var TEMP_PAT_DET_GENDER = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("GENDER_ID"), "GENDER")

        var TEMP_PAT_DET_RACE = tempgrp(FILTER_PATDEM1, concat_ws("", FILTER_PATDEM1("CLIENT_DS_ID_pat"), FILTER_PATDEM1("RACE_ID")), "RACE")

        var ETH_COL = when(FILTER_PATDEM1("ETHNICITY").isNotNull.and(FILTER_PATDEM1("ETHNICITYDE") !== 0), concat_ws("",FILTER_PATDEM1("CLIENT_DS_ID_pat"), FILTER_PATDEM1("ETHNICITYDE")))
                .when(FILTER_PATDEM1("RACE_ID").isNotNull.and(FILTER_PATDEM1("RACE_ID") !== "0"), concat_ws("",FILTER_PATDEM1("CLIENT_DS_ID_pat"), FILTER_PATDEM1("RACE_ID"))).otherwise("NULL")
        var TEMP_PAT_DET_ETHNICITY = tempgrp(FILTER_PATDEM1, ETH_COL, "ETHNICITY")
        var TEMP_PAT_DET_DECEASED = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("DEATH_FLAG_INDICATOR"), "DECEASED")
        var TEMP_PAT_DET_LANGUAGE = tempgrp(FILTER_PATDEM1, concat_ws("", FILTER_PATDEM1("CLIENT_DS_ID_pat"),FILTER_PATDEM1("PRIMARY_LANGUAGE_ID")), "LANGUAGE")
        var TEMP_PAT_DET_MID_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_MIDDLE_NAME"), "MIDDLE_NAME")
        var TEMP_PAT_DET_MARITAL = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("MARITAL_STATUS_ID"), "MARITAL")
        FILTER_PATDEM1 = FILTER_PATDEM1.join(PATIENT_MPI1, Seq("PATIENTID"), "left_outer")

        var TEMP_PATIENT_ID1 = FILTER_PATDEM1.withColumn("DATASRC", lit("patdem")).withColumn("PATIENTID", FILTER_PATDEM1("PATIENT_MRN")).withColumn("IDTYPE", lit("PATIENT_SSN")).withColumnRenamed("COLUMN_VALUE", "SSN").withColumn("ID_SUBTYPE", lit("NULL"))

        var group2 = Window.partitionBy(TEMP_PATIENT_ID1("PATIENTID")).orderBy(TEMP_PATIENT_ID1("LAST_UPDATED_DATE").desc)
        var FIL_PATIENT_ID_SSN = TEMP_PATIENT_ID1.withColumn("ROW_NUMBER", row_number().over(group2))
        FIL_PATIENT_ID_SSN = FIL_PATIENT_ID_SSN.filter((FIL_PATIENT_ID_SSN("ROW_NUMBER")===1).and(FIL_PATIENT_ID_SSN("SSN").isNotNull))


        var TEMP_PATIENT_ID =FIL_PATIENT_ID_SSN.withColumn("IDVALUE", lit("NULL")).withColumn("FACILITYID", lit("NULL"))
        TEMP_PATIENT_ID = TEMP_PATIENT_ID.select("PATIENTID_pat", "DATASRC", "IDTYPE", "IDVALUE", "HGPID", "GRP_MPI", "ID_SUBTYPE", "FACILITYID").distinct()
        PATIENT_MPI1 = PATIENT_MPI1.withColumnRenamed("HGPID","HGPID_pmi").withColumnRenamed("GRP_MPI","GRP_MPI_pmi")
        TEMP_PATIENT_ID.join(PATIENT_MPI1, TEMP_PATIENT_ID("PATIENTID_pat")===PATIENT_MPI1("PATIENTID"), "left_outer")
    }

    map = Map(
        "GROUPID" -> mapFrom("GROUPID"),
        "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID")
    )
}

//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")