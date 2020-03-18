package com.humedica.mercury.etl.crossix.rxorderphi


import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderphiEP2TempPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("DATASRC", "PATIENTID", "FACILITYID","DATEOFBIRTH","MEDICALRECORDNUMBER","HGPID", "DATEOFDEATH", "CLIENT_DS_ID", "GRP_MPI" , "INACTIVE_FLAG")

    tables = List("patreg", "cdr.map_predicate_values", "identify_id", "pat_race", "patient_3", "patient_fyi_flags", "zh_state", "cdr.patient_mpi")

    columnSelect = Map(
        "patreg" -> List("FILE_ID", "CLIENT_DS_ID_pat", "PAT_ID", "PAT_MRN_ID", "BIRTH_DATE", "DEATH_DATE", "PAT_STATUS_C",
            "MARITAL_STATUS_C", "PAT_MIDDLE_NAME", "ETHNIC_GROUP_C", "PAT_FIRST_NAME", "PAT_LAST_NAME",
            "SEX_C", "CITY", "STATE_C", "ZIP", "SSN", "LANGUAGE_C", "UPDATE_DATE"),
        "identify_id" -> List(),
        "pat_race" -> List("FILE_ID", "PATIENT_RACE_C", "PAT_ID", "UPDATE_DATE"),
        "patient_3" -> List("FILE_ID",   "IS_TEST_PAT_YN", "PAT_ID", "UPDATE_DATE"),
        "patient_fyi_flags" -> List("FILE_ID",  "PAT_FLAG_TYPE_C","PATIENT_ID"),
        "zh_state" -> List(),
        "cdr.patient_mpi" -> List("CLIENT_DS_ID","PATIENTID","HGPID","GRP_MPI")

    )


    def predicate_value_list(p_mpv: DataFrame, dataSrc: String, entity: String, table: String, column: String, colName: String): DataFrame = {
        var mpv1 = p_mpv.filter(p_mpv("DATA_SRC").equalTo(dataSrc).and(p_mpv("ENTITY").equalTo(entity)).and(p_mpv("TABLE_NAME").equalTo(table)).and(p_mpv("COLUMN_NAME").equalTo(column)))
        mpv1=mpv1.withColumn(colName, mpv1("COLUMN_VALUE"))
        mpv1.select(colName).orderBy(mpv1("DTS_VERSION").desc).distinct()
    }

    def STANDARDIZE_POSTAL_CODE(colm : Column) = {
        var count: Int = 5
        regexp_replace(when (length(colm) < 5, colm).otherwise(trim(colm).substr(0, count)), "O", "0")
    }


    join = (dfs: Map[String, DataFrame])  => {
        var PATREG = dfs("patreg")

        var PA = PATREG.withColumnRenamed("UPDATE_DATE", "PA_UPDATE_DATE")
        var MPV = dfs("cdr.map_predicate_values")

        var VAL1 = predicate_value_list(MPV, "IDENTITY_ID", "PATIENT", "IDENTITY_ID", "IDENTITY_TYPE_ID", "COLUMN_VALUE")
        var VAL2 = predicate_value_list(MPV, "IDENTITY_ID", "PATIENT_ID", "IDENTITY_ID", "IDENTITY_TYPE_ID", "COLUMN_VALUE")
        var VAL = VAL1.unionAll(VAL2)
        var ID_SUBTYPE = VAL.withColumn("IDENTITY_TYPE_ID", VAL("COLUMN_VALUE").substr(1,1)).withColumn("IDENTITY_SUBTYPE_ID", VAL("COLUMN_VALUE").substr(1,1))
        var IDENTITY_ID = dfs("identify_id")
        IDENTITY_ID = IDENTITY_ID.withColumnRenamed("IDENTITY_TYPE_ID", "IDENTITY_TYPE_ID1")
        var JOIN_ID = IDENTITY_ID.join(ID_SUBTYPE, IDENTITY_ID("IDENTITY_TYPE_ID1")===ID_SUBTYPE("IDENTITY_TYPE_ID"), "left_outer")
        var ID = JOIN_ID.select("IDENTITY_TYPE_ID","IDENTITY_ID", "PAT_ID", "IDENTITY_SUBTYPE_ID").distinct()

        var PAT_RACE = dfs("pat_race").withColumnRenamed("UPDATE_DATE","RA_UPDATE_DATE")

        val group1 = Window.partitionBy(PAT_RACE("PAT_ID")).orderBy(PAT_RACE("RA_UPDATE_DATE").desc)
        var PAT_RACE1 = PAT_RACE.withColumn("RA_RN", row_number().over(group1))

        var PATIENT_3 = dfs("patient_3")

        val group2 = Window.partitionBy(PATIENT_3("PAT_ID")).orderBy(PATIENT_3("UPDATE_DATE").desc)
        var PAT3_D = PATIENT_3.withColumn("P3_RNBR", row_number().over(group2)).filter("P3_RNBR == 1")

        var PATIENT_FYI_FLAGS = dfs("patient_fyi_flags")

        var ZH_STATE = dfs("zh_state").withColumnRenamed("NAME", "ZH_STATE_NAME")
        PA = PA.withColumnRenamed("PAT_ID", "PAT_ID_PA")
        ID = ID.withColumnRenamed("PAT_ID", "PAT_ID_ID")
        PAT_RACE1 = PAT_RACE1.withColumnRenamed("PAT_ID", "PAT_ID_PR")
        PAT3_D = PAT3_D.withColumnRenamed("PAT_ID", "PAT_ID_PR")

        var PA_ID = PA.join(ID, PA("PAT_ID_PA")===ID("PAT_ID_ID"), "left_outer").drop("PAT_ID_ID")
        var PA_ID_RA = PA_ID.join(PAT_RACE1, PA_ID("PAT_ID_PA")===PAT_RACE1("PAT_ID_PR"), "left_outer")
        var PA_ID_RA_PA3 = PA_ID_RA.join(PAT3_D,  PA_ID("PAT_ID_PA")===PAT3_D("PAT_ID_PR"), "left_outer")
        var PA_PATFLAG = PA_ID_RA_PA3.join(PATIENT_FYI_FLAGS, PA_ID_RA_PA3("PAT_ID_PA")===PATIENT_FYI_FLAGS("PATIENT_ID"), "left_outer")
        var PA_PATFLAG_ZH = PA_PATFLAG.join(ZH_STATE, PA_PATFLAG("STATE_C")===ZH_STATE("STATE_C") , "left_outer")
        PA_PATFLAG_ZH = PA_PATFLAG_ZH.withColumnRenamed("PAT_ID_PA", "PAT_ID")
        var TEMP_EPIC_PATS_GRPID = PA_PATFLAG_ZH.withColumn("PATIENTID", PA_PATFLAG_ZH("PAT_ID"))
                .withColumn("MEDICALRECORDNUMBER", PA_PATFLAG_ZH("IDENTITY_ID"))
                .withColumn("MRN_ID", PA_PATFLAG_ZH("IDENTITY_ID"))
                .withColumn("DATEOFBIRTH", PA_PATFLAG_ZH("BIRTH_DATE"))
                .withColumn("DATEOFDEATH", PA_PATFLAG_ZH("DEATH_DATE"))
                .withColumn("PAT_STATUS", PA_PATFLAG_ZH("PAT_STATUS_C"))
                .withColumn("MARITAL_STATUS", PA_PATFLAG_ZH("MARITAL_STATUS_C"))
                .withColumn("MIDDLE_NAME", PA_PATFLAG_ZH("PAT_MIDDLE_NAME"))
                .withColumn("ETHNICITY", when(PA_PATFLAG_ZH("ETHNIC_GROUP_C")!== "-1", concat_ws("", lit("e."), PA_PATFLAG_ZH("ETHNIC_GROUP_C")))
                        .when(PA_PATFLAG_ZH("PATIENT_RACE_C").isNull, concat_ws("", lit("r."), PA_PATFLAG_ZH("PATIENT_RACE_C"))))
                .withColumn("ETH_UPDATE_DATE", when(PA_PATFLAG_ZH("ETHNIC_GROUP_C")!== "-1", PA_PATFLAG_ZH("PA_UPDATE_DATE")).otherwise(PA_PATFLAG_ZH("RA_UPDATE_DATE")))
                .withColumn("FIRSTNAME", PA_PATFLAG_ZH("PAT_FIRST_NAME"))
                .withColumn("LASTNAME", PA_PATFLAG_ZH("PAT_LAST_NAME"))
                .withColumn("GENDER", PA_PATFLAG_ZH("SEX_C"))
                .withColumn("RACE", PA_PATFLAG_ZH("PATIENT_RACE_C"))
                .withColumn("RA_UPDATE_DATE", PA_PATFLAG_ZH("UPDATE_DATE"))
                .withColumn("UPDATE_DATE", PA_PATFLAG_ZH("PA_UPDATE_DATE"))
                .withColumn("STATE", PA_PATFLAG_ZH("ZH_STATE_NAME"))
                .withColumn("ZIP", STANDARDIZE_POSTAL_CODE(PA_PATFLAG_ZH("ZIP")))
                .withColumn("LANGUAGE", PA_PATFLAG_ZH("LANGUAGE_C"))
                .withColumn("SSN", when(PA_PATFLAG_ZH("SSN")=== "123-45-6789", null).otherwise(PA_PATFLAG_ZH("SSN")))
                .withColumn("DATASRC", lit("PATREG"))
                .withColumn("DOB_IND", when(PA_PATFLAG_ZH("BIRTH_DATE").isNull, "1").otherwise("0"))

        val group3 = Window.partitionBy(TEMP_EPIC_PATS_GRPID("PATIENTID")).orderBy(TEMP_EPIC_PATS_GRPID("PATIENTID"),TEMP_EPIC_PATS_GRPID("DOB_IND").desc,TEMP_EPIC_PATS_GRPID("UPDATE_DATE").desc)

        var TEMP_PATIENT_2 = TEMP_EPIC_PATS_GRPID.withColumn("DATEOFBIRTH", first(TEMP_EPIC_PATS_GRPID("DATEOFBIRTH")).over(group3))

        val group4 = Window.partitionBy(TEMP_PATIENT_2("PATIENTID")).orderBy(TEMP_PATIENT_2("UPDATE_DATE").desc)
        var TEMP_PAT_FINAL = TEMP_PATIENT_2.withColumn("DATEOFDEATH", first(TEMP_PATIENT_2("DATEOFDEATH")).over(group4))
                .withColumn("MEDICALRECORDNUMBER", first(TEMP_PATIENT_2("MEDICALRECORDNUMBER")).over(group4))
                .withColumn("ROWNBR", row_number().over(group4)).filter("ROWNBR==1")
                .withColumn("FACILITYID", lit("NULL")).withColumn("INACTIVE_FLAG", lit("NULL"))


        TEMP_PAT_FINAL = TEMP_PAT_FINAL.withColumn("FACILITYID", when(TEMP_PAT_FINAL("FACILITYID")!=="NULL", TEMP_PAT_FINAL("FACILITYID") ))
                .withColumn("INACTIVE_FLAG", when(TEMP_PAT_FINAL("INACTIVE_FLAG")!=="NULL", TEMP_PAT_FINAL("INACTIVE_FLAG") ))

        var PATIENT_MPI =dfs("patient_mpi")
        TEMP_PAT_FINAL = TEMP_PAT_FINAL.withColumnRenamed("HGPID", "HGPID_tpf").withColumnRenamed("GRP_MPI", "GRP_MPI_tpf")
        TEMP_PAT_FINAL.join(PATIENT_MPI, TEMP_PAT_FINAL("PATIENTID")===PATIENT_MPI("PATIENTID_MPI"), "left_outer")
    }

    map = Map(
        "GROUPID" -> mapFrom("GROUPID"),
        "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID")
    )
}

//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")