package com.humedica.mercury.etl.crossix.rxorderphi

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderphiEP2TempPatientDetails(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List( "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")

    tables = List("patreg", "cdr.map_predicate_values", "identify_id", "pat_race", "patient_3", "patient_fyi_flags", "zh_state", "cdr.patient_mpi")

    columnSelect = Map(
        "patreg" -> List("FILE_ID",  "PAT_ID", "PAT_MRN_ID", "BIRTH_DATE", "DEATH_DATE", "PAT_STATUS_C",
            "MARITAL_STATUS_C", "PAT_MIDDLE_NAME", "ETHNIC_GROUP_C", "PAT_FIRST_NAME", "PAT_LAST_NAME",
            "SEX_C", "CITY", "STATE_C", "ZIP", "SSN", "LANGUAGE_C", "UPDATE_DATE"),
        "identify_id" -> List(),
        "pat_race" -> List("FILE_ID", "PATIENT_RACE_C", "PAT_ID", "UPDATE_DATE"),
        "patient_3" -> List("FILE_ID",   "IS_TEST_PAT_YN", "PAT_ID", "UPDATE_DATE"),
        "patient_fyi_flags" -> List("FILE_ID",  "PAT_FLAG_TYPE_C","PATIENT_ID"),
        "zh_state" -> List(),
        "cdr.patient_mpi" -> List("PATIENTID","HGPID","GRP_MPI")
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
        var ID_SUBTYPE = VAL.withColumn("IDENTITY_TYPE_ID", VAL("COLUMN_VALUE").substr(1, 1)).withColumn("IDENTITY_SUBTYPE_ID", VAL("COLUMN_VALUE").substr(1, 1))
        var IDENTITY_ID = dfs("identify_id")
        IDENTITY_ID = IDENTITY_ID.withColumnRenamed("IDENTITY_TYPE_ID", "IDENTITY_TYPE_ID1")
        var JOIN_ID = IDENTITY_ID.join(ID_SUBTYPE, IDENTITY_ID("IDENTITY_TYPE_ID1") === ID_SUBTYPE("IDENTITY_TYPE_ID"), "left_outer")
        var ID = JOIN_ID.select("IDENTITY_TYPE_ID", "IDENTITY_ID", "PAT_ID", "IDENTITY_SUBTYPE_ID").distinct()

        var PAT_RACE = dfs("pat_race").withColumnRenamed("UPDATE_DATE", "RA_UPDATE_DATE")

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

        var PA_ID = PA.join(ID, PA("PAT_ID_PA") === ID("PAT_ID_ID"), "left_outer").drop("PAT_ID_ID")
        var PA_ID_RA = PA_ID.join(PAT_RACE1, PA_ID("PAT_ID_PA") === PAT_RACE1("PAT_ID_PR"), "left_outer")
        var PA_ID_RA_PA3 = PA_ID_RA.join(PAT3_D, PA_ID("PAT_ID_PA") === PAT3_D("PAT_ID_PR"), "left_outer")
        var PA_PATFLAG = PA_ID_RA_PA3.join(PATIENT_FYI_FLAGS, PA_ID_RA_PA3("PAT_ID_PA") === PATIENT_FYI_FLAGS("PATIENT_ID"), "left_outer")
        var PA_PATFLAG_ZH = PA_PATFLAG.join(ZH_STATE, PA_PATFLAG("STATE_C") === ZH_STATE("STATE_C"), "left_outer")
        PA_PATFLAG_ZH = PA_PATFLAG_ZH.withColumnRenamed("PAT_ID_PA", "PAT_ID")
        var TEMP_EPIC_PATS_GRPID = PA_PATFLAG_ZH.withColumn("PATIENTID", PA_PATFLAG_ZH("PAT_ID"))
                .withColumn("MEDICALRECORDNUMBER", PA_PATFLAG_ZH("IDENTITY_ID"))
                .withColumn("MRN_ID", PA_PATFLAG_ZH("IDENTITY_ID"))
                .withColumn("DATEOFBIRTH", PA_PATFLAG_ZH("BIRTH_DATE"))
                .withColumn("DATEOFDEATH", PA_PATFLAG_ZH("DEATH_DATE"))
                .withColumn("PAT_STATUS", PA_PATFLAG_ZH("PAT_STATUS_C"))
                .withColumn("MARITAL_STATUS", PA_PATFLAG_ZH("MARITAL_STATUS_C"))
                .withColumn("MIDDLE_NAME", PA_PATFLAG_ZH("PAT_MIDDLE_NAME"))
                .withColumn("ETHNICITY", when(PA_PATFLAG_ZH("ETHNIC_GROUP_C") !== "-1", concat_ws("", lit("e."), PA_PATFLAG_ZH("ETHNIC_GROUP_C")))
                        .when(PA_PATFLAG_ZH("PATIENT_RACE_C").isNull, concat_ws("", lit("r."), PA_PATFLAG_ZH("PATIENT_RACE_C"))))
                .withColumn("ETH_UPDATE_DATE", when(PA_PATFLAG_ZH("ETHNIC_GROUP_C") !== "-1", PA_PATFLAG_ZH("PA_UPDATE_DATE")).otherwise(PA_PATFLAG_ZH("RA_UPDATE_DATE")))
                .withColumn("FIRSTNAME", PA_PATFLAG_ZH("PAT_FIRST_NAME"))
                .withColumn("LASTNAME", PA_PATFLAG_ZH("PAT_LAST_NAME"))
                .withColumn("GENDER", PA_PATFLAG_ZH("SEX_C"))
                .withColumn("RACE", PA_PATFLAG_ZH("PATIENT_RACE_C"))
                .withColumn("RA_UPDATE_DATE", PA_PATFLAG_ZH("UPDATE_DATE"))
                .withColumn("UPDATE_DATE", PA_PATFLAG_ZH("PA_UPDATE_DATE"))
                .withColumn("STATE", PA_PATFLAG_ZH("ZH_STATE_NAME"))
                .withColumn("ZIP", STANDARDIZE_POSTAL_CODE(PA_PATFLAG_ZH("ZIP")))
                .withColumn("LANGUAGE", PA_PATFLAG_ZH("LANGUAGE_C"))
                .withColumn("SSN", when(PA_PATFLAG_ZH("SSN") === "123-45-6789", null).otherwise(PA_PATFLAG_ZH("SSN")))
                .withColumn("DATASRC", lit("PATREG"))
                .withColumn("DOB_IND", when(PA_PATFLAG_ZH("BIRTH_DATE").isNull, "1").otherwise("0"))

        val group3 = Window.partitionBy(TEMP_EPIC_PATS_GRPID("PATIENTID")).orderBy(TEMP_EPIC_PATS_GRPID("PATIENTID"), TEMP_EPIC_PATS_GRPID("DOB_IND").desc, TEMP_EPIC_PATS_GRPID("UPDATE_DATE").desc)

        var TEMP_PATIENT_2 = TEMP_EPIC_PATS_GRPID.withColumn("DATEOFBIRTH", first(TEMP_EPIC_PATS_GRPID("DATEOFBIRTH")).over(group3))

        val group4 = Window.partitionBy(TEMP_PATIENT_2("PATIENTID")).orderBy(TEMP_PATIENT_2("UPDATE_DATE").desc)
        var TEMP_PAT_FINAL = TEMP_PATIENT_2.withColumn("DATEOFDEATH", first(TEMP_PATIENT_2("DATEOFDEATH")).over(group4))
                .withColumn("MEDICALRECORDNUMBER", first(TEMP_PATIENT_2("MEDICALRECORDNUMBER")).over(group4))
                .withColumn("ROWNBR", row_number().over(group4)).filter("ROWNBR==1")
                .withColumn("FACILITYID", lit("NULL")).withColumn("INACTIVE_FLAG", lit("NULL"))


        TEMP_PAT_FINAL = TEMP_PAT_FINAL.withColumn("FACILITYID", when(TEMP_PAT_FINAL("FACILITYID") !== "NULL", TEMP_PAT_FINAL("FACILITYID")))
                .withColumn("INACTIVE_FLAG", when(TEMP_PAT_FINAL("INACTIVE_FLAG") !== "NULL", TEMP_PAT_FINAL("INACTIVE_FLAG")))

        var PATIENT_MPI = dfs("patient_mpi").select("PATIENTID", "HGPID", "GRP_MPI").withColumnRenamed("PATIENTID", "PATIENTID_MPI").distinct()
        TEMP_PAT_FINAL = TEMP_PAT_FINAL.withColumnRenamed("HGPID", "HGPID_tpf").withColumnRenamed("GRP_MPI", "GRP_MPI_tpf")
        TEMP_PAT_FINAL = TEMP_PAT_FINAL.join(PATIENT_MPI, TEMP_PAT_FINAL("PATIENTID") === PATIENT_MPI("PATIENTID_MPI"), "left_outer")
        TEMP_PAT_FINAL = TEMP_PAT_FINAL.select("MRN_ID", "IDENTITY_TYPE_ID", "SSN", "MIDDLE_NAME", "LANGUAGE", "MARITAL_STATUS", "PAT_STATUS", "ETH_UPDATE_DATE", "ETHNICITY", "RACE", "GENDER", "LASTNAME", "FIRSTNAME", "UPDATE_DATE",  "DATASRC", "PATIENTID", "FACILITYID", "DATEOFBIRTH", "MEDICALRECORDNUMBER", "HGPID", "DATEOFDEATH", "GRP_MPI", "INACTIVE_FLAG", "ZIP")
        //TEMP_PAT_FINAL = TEMP_PAT_FINAL.filter("GRP_MPI IS NULL")

        TEMP_PAT_FINAL = TEMP_PAT_FINAL.withColumn("ENCOUNTERID", lit("NULL"))
        TEMP_EPIC_PATS_GRPID = TEMP_PAT_FINAL.select( "MRN_ID", "IDENTITY_TYPE_ID", "SSN", "MIDDLE_NAME", "LANGUAGE", "MARITAL_STATUS", "PAT_STATUS", "ETH_UPDATE_DATE", "ETHNICITY", "RACE", "GENDER", "ENCOUNTERID", "LASTNAME", "FIRSTNAME",  "DATASRC", "PATIENTID", "FACILITYID", "DATEOFBIRTH", "MEDICALRECORDNUMBER", "HGPID", "DATEOFDEATH",  "GRP_MPI", "INACTIVE_FLAG", "UPDATE_DATE", "ZIP")

        //      ---TEMP_PATIENT_DETAIL_FIRST_NAME
        var TEMP_PATIENT_DETAIL_FIRST_NAME = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("FIRST_NAME")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("FIRSTNAME")).select("HGPID", "GRP_MPI", "ENCOUNTERID",  "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
        var group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_FIRST_NAME("PATIENTID"), TEMP_PATIENT_DETAIL_FIRST_NAME("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_FIRST_NAME("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_FNAME1 = TEMP_PATIENT_DETAIL_FIRST_NAME.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null").drop("ROW_NUM")
        var TEMP_PAT_DET_FST_NAME = FIL_TEMP_PATDET_FNAME1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")

        //        ---TEMP_PATIENT_DETAIL_FIRST_NAME
        var TEMP_PATIENT_DETAIL_LAST_NAME = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("LAST_NAME")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("LASTNAME")).select("HGPID", "GRP_MPI", "ENCOUNTERID",  "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_LAST_NAME("PATIENTID"), TEMP_PATIENT_DETAIL_LAST_NAME("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_LAST_NAME("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_LNAME1 = TEMP_PATIENT_DETAIL_LAST_NAME.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_LST_NAME = FIL_TEMP_PATDET_LNAME1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP",  "HGPID", "GRP_MPI")

        //        ---TEMP_PATIENT_DETAIL_CITY
        var TEMP_PATIENT_DETAIL_CITY = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("CITY")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("LASTNAME")).select("HGPID", "GRP_MPI", "ENCOUNTERID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_CITY("PATIENTID"), TEMP_PATIENT_DETAIL_CITY("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_CITY("PATDETAIL_TIMESTAMP").desc)
        var TEMP_PATDET_CITY1 = TEMP_PATIENT_DETAIL_CITY.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_CITY = TEMP_PATDET_CITY1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")
        //        ---TEMP_PATIENT_DETAIL_CITY
        var TEMP_PATIENT_DETAIL_STATE = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("STATE")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("LASTNAME")).select("HGPID", "GRP_MPI", "ENCOUNTERID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_STATE("PATIENTID"), TEMP_PATIENT_DETAIL_STATE("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_STATE("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_STATE1 = TEMP_PATIENT_DETAIL_STATE.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_STATE = FIL_TEMP_PATDET_STATE1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")

        //        ---TEMP_PATIENT_DETAIL_CITY
        var TEMP_PATIENT_DETAIL_ZIP = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("ZIPCODE")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("ZIP")).select("HGPID", "GRP_MPI", "ENCOUNTERID",  "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_ZIP("PATIENTID"), TEMP_PATIENT_DETAIL_ZIP("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_ZIP("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_ZIP1 = TEMP_PATIENT_DETAIL_ZIP.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_ZIP = FIL_TEMP_PATDET_ZIP1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")

        var TEMP_PATIENT_DETAIL_GENDER = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("GENDER")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("GENDER"))).select("GENDER", "HGPID", "GRP_MPI", "ENCOUNTERID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "GENDER")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_GENDER("PATIENTID"), TEMP_PATIENT_DETAIL_GENDER("GENDER")).orderBy(TEMP_PATIENT_DETAIL_GENDER("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_GENDER1 = TEMP_PATIENT_DETAIL_GENDER.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_GENDER = FIL_TEMP_PATDET_GENDER1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID", "HGPID", "GRP_MPI")

        var TEMP_PATIENT_DETAIL_RACE = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("RACE")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("RACE"))).select("RACE", "HGPID", "GRP_MPI", "ENCOUNTERID",  "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "RACE")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_RACE("PATIENTID"), TEMP_PATIENT_DETAIL_RACE("RACE")).orderBy(TEMP_PATIENT_DETAIL_RACE("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_RACE1 = TEMP_PATIENT_DETAIL_RACE.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_RACE = FIL_TEMP_PATDET_RACE1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")

        var TEMP_PATIENT_DETAIL_ETHNICITY = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("ETHNICITY")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("ETHNICITY"))).select("ETH_UPDATE_DATE", "ETHNICITY", "HGPID", "GRP_MPI", "ENCOUNTERID",  "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "ETHNICITY", "ETH_UPDATE_DATE")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_ETHNICITY("PATIENTID"), TEMP_PATIENT_DETAIL_ETHNICITY("ETHNICITY")).orderBy(TEMP_PATIENT_DETAIL_ETHNICITY("ETH_UPDATE_DATE").desc)
        var FIL_TEMP_PATDET_ETHNICITY1 = TEMP_PATIENT_DETAIL_ETHNICITY.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_ETHNICITY = FIL_TEMP_PATDET_ETHNICITY1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")

        var TEMP_PATIENT_DETAIL_STATUS = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("DECEASED")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("PAT_STATUS"))).select("PAT_STATUS", "HGPID", "GRP_MPI", "ENCOUNTERID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "PAT_STATUS")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_STATUS("PATIENTID"), TEMP_PATIENT_DETAIL_STATUS("PAT_STATUS")).orderBy(TEMP_PATIENT_DETAIL_STATUS("PATDETAIL_TIMESTAMP").desc)
        var TEMP_PATDET_STATUS1 = TEMP_PATIENT_DETAIL_STATUS.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_STATUS = TEMP_PATDET_STATUS1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")

        var TEMP_PATIENT_DETAIL_MARITAL = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("MARITAL")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("PAT_STATUS"))).select("MARITAL_STATUS", "HGPID", "GRP_MPI", "ENCOUNTERID",  "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "MARITAL_STATUS")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_MARITAL("PATIENTID"), TEMP_PATIENT_DETAIL_MARITAL("MARITAL_STATUS")).orderBy(TEMP_PATIENT_DETAIL_MARITAL("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_MARITAL1 = TEMP_PATIENT_DETAIL_MARITAL.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_MARITAL = FIL_TEMP_PATDET_MARITAL1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")


        var TEMP_PATIENT_DETAIL_MIDDLE_NAME = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("MIDDLE_NAME")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("MIDDLE_NAME")).select("MIDDLE_NAME", "DATASRC", "PATIENTDETAILTYPE", "HGPID", "GRP_MPI", "ENCOUNTERID", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_MIDDLE_NAME("PATIENTID"), TEMP_PATIENT_DETAIL_MIDDLE_NAME("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_MIDDLE_NAME("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_MNAME1 = TEMP_PATIENT_DETAIL_MIDDLE_NAME.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_MID_NAME = FIL_TEMP_PATDET_MNAME1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")


        var TEMP_PATIENT_DETAIL_LANG = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("LANGUAGE")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("LANGUAGE"))).select(  "LANGUAGE", "HGPID", "GRP_MPI", "ENCOUNTERID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "LANGUAGE")
        group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_LANG("PATIENTID"), TEMP_PATIENT_DETAIL_LANG("LANGUAGE")).orderBy(TEMP_PATIENT_DETAIL_LANG("PATDETAIL_TIMESTAMP").desc)
        var FIL_TEMP_PATDET_LANG1 = TEMP_PATIENT_DETAIL_LANG.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
        var TEMP_PAT_DET_LANGUAGE = FIL_TEMP_PATDET_LANG1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID", "GRP_MPI")

        var TEMP_PATIENT_ID_SSN1 = TEMP_EPIC_PATS_GRPID.filter("SSN is not null").withColumn("DATASRC", lit("PATREG")).withColumn("IDVALUE", TEMP_EPIC_PATS_GRPID("SSN")).withColumn("IDTYPE", lit("SSN")).withColumn("ID_SUBTYPE", lit("NULL")).select("SSN", "PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "HGPID", "GRP_MPI", "ID_SUBTYPE")
        var TEMP_PATIENT_ID_MRN = TEMP_EPIC_PATS_GRPID.join(VAL2, TEMP_EPIC_PATS_GRPID("IDENTITY_TYPE_ID") === VAL2("COLUMN_VALUE"), "left_outer")
        TEMP_PATIENT_ID_MRN = TEMP_PATIENT_ID_MRN.withColumn("DATASRC", lit("PATREG")).withColumn("IDVALUE", concat_ws("", TEMP_EPIC_PATS_GRPID("MRN_ID"), lit("A"))).withColumn("IDTYPE", lit("MRN")).withColumn("ID_SUBTYPE", lit("NULL"))
        TEMP_PATIENT_ID_MRN = TEMP_PATIENT_ID_MRN.select("PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "HGPID", "GRP_MPI", "ID_SUBTYPE", "IDENTITY_TYPE_ID", "MRN_ID")
        TEMP_PATIENT_ID_MRN = TEMP_PATIENT_ID_MRN.withColumn("IDENTITY_SUBTYPE_ID", when(TEMP_PATIENT_ID_MRN("IDENTITY_TYPE_ID").isNotNull, concat_ws("", TEMP_PATIENT_ID_MRN("IDENTITY_TYPE_ID"), lit("."))))
        TEMP_PATIENT_ID_MRN = TEMP_PATIENT_ID_MRN.withColumn("MRN_SUBTYPE_JOIN", concat_ws("", TEMP_PATIENT_ID_MRN("IDENTITY_SUBTYPE_ID"), TEMP_PATIENT_ID_MRN("IDENTITY_TYPE_ID")))

        var JOIN_EPIC_PATS_MPV1 = TEMP_PATIENT_ID_MRN.join(VAL2, TEMP_PATIENT_ID_MRN("MRN_SUBTYPE_JOIN") === VAL2("COLUMN_VALUE"), "inner")
        var FILTER_EPIC_PATS_MPV1 = JOIN_EPIC_PATS_MPV1.filter("MRN_ID is not null and IDENTITY_SUBTYPE_ID is not null")
        var TEMP_PATIENT_ID_MRN1 = FILTER_EPIC_PATS_MPV1.withColumn("DATASRC", lit("PATREG")).withColumn("IDTYPE", lit("MRN")).withColumn("IDVALUE", lit("MRN_ID")).withColumn("ID_SUBTYPE", lit("NULL"))
                .select("PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "HGPID", "GRP_MPI", "ID_SUBTYPE")


        var TEMP_PATIENTDETAIL = TEMP_PAT_DET_LANGUAGE.unionAll(TEMP_PAT_DET_STATE).unionAll(TEMP_PAT_DET_MID_NAME).unionAll(TEMP_PAT_DET_MARITAL)
                .unionAll(TEMP_PAT_DET_STATUS).unionAll(TEMP_PAT_DET_ETHNICITY).unionAll(TEMP_PAT_DET_RACE).unionAll(TEMP_PAT_DET_GENDER).unionAll(TEMP_PAT_DET_ZIP)
                .unionAll(TEMP_PAT_DET_CITY).unionAll(TEMP_PAT_DET_LST_NAME).unionAll(TEMP_PAT_DET_FST_NAME)


        TEMP_PATIENT_ID_SSN1 = TEMP_PATIENT_ID_SSN1.withColumnRenamed("HGPID", "HGPID_SSN").withColumnRenamed("GRP_MPI", "GRP_MPI_SSN").drop("IDENTITY_SUBTYPE_ID").drop("IDENTITY_TYPE_ID").drop("MRN_ID").drop("MRN_SUBTYPE_JOIN").drop("SSN")
        TEMP_PATIENT_ID_MRN = TEMP_PATIENT_ID_MRN.withColumnRenamed("GRP_MPI", "GRP_MPI_MRN").withColumnRenamed("GRP_MPI", "GRP_MPI_MRN").drop("IDENTITY_SUBTYPE_ID").drop("IDENTITY_TYPE_ID").drop("MRN_ID").drop("MRN_SUBTYPE_JOIN")

        var TEMP_PATIENT_ID_1 = TEMP_PATIENT_ID_SSN1.unionAll(TEMP_PATIENT_ID_MRN).unionAll(TEMP_PATIENT_ID_MRN1)
        var TEMP_PATIENT_ID_JOIN = TEMP_PATIENT_ID_1.join(PATIENT_MPI, TEMP_PATIENT_ID_1("PATIENTID") === PATIENT_MPI("PATIENTID_MPI"), "left_outer")
                .select("PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "HGPID", "GRP_MPI", "ID_SUBTYPE")

        var TEMP_PATDETAIL_JOIN = TEMP_PATIENTDETAIL.withColumnRenamed("HGPID", "HGPID_tpj1").withColumnRenamed("GRP_MPI", "GRP_MPI_tpj1").join(PATIENT_MPI, TEMP_PATIENT_ID_1("PATIENTID") === PATIENT_MPI("PATIENTID_MPI"), "left_outer")

        TEMP_PATDETAIL_JOIN.withColumn("DATASRC", lit("PATREG")).withColumn("IDTYPE", lit("MRN")).withColumn("IDVALUE", lit("MRN_ID")).withColumn("ID_SUBTYPE", lit("NULL"))

    }

    map = Map(
        "DATASRC" -> literal("PATREG"),
        "IDTYPE" -> literal("MRN"),
        "IDVALUE" -> literal("MRN_ID"),
        "ID_SUBTYPE" -> literal("NULL")
    )
}

//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")