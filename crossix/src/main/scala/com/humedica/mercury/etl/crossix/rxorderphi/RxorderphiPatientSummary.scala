package com.humedica.mercury.etl.crossix.rxorderphi

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderphiPatientSummary(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("PATIENTID", "MEDICALRECORDNUMBER", "YOB", "MOB", "DOB", "LOCAL_GENDER",
        "LOCAL_ETHNICITY", "LOCAL_RACE", "LOCAL_ZIPCODE", "FIRST_NAME", "LAST_NAME", "MAPPED_GENDER",
        "MAPPED_ETHNICITY", "MAPPED_RACE", "MAPPED_ZIPCODE", "LOCAL_DEATH_IND", "MAPPED_DEATH_IND",
        "DATE_OF_DEATH", "LOCAL_DOD", "SSDI_DOD", "SSDI_NAME_MATCH","INACTIVE_FLAG", "LOCAL_LANGUAGE",
        "MAPPED_LANGUAGE", "LOCAL_MARITAL_STATUS", "MAPPED_MARITAL_STATUS", "RELIGION")

    tables = List(
        "temptable:crossix.patient.ASTempPatient",
        "temptable:crossix.patient.ASTempPatientDetails",
        "temptable:crossix.patient.ASTempPatientId",
        "temptable:crossix.patient.EP2TempPatient",
        "temptable:crossix.patient.EP2TempPatientDetails",
        "temptable:crossix.patient.EP2TempPatientId",
        "temptable:crossix.patient.CR2TempPatient",
        "temptable:crossix.patient.CR2TempPatientDetails",
        "temptable:crossix.patient.CR2TempPatientId",
        "cdr.zcm_datasrc_priority","cdr.map_gender","cdr.map_race",
        "cdr.map_ethnicity","cdr.map_deceased_indicator", "cdr.map_language", "cdr.map_marital_status", "cdr.ref_death_index"
    )

    columnSelect = Map(
        "temptable:crossix.patient.ASTempPatient" -> List(),
        "temptable:crossix.patient.ASTempPatientDetails" -> List(),
        "temptable:crossix.patient.ASTempPatientId" -> List(),
        "temptable:crossix.patient.EP2TempPatient" -> List(),
        "temptable:crossix.patient.EP2TempPatientDetails" -> List(),
        "temptable:crossix.patient.EP2TempPatientId" -> List(),
        "temptable:crossix.patient.CR2TempPatient" -> List(),
        "temptable:crossix.patient.CR2TempPatientDetails" -> List(),
        "temptable:crossix.patient.CR2TempPatientId" -> List(),
        "cdr.zcm_datasrc_priority" -> List(),
        "cdr.map_gender" -> List(),
        "cdr.map_race" -> List(),
        "cdr.map_ethnicity" -> List(),
        "cdr.map_deceased_indicator" -> List(),
        "cdr.map_language" -> List(),
        "cdr.map_marital_status" -> List(),
        "cdr.ref_death_index" -> List()
    )


    beforeJoin = Map(
        "cdr.map_gender" -> ((df: DataFrame) => {df.withColumnRenamed("CUI", "MG_CUI")}),
        "cdr.map_race" -> ((df: DataFrame) => {df.withColumnRenamed("CUI", "MR_CUI")}),
        "cdr.map_ethnicity" -> ((df: DataFrame) => {df.withColumnRenamed("CUI", "ME_CUI")}),
        "cdr.map_deceased_indicator" -> ((df: DataFrame) => {df.withColumnRenamed("CUI", "MD_CUI")}),
        "cdr.map_language" -> ((df: DataFrame) => {df.withColumnRenamed("CUI", "ML_CUI")}),
        "cdr.map_marital_status" -> ((df: DataFrame) => {df.withColumnRenamed("CUI", "M_CUI")}),
        "cdr.ref_death_index" -> ((df: DataFrame) => {df.withColumnRenamed("FIRST_NAME", "RDI_FIRST_NAME").withColumnRenamed("LAST_NAME", "RDI_LAST_NAME").withColumnRenamed("DATE_OF_DEATH", "RDI_DATE_OF_DEATH").withColumnRenamed("SSN", "RDI_SSN")})
    )



    join = (dfs: Map[String, DataFrame])  => {

        var pat = dfs("temptable:crossix.patient.ASTempPatient")
                .unionAll(dfs("temptable:crossix.patient.EP2TempPatient"))
                .unionAll(dfs("temptable:crossix.patient.CR2TempPatient"))

        var patd = dfs("temptable:crossix.patient.ASTempPatientDetails")
                .unionAll(dfs("temptable:crossix.patient.EP2TempPatientDetails"))
                .unionAll(dfs("temptable:crossix.patient.CR2TempPatientDetails"))

        var patid = dfs("temptable:crossix.patient.ASTempPatientId")
                .unionAll(dfs("temptable:crossix.patient.EP2TempPatientId"))
                .unionAll(dfs("temptable:crossix.patient.CR2TempPatientId"))


        var dsp = dfs("cdr.zcm_datasrc_priority")

        var d = pat.join(dsp, Seq("DATASRC"), "left_outer")
        d = d.groupBy("PATIENTID").agg(min("DATEOFBIRTH").as("DATEOFBIRTH"), max("DATEOFDEATH").as("DATEOFDEATH"),max("MEDICALRECORDNUMBER").as("MEDICALRECORDNUMBER"),max("INACTIVE_FLAG").as("INACTIVE_FLAG"))


        var gender = patd.filter("PATIENTDETAILTYPE == 'GENDER'")
        var map_gender = dfs("cdr.map_gender")
        gender=gender.join(map_gender, gender("LOCALVALUE")===map_gender("MNEMONIC"), "left_outer" ).drop("MG_GROUPID")
        gender=gender.groupBy("PATIENTID").agg(max(gender("LOCALVALUE")).as("GENDER"),max(gender("MG_CUI")).as("GENDER_CUI"))


        var race = patd.filter("PATIENTDETAILTYPE == 'RACE'")
        var map_race = dfs("cdr.map_race")
        race=race.join(map_race, race("LOCALVALUE")===map_race("MNEMONIC"), "left_outer" )
        race=race.groupBy("PATIENTID")
                .agg(max(race("LOCALVALUE")).as("RACE"),max(race("MR_CUI")).as("RACE_CUI"))

        var firstname = patd.filter("PATIENTDETAILTYPE == 'FIRST_NAME'")
        firstname=firstname.groupBy("PATIENTID").agg(max(firstname("LOCALVALUE")).as("FIRST_NAME"))

        var religion = patd.filter("PATIENTDETAILTYPE == 'RELIGION'")
        religion=religion.groupBy("PATIENTID").agg(max(religion("LOCALVALUE")).as("RELIGION"))


        var lastname = patd.filter("PATIENTDETAILTYPE == 'LAST_NAME'")
        lastname=lastname.groupBy("PATIENTID")
                .agg(max(lastname("LOCALVALUE")).as("LAST_NAME"))

        var ethnicity = patd.filter("PATIENTDETAILTYPE == 'ETHNICITY'")
        var map_ethnicity = dfs("cdr.map_ethnicity")
        ethnicity=ethnicity.join(map_ethnicity, ethnicity("LOCALVALUE")===map_ethnicity("MNEMONIC"), "left_outer" )
        ethnicity=ethnicity.groupBy("PATIENTID")
                .agg(max(ethnicity("LOCALVALUE")).as("ETHNICITY"),max(ethnicity("ME_CUI")).as("ETHNIC_CUI"))

        var zipcode = patd.filter("PATIENTDETAILTYPE == 'ZIPCODE'")
        zipcode=zipcode.groupBy("PATIENTID")
                .agg(max(zipcode("LOCALVALUE")).as("ZIPCODE"))

        var deceased = patd.filter("PATIENTDETAILTYPE == 'DECEASED'")
        var map_deceased = dfs("map_deceased")
        deceased=deceased.join(map_deceased, when(deceased("LOCALVALUE").isNull, "DEFAULT")===map_deceased("MNEMONIC"), "left_outer" )
        deceased=deceased.groupBy("PATIENTID").agg(max(deceased("LOCALVALUE")).as("DECEASED"),max(deceased("MD_CUI")).as("DECEASED_CUI"))


        var language = patd.filter("PATIENTDETAILTYPE == 'LANGUAGE'")
        var map_language = dfs("map_language")
        language=language.join(map_language, language("LOCALVALUE")===map_language("MNEMONIC") , "left_outer" )
        language=language.groupBy("PATIENTID").agg(max(language("LOCALVALUE")).as("LANGUAGE"),max(language("ML_CUI")).as("LANGUAGE_CUI"))


        var marital = patd.filter("PATIENTDETAILTYPE == 'MARITAL'")
        var map_marital = dfs("map_marital")
        marital=marital.join(map_marital, marital("LOCALVALUE")===map_marital("LOCAL_CODE"), "left_outer" )
        marital=marital.groupBy("PATIENTID").agg(max(marital("LOCAL_CODE")).as("MARITAL_STATUS"),max(marital("M_CUI")).as("MARITAL_STATUS_CUI"))

        patid=patid.groupBy("PATIENTID").agg(count(regexp_replace(patid("IDVALUE"), "-", "")).as("COUNT1"), min(regexp_replace(patid("IDVALUE"), "-", "")).as("SSN")).filter("COUNT1 == 1").drop("COUNT1")

        var ref_death_index = dfs("cdr.ref_death_index")
        ref_death_index=ref_death_index.withColumn("DTB", from_unixtime(unix_timestamp(ref_death_index("DATE_OF_BIRTH"), "MMddyyyy"), "yyyy-MM-dd"))


        d.join(gender, Seq("PATIENTID"), "left_outer")
                .join(race, Seq("PATIENTID"), "left_outer")
                .join(firstname, Seq("PATIENTID"), "left_outer")
                .join(religion, Seq("PATIENTID"), "left_outer")
                .join(lastname, Seq("PATIENTID"), "left_outer")
                .join(ethnicity, Seq("PATIENTID"), "left_outer")
                .join(zipcode, Seq("PATIENTID"), "left_outer")
                .join(deceased, Seq("PATIENTID"), "left_outer")
                .join(language, Seq("PATIENTID"), "left_outer")
                .join(marital, Seq("PATIENTID"), "left_outer")
                .join(ref_death_index, d("DATEOFBIRTH").contains(ref_death_index("DTB")) and d("SSN")===(ref_death_index("RDI_SSN")), "left_outer")
    }


    map = Map(

        "DATE_OF_DEATH" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("RDI_DATE_OF_DEATH").isNotNull,  df("RDI_DATE_OF_DEATH")).otherwise(df("DATEOFBIRTH")))}),
        "LOCAL_DOD" -> mapFrom("DATEOFBIRTH"),
        "SSDI_DOD" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("DATE_OF_DEATH").isNotNull, "Y").otherwise("N"))}),
        "SSDI_NAME_MATCH" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when((upper(trim(df("RDI_FIRST_NAME"))) === upper(trim(df("FIRST_NAME")))) and (upper(trim(df("RDI_LAST_NAME"))) === upper(trim(df("LAST_NAME")))), "Y").otherwise("N"))}),
        "LOCAL_GENDER" -> mapFrom("GENDER"),
        "LOCAL_RACE" -> mapFrom("RACE"),
        "LOCAL_ETHNICITY" -> mapFrom("ETHNICITY"),
        "LOCAL_ZIPCODE" -> mapFrom("ZIPCODE"),
        "LOCAL_LANGUAGE" -> mapFrom("LANGUAGE"),
        "LOCAL_MARITAL_STATUS" -> mapFrom("MARITAL_STATUS"),
        "MAPPED_ZIPCODE" -> ((col: String, df: DataFrame) => {df.withColumn(col,

            when((trim(df("ZIPCODE")).isNull) or (trim(df("ZIPCODE"))==="NULL"), "CH999999")
                    .when(trim(df("ZIPCODE"))==="00000", "CH999990")
                    .when(trim(df("ZIPCODE")).contains("-"), trim(df("ZIPCODE")).substr(0,5))
                    .when((trim(df("ZIPCODE")).between("00601", "99950")===false), "CH999990")
                    .when(trim(df("ZIPCODE")).rlike("[:space:]*(([A-Z][0-9]|[a-z][0-9]){3}[:space:]*)"), trim(upper(df("ZIPCODE"))))
                    .when((regexp_replace(trim(df("ZIPCODE")), "O", "0").rlike("[:space:]*([0-9]{5}([- ]?[0-9]{4})?)[:space:]*") === false), "CH999990")
                    .otherwise(regexp_replace(regexp_replace(trim(df("ZIPCODE")), "O", "0"), "[:space:]*([0-9]{5})[- ]?[0-9]{4}[:space:]*", ""))

        ) }),
        "LOCAL_DEATH_IND" -> mapFrom("DECEASED"),
        "MAPPED_DEATH_IND" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("DECEASED_CUI").isNull, "CH999999").otherwise(df("DECEASED_CUI")))}),
        "MAPPED_GENDER" -> ((col: String, df: DataFrame) => {df.withColumn(col,
            when(df("GENDER").isNull and df("GENDER_CUI").isNull, "CH999999")
                    .when(df("GENDER").isNotNull and df("GENDER_CUI").isNull, "CH999990").otherwise(df("GENDER_CUI")))}),


        "MAPPED_ETHNICITY" -> ((col: String, df: DataFrame) => {df.withColumn(col,
            when(df("ETHNICITY").isNull and df("ETHNIC_CUI").isNull, "CH999999")
                    .when(df("ETHNICITY").isNotNull and df("ETHNIC_CUI").isNull, "CH999990").otherwise(df("ETHNIC_CUI")))}),

        "MAPPED_RACE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("RACE").isNull and df("RACE_CUI").isNull, "CH999999")
                .when(df("RACE").isNotNull and df("RACE_CUI").isNull, "CH999990").otherwise(df("RACE_CUI")))}),

        "MAPPED_LANGUAGE" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("LANGUAGE").isNull and df("LANGUAGE_CUI").isNull, "CH999999")
                .when(df("LANGUAGE").isNotNull and df("LANGUAGE_CUI").isNull, "CH999990").otherwise(df("LANGUAGE_CUI")))}),

        "MAPPED_MARITAL_STATUS" -> ((col: String, df: DataFrame) => {df.withColumn(col, when(df("MARITAL_STATUS").isNull and df("MARITAL_STATUS_CUI").isNull, "CH999999")
                .when(df("MARITAL_STATUS").isNotNull and df("MARITAL_STATUS_CUI").isNull, "CH999990").otherwise(df("MARITAL_STATUS_CUI")))}),

        "INACTIVE_FLAG" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("INACTIVE_FLAG") !== "NULL", df("INACTIVE_FLAG")))})
    )
}


//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")