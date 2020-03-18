package com.humedica.mercury.etl.crossix.rxorderphi


import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderphiCR2TempPatientDetails(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")

    tables = List("cdr.map_predicate_values", "person", "person_alias", "address", "cdr.patient_mpi")

    columnSelect = Map(
        "person" -> List("FILE_ID", "NAME_LAST", "NAME_FULL_FORMATTED","PERSON_ID", "BIRTH_DT_TM", "DECEASED_DT_TM", "NAME_FIRST", "UPDT_DT_TM", "religion_cd", "BEG_EFFECTIVE_DT_TM",
            "ethnic_grp_cd", "sex_cd", "race_cd", "deceased_cd","nationality_cd", "language_cd", "marital_type_cd", "name_middle", "active_ind", "citizenship_cd"),
        "person_alias" -> List("FILE_ID", "person_alias_type_cd", "alias_pool_cd", "person_id", "end_effective_dt_tm","alias", "updt_dt_tm", "active_ind"),
        "address" -> List("FILEID", "ZIPCODE", "PARENT_ENTITY_ID"),
        "cdr.patient_mpi" -> List("PATIENTID","HGPID","GRP_MPI")
    )


    def predicate_value_list(p_mpv: DataFrame, dataSrc: String, entity: String, table: String, column: String, colName: String): DataFrame = {
        var mpv1 = p_mpv.filter(p_mpv("DATA_SRC").equalTo(dataSrc).and(p_mpv("ENTITY").equalTo(entity)).and(p_mpv("TABLE_NAME").equalTo(table)).and(p_mpv("COLUMN_NAME").equalTo(column)))
        mpv1=mpv1.withColumn(colName, mpv1("COLUMN_VALUE"))
        mpv1.select(colName).orderBy(mpv1("DTS_VERSION").desc).distinct()
    }


    join = (dfs: Map[String, DataFrame])  => {


        var mpv = dfs("cdr.map_predicate_values")

        var list_person_alias_type_cd = predicate_value_list(mpv, "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "LIST_PERSON_ALIAS_TYPE_CD_COL_VAL")
        var list_ssn_person_alias_type_cd = predicate_value_list(mpv, "SSN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "list_ssn_person_alias_type_cd_val")
        var list_cmrn_person_alias_type_cd = predicate_value_list(mpv, "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "list_cmrn_person_alias_type_cd_val")
        var list_citizenship_cd = predicate_value_list(mpv, "PATIENT", "PATIENT", "PERSON", "CITIZENSHIP_CD", "list_citizenship_cd_val").select("list_citizenship_cd_val")
        var list_rmrn_person_alias_type_cd = predicate_value_list(mpv, "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "list_rmrn_person_alias_type_cd_val")
        var list_mcmrn_prsn_alias_type_cd = predicate_value_list(mpv, "MRN_CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "list_mcmrn_prsn_alias_type_cd_val")
        var list_cmrn_alias_pool_cd = predicate_value_list(mpv, "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD", "list_cmrn_alias_pool_cd_val")
        var list_alias_pool_cd = predicate_value_list(mpv, "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD", "LIST_ALIAS_POOL_CD_COL_VAL")
        var LIST_RMRN_ALIAS_POOL_CD = predicate_value_list(mpv, "MRN","PATIENT_IDENTIFIER","PERSON_ALIAS","ALIAS_POOL_CD", "LIST_RMRN_ALIAS_POOL_CD_VAL");
        var LIST_CMRN_ALIAS_POOL_CD = predicate_value_list(mpv, "CMRN","PATIENT_IDENTIFIER","PERSON_ALIAS","ALIAS_POOL_CD", "LIST_CMRN_ALIAS_POOL_CD_VAL");


        var person = dfs("person").withColumnRenamed("active_ind", "person_active_ind")

        var name_last = person("name_last")
        var name_full_formatted = person("name_full_formatted")
        var person1 = person.filter((name_last.contains("zz").or(name_last.contains("ZZ")).and((name_full_formatted.contains("patient").and(name_full_formatted.contains("test"))))) !==true)

        var personAlias = dfs("person_alias").withColumnRenamed("person_id", "person_alias_id")

        var person_alias_type_cd = personAlias("person_alias_type_cd")
        var alias_pool_cd = personAlias("alias_pool_cd")
        var alias = personAlias("alias")
        personAlias = personAlias.withColumn("end_effective_dt_tm", from_unixtime(personAlias("end_effective_dt_tm").divide(1000)))
        var end_effective_dt_tm =personAlias("end_effective_dt_tm")

        var pa0 = personAlias.filter(end_effective_dt_tm < current_timestamp())

        var pa = personAlias.join(list_cmrn_person_alias_type_cd, personAlias("person_alias_type_cd")===list_cmrn_person_alias_type_cd("list_cmrn_person_alias_type_cd_val"), "left_outer")
        pa = pa.join(list_ssn_person_alias_type_cd, pa("person_alias_type_cd")===list_ssn_person_alias_type_cd("list_ssn_person_alias_type_cd_val"), "left_outer")
        pa = pa.join(list_person_alias_type_cd, pa("person_alias_type_cd")===list_person_alias_type_cd("LIST_PERSON_ALIAS_TYPE_CD_COL_VAL"), "left_outer")
        pa = pa.join(list_alias_pool_cd, pa("alias_pool_cd")===list_alias_pool_cd("LIST_ALIAS_POOL_CD_COL_VAL"), "left_outer")
        pa = pa.join(list_rmrn_person_alias_type_cd, pa("ALIAS_POOL_CD")===list_rmrn_person_alias_type_cd("list_rmrn_person_alias_type_cd_val"), "left_outer")
        pa = pa.join(LIST_RMRN_ALIAS_POOL_CD, pa("ALIAS_POOL_CD")===LIST_RMRN_ALIAS_POOL_CD("LIST_RMRN_ALIAS_POOL_CD_VAL"), "left_outer")
        pa = pa.join(LIST_CMRN_ALIAS_POOL_CD, pa("ALIAS_POOL_CD")===LIST_CMRN_ALIAS_POOL_CD("LIST_CMRN_ALIAS_POOL_CD_VAL"), "left_outer")

        pa = pa.filter((pa("person_alias_type_cd").isNotNull.or(pa("person_alias_type_cd").isNotNull).or(pa("list_ssn_person_alias_type_cd_val").isNotNull).or(pa("LIST_PERSON_ALIAS_TYPE_CD_COL_VAL").isNotNull)
                .or(pa("list_cmrn_person_alias_type_cd_val").isNotNull)).or(pa("list_rmrn_person_alias_type_cd_val").isNotNull))

        val alias_row = Window.partitionBy(pa("person_alias_id"), pa("LIST_PERSON_ALIAS_TYPE_CD_COL_VAL")).orderBy(pa("updt_dt_tm").desc, pa("active_ind").desc)
        var pa1 = pa.withColumn("alias_row", row_number.over(alias_row))
        var personAlias1 = pa1.withColumnRenamed("updt_dt_tm", "pa_updt_dt_tm").filter(pa1("alias_row")===1)

        var ppa = person1.join(personAlias1, person1("person_id") === personAlias1("person_alias_id"), "left_outer")

        var address = dfs("address")
        address=address.withColumn("ZIPCODE", when(address("ZIPCODE") !== "0", address("ZIPCODE").substr(1,5))).withColumn("FILE_ID", address("FILEID").cast("String")).select("FILE_ID", "ZIPCODE", "PARENT_ENTITY_ID")
        ppa = ppa.join(address, ppa("PERSON_ID")===address("PARENT_ENTITY_ID"), "left_outer")


        var ppa2 = ppa.select("person_id", "birth_dt_tm", "deceased_dt_tm", "name_first", "name_last", "updt_dt_tm", "pa_updt_dt_tm",
            "end_effective_dt_tm", "beg_effective_dt_tm", "ethnic_grp_cd", "sex_cd", "race_cd", "deceased_cd",
            "nationality_cd", "language_cd", "marital_type_cd", "name_middle", "person_active_ind", "person_alias_type_cd", "alias_pool_cd",
            "alias",  "citizenship_cd", "list_cmrn_person_alias_type_cd_val", "list_ssn_person_alias_type_cd_val", "LIST_PERSON_ALIAS_TYPE_CD_COL_VAL",
            "LIST_ALIAS_POOL_CD_COL_VAL", "list_rmrn_person_alias_type_cd_val", "LIST_RMRN_ALIAS_POOL_CD_VAL", "LIST_CMRN_ALIAS_POOL_CD_VAL", "religion_cd", "ZIPCODE", "PARENT_ENTITY_ID")

        var PATIENT_MPI1 = dfs("cdr.patient_mpi")

        ppa2 = ppa2.join(PATIENT_MPI1, ppa2("person_id")===PATIENT_MPI1("patientid"), "left_outer")
        var client_ds_id=ppa2("CLIENT_DS_ID_person")

        var ppa3 = ppa2.withColumnRenamed("person_id", "personid")
        ppa3=ppa3.withColumnRenamed("birth_dt_tm", "dateofbirth")
        ppa3=ppa3.withColumnRenamed("deceased_dt_tm", "dateofdeath")
        ppa3=ppa3.withColumnRenamed("name_first", "first_name")
        ppa3=ppa3.withColumnRenamed("name_last", "last_name")
        ppa3=ppa3.withColumnRenamed("updt_dt_tm", "update_date")
        ppa3=ppa3.withColumnRenamed("pa_updt_dt_tm", "pa_update_date")
        ppa3=ppa3.withColumn("ethnicity_value", when(ppa2("ethnic_grp_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("ethnic_grp_cd"))))
        ppa3=ppa3.withColumn("gender", when(ppa2("sex_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("sex_cd"))))
        ppa3=ppa3.withColumn("race", when(ppa2("race_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("race_cd"))))
        ppa3=ppa3.withColumn("death_ind", when(ppa2("deceased_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("deceased_cd"))))
        ppa3=ppa3.withColumn("ethnic_grp", when(ppa2("nationality_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("nationality_cd"))))
        ppa3=ppa3.withColumn("language", when(ppa2("language_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("language_cd"))))
        ppa3=ppa3.withColumn("marital", when(ppa2("marital_type_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("marital_type_cd"))))
        ppa3=ppa3.withColumnRenamed("name_middle", "middle_name")
        ppa3=ppa3.withColumn("mrn", when(ppa3("person_active_ind") === "1" and (ppa3("LIST_PERSON_ALIAS_TYPE_CD_COL_VAL").isNotNull.or(ppa3("LIST_ALIAS_POOL_CD_COL_VAL").isNotNull)), ppa3("alias")))
        ppa3=ppa3.withColumn("rmrn", when(ppa3("person_active_ind") === "1" and (ppa3("list_rmrn_person_alias_type_cd_val").isNotNull.or(ppa3("LIST_RMRN_ALIAS_POOL_CD_VAL").isNotNull)), ppa3("alias")))
        ppa3=ppa3.withColumn("empi", when(ppa3("person_alias_type_cd") === "2", ppa3("alias")))
        ppa3=ppa3.withColumn("ssn", when(ppa3("person_alias_type_cd") === ppa3("list_ssn_person_alias_type_cd_val"), ppa3("alias")))
        ppa3=ppa3.withColumn("ID_SUBTYPE", when(ppa3("LIST_CMRN_ALIAS_POOL_CD_VAL").isNotNull, "CMRN").otherwise(when(ppa3("LIST_RMRN_ALIAS_POOL_CD_VAL").isNotNull, "MRN")))
        ppa3=ppa3.withColumn("active_id_flag", when(ppa3("person_active_ind") === "1", "1"))
        ppa3=ppa3.withColumnRenamed("display", "religion_cd")
        ppa3=ppa3.withColumnRenamed("citizenship_cd", "citizenship_code")
        ppa3 = ppa3.withColumn("ethnicity", when(ppa3("ethnicity_value").isNotNull, ppa3("ethnicity_value")).otherwise(ppa3("race")))

        val dob = Window.partitionBy(ppa3("patientid")).orderBy((when(ppa3("dateofbirth").isNull, lit("1")).otherwise(lit("0"))).desc)
        val dod = Window.partitionBy(ppa3("patientid")).orderBy(ppa3("update_date").desc)
        val medicalrecordnumber = Window.partitionBy(ppa3("patientid")).orderBy(ppa3("active_id_flag").desc, (when(ppa3("mrn").isNull, lit("1")).otherwise(lit("0"))).desc, ppa3("pa_update_date"))
        val medicalrecordnumber_excp = Window.partitionBy(ppa3("patientid")).orderBy(ppa3("active_id_flag").desc, (when(ppa3("rmrn").isNull, lit("1")).otherwise(lit("0"))).desc, ppa3("pa_update_date"))
        val rownumber = Window.partitionBy(ppa3("patientid")).orderBy(ppa3("update_date").desc)
        val status_row = Window.partitionBy(ppa3("patientid"), ppa3("death_ind")).orderBy(ppa3("update_date").desc)
        val ethnicity_row = Window.partitionBy(ppa3("patientid"), when(ppa3("ethnicity_value").isNull, ppa3("race")).otherwise(ppa3("ethnicity_value"))).orderBy(ppa3("update_date").desc)
        val first_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("first_name"))).orderBy(ppa3("update_date").desc)
        val last_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("last_name"))).orderBy(ppa3("update_date").desc)
        val gender_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("gender"))).orderBy(ppa3("update_date").desc)
        val race_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("race"))).orderBy(ppa3("update_date").desc)
        val ssn_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("ssn"))).orderBy(ppa3("update_date").desc)
        val empi_row = Window.partitionBy(ppa3("patientid"), when(ppa3("empi").isNull, "0").otherwise("1")).orderBy(ppa3("end_effective_dt_tm").desc, ppa3("beg_effective_dt_tm").desc, ppa3("pa_update_date").desc)
        val mrn_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("mrn"))).orderBy(ppa3("update_date").desc)
        val rmrn_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("rmrn"))).orderBy(ppa3("update_date").desc)
        val ethnic_grp_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("ethnic_grp"))).orderBy(ppa3("update_date").desc)
        val language_row = Window.partitionBy(ppa3("patientid"),  upper(ppa3("language"))).orderBy(ppa3("update_date").desc)
        val marital_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("marital"))).orderBy(ppa3("update_date").desc)
        val middle_row = Window.partitionBy(ppa3("patientid"), upper(ppa3("middle_name"))).orderBy(ppa3("update_date").desc)
        val religion_row = Window.partitionBy(ppa3("patientid"),  upper(ppa3("religion_cd"))).orderBy(ppa3("update_date").desc)
        val zipcode_row = Window.partitionBy(ppa3("PARENT_ENTITY_ID"), upper(ppa3("ZIPCODE"))).orderBy(ppa3("update_date").desc)

        var ppa5=ppa3.withColumn("datasrc", lit("patient"))
        ppa5=ppa5.withColumn("dob", first("dateofbirth").over(dob))
        ppa5=ppa5.withColumn("dod", first("dateofdeath").over(dod))
        ppa5=ppa5.withColumn("MEDICALRECORDNUMBER", first("mrn").over(medicalrecordnumber))
        ppa5=ppa5.withColumn("medicalrecordnumber_excp", first("rmrn").over(medicalrecordnumber_excp))
        ppa5=ppa5.withColumn("rownumber", row_number().over(rownumber))
        ppa5=ppa5.withColumn("status_row", row_number().over(status_row))
        ppa5=ppa5.withColumn("ethnicity_row", row_number().over(ethnicity_row))
        ppa5=ppa5.withColumn("last_row", row_number().over(last_row))
        ppa5=ppa5.withColumn("race_row", row_number().over(race_row))
        ppa5=ppa5.withColumn("first_row", row_number().over(first_row))
        ppa5=ppa5.withColumn("gender_row", row_number().over(gender_row))
        ppa5=ppa5.withColumn("ssn_row", row_number().over(ssn_row))
        ppa5=ppa5.withColumn("empi_row", row_number().over(empi_row))
        ppa5=ppa5.withColumn("mrn_row", row_number().over(mrn_row))
        ppa5=ppa5.withColumn("rmrn_row", row_number().over(rmrn_row))
        ppa5=ppa5.withColumn("ethnic_grp_row", row_number().over(ethnic_grp_row))
        ppa5=ppa5.withColumn("language_row", row_number().over(language_row))
        ppa5=ppa5.withColumn("marital_row", row_number().over(marital_row))
        ppa5=ppa5.withColumn("middle_row", row_number().over(middle_row))
        ppa5=ppa5.withColumn("religion_row", row_number().over(religion_row))
        ppa5=ppa5.withColumn("zip_row", row_number().over(zipcode_row))
        ppa5=ppa5.withColumn("middle_row", row_number().over(middle_row))

        var temp_patientdetail1 = ppa5.filter(ppa5("first_row") === 1 && ppa5("first_name").isNotNull).select( "datasrc", "patientid", "update_date", "first_name", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("FIRST_NAME"))
                .withColumn("localvalue", ppa5("first_name"))
        temp_patientdetail1 = temp_patientdetail1.select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")

        var temp_patientdetail2 = ppa5.filter(ppa5("last_row") === 1 && ppa5("last_name").isNotNull).select("datasrc", "patientid", "update_date", "last_name", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("LAST_NAME"))
                .withColumn("localvalue", ppa5("last_name"))
        temp_patientdetail2 = temp_patientdetail2.select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")

        var temp_patientdetail3 = ppa5.filter(ppa5("gender_row") === 1 && ppa5("gender").isNotNull).select("datasrc", "patientid", "update_date", "gender", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("GENDER"))
                .withColumn("localvalue", ppa5("gender"))
        temp_patientdetail3 = temp_patientdetail3.select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")

        var temp_patientdetail4 = ppa5.filter(ppa5("race_row") === 1 && ppa5("race").isNotNull).select("datasrc", "patientid", "update_date", "race", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("RACE"))
                .withColumn("localvalue", ppa5("race"))
        temp_patientdetail4 = temp_patientdetail4.select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")

        var temp_patientdetail5 = ppa5.filter(ppa5("ethnicity_row") === 1 && ppa5("ethnicity").isNotNull).select( "datasrc", "patientid", "update_date", "ethnicity", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("ETHNICITY"))
                .withColumn("localvalue", ppa5("ethnicity"))
        temp_patientdetail5 = temp_patientdetail5.select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")

        var temp_patientdetail6 = ppa5.filter(ppa5("status_row") === 1 && ppa5("death_ind").isNotNull).select("datasrc", "patientid", "update_date", "death_ind", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("DECEASED"))
                .withColumn("localvalue", ppa5("death_ind"))
        temp_patientdetail6 = temp_patientdetail6.select( "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")


        var temp_patientdetail7 = ppa5.filter(ppa5("ethnic_grp_row") === 1 && ppa5("ethnic_grp").isNotNull).select("datasrc", "patientid",  "update_date", "ethnic_grp", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("ETHNIC_GROUP"))
                .withColumn("localvalue", ppa5("ethnic_grp"))
        temp_patientdetail7 = temp_patientdetail7.select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")


        var temp_patientdetail8 = ppa5.filter(ppa5("language_row") === 1 && ppa5("language").isNotNull).select( "datasrc", "patientid", "update_date", "language", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("LANGUAGE"))
                .withColumn("localvalue", ppa5("language"))
        temp_patientdetail8 = temp_patientdetail8.select( "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")


        var temp_patientdetail9 = ppa5.filter(ppa5("marital_row") === 1 && ppa5("marital").isNotNull).select("datasrc", "patientid", "update_date", "marital", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("MARITAL"))
                .withColumn("localvalue", ppa5("marital"))
        temp_patientdetail9 = temp_patientdetail9.select( "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")



        var temp_patientdetail10 = ppa5.filter(ppa5("middle_row") === 1 && ppa5("middle_name").isNotNull).select( "datasrc", "patientid", "update_date", "middle_name", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("MIDDLE_NAME"))
                .withColumn("localvalue", ppa5("middle_name"))
        temp_patientdetail10 = temp_patientdetail10.select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")



        var temp_patientdetail11 = ppa5.filter(ppa5("religion_row") === 1 && ppa5("religion_cd").isNotNull).select("datasrc", "patientid", "update_date", "religion_cd", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("RELIGION"))
                .withColumn("localvalue", ppa5("religion_cd"))
        temp_patientdetail11 = temp_patientdetail11.select( "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")


        var temp_patientdetail12 = ppa5.filter(ppa5("zip_row") === 1 && ppa5("zipcode").isNotNull).select("datasrc", "patientid", "update_date", "ZIPCODE", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
                .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
                .withColumn("patientdetailtype", lit("ZIPCODE"))
                .withColumn("localvalue", ppa5("ZIPCODE"))
        temp_patientdetail12 = temp_patientdetail12.select("DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "HGPID","GRP_MPI")
        temp_patientdetail1.unionAll(temp_patientdetail2).unionAll(temp_patientdetail3).unionAll(temp_patientdetail4)
                .unionAll(temp_patientdetail2).unionAll(temp_patientdetail3).unionAll(temp_patientdetail4)
                .unionAll(temp_patientdetail5).unionAll(temp_patientdetail6).unionAll(temp_patientdetail7)
                .unionAll(temp_patientdetail8).unionAll(temp_patientdetail9).unionAll(temp_patientdetail10)
                .unionAll(temp_patientdetail11).unionAll(temp_patientdetail12)
    }

}

//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")