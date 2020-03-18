package com.humedica.mercury.etl.backend.labresult

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


class LabresultBackend(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("labresult", "cdr.unit_refrange_lab_map", "cdr.map_lab_codes", "cdr.map_unit", "cdr.v_metadata_lab",
    "cdr.map_hts_term", "cdr.unit_remap", "cdr.unit_conversion", "cdr.PATIENT_MPI")



  columns=List("GROUPID", "DATASRC","LABRESULTID", "LABORDERID", "FACILITYID", "ENCOUNTERID", "PATIENTID", "DATECOLLECTED", "DATEAVAILABLE", "RESULTSTATUS",
    "LOCALRESULT", "LOCALCODE", "LOCALNAME", "NORMALRANGE", "LOCALUNITS", "STATUSCODE", "LABRESULT_DATE", "MAPPEDCODE", "MAPPEDNAME", "LOCALRESULT_NUMERIC", "LOCALRESULT_INFERRED",
    "RESULTTYPE", "RELATIVEINDICATOR", "LOCALUNITS_INFERRED", "MAPPEDUNITS", "NORMALIZEDVALUE", "LOCALSPECIMENTYPE", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "LOCALTESTNAME", "LABORDEREDDATE",
    "LOCALLISRESOURCE", "MAPPEDLOINC", "MAPPED_QUAL_CODE", "LOCAL_LOINC_CODE")



  beforeJoin = Map(
    "labresult" -> ((df: DataFrame) => {
      df.withColumn("range_low", regexp_extract(substring_index(df("normalrange"), "-", 1), "[0-9]*[.]{0,1}[0-9]*", 0))
        .withColumn("localresult_inferred_c", coalesce(df("localresult_numeric"), df("localresult_inferred")))
        .withColumn("localunits_l", lower(df("localunits")))
        .withColumn("nrml_rng", upper(df("normalrange")))
        .withColumnRenamed("LOCALCODE", "LOCALCODE_lab").drop("HGPID", "GRP_MPI")
    }),
    "cdr.map_lab_codes" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" +config(GROUP)+ "'").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_mlc").withColumnRenamed("CUI", "CUI_mlc").drop("GROUPID")
    }),
    "cdr.map_unit" -> renameColumns(List(("LOCALCODE", "LOCALCODE_mu"), ("CUI", "CUI_mu"))),
    "cdr.v_metadata_lab" -> renameColumns(List(("CUI", "CUI_mdl"), ("UNIT", "UNIT_mdl"))) ,
    "cdr.map_hts_term" -> ((df: DataFrame) => {
      val df1 = df.filter("PREFERRED = 'Y' and TERMINOLOGY_CUI = 'CH002050'")
      df1.groupBy("HTS_CUI").agg(min("TERM_CODE").as("TERM_CODE")).select("HTS_CUI", "TERM_CODE")
    }),
    "cdr.unit_refrange_lab_map" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").drop("GROUPID")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("labresult")
      .join(dfs("cdr.map_lab_codes"),
        (dfs("labresult")("LOCALCODE_lab") === dfs("cdr.map_lab_codes")("MNEMONIC")) &&
          (coalesce(dfs("labresult")("CLIENT_DS_ID"), lit("0")) === coalesce(dfs("cdr.map_lab_codes")("CLIENT_DS_ID_mlc"), lit("0"))),
        "left_outer")
      .join(dfs("cdr.map_unit"),
        coalesce(dfs("labresult")("localunits"), dfs("labresult")("localunits_inferred")) === dfs("cdr.map_unit")("LOCALCODE_mu"), "left_outer")
      .join(dfs("cdr.unit_refrange_lab_map")
        ,dfs("labresult")("LOCALCODE_lab") === dfs("cdr.unit_refrange_lab_map")("LOCAL_CODE")
          &&
          (coalesce(dfs("labresult")("localunits_l"), lit("NULL")) ===
            when(dfs("cdr.unit_refrange_lab_map")("local_units") === lit("ANY"), coalesce(dfs("labresult")("localunits_l"), lit("NULL")))
              .when(dfs("cdr.unit_refrange_lab_map")("local_units") === lit("NULL"), lit("NULL"))
              .otherwise(dfs("cdr.unit_refrange_lab_map")("local_units"))) &&
          (coalesce(dfs("labresult")("localunits_l"), lit("NULL")) ===
            when(dfs("cdr.unit_refrange_lab_map")("local_units") === lit("ANY"), coalesce(dfs("labresult")("localunits_l"), lit("NULL")))
              .when(dfs("cdr.unit_refrange_lab_map")("local_units") === lit("NULL"), lit("NULL"))
              .otherwise(dfs("cdr.unit_refrange_lab_map")("local_units"))) &&
          (when(isnull(dfs("labresult")("nrml_rng")) || dfs("cdr.unit_refrange_lab_map")("localrange_lower") === lit("NULL"), lit("Y"))
            .when(dfs("labresult")("nrml_rng").isNotNull && isnull(dfs("labresult")("range_low"))
              && (dfs("cdr.unit_refrange_lab_map")("localrange_compare") === lit("="))
              && (dfs("labresult")("nrml_rng") === dfs("cdr.unit_refrange_lab_map")("localrange_lower")), lit("Y"))
            .when((dfs("cdr.unit_refrange_lab_map")("localrange_compare") === lit(">"))
              && (dfs("labresult")("range_low") > dfs("cdr.unit_refrange_lab_map")("localrange_lower")) &&
              dfs("cdr.unit_refrange_lab_map")("localrange_compare").isNotNull, lit("Y"))
            .when((dfs("cdr.unit_refrange_lab_map")("localrange_compare") === lit(">="))
              && (dfs("labresult")("range_low") >= dfs("cdr.unit_refrange_lab_map")("localrange_lower")) &&
              dfs("cdr.unit_refrange_lab_map")("localrange_compare").isNotNull, lit("Y"))
            .when((dfs("cdr.unit_refrange_lab_map")("localrange_compare") === lit("<="))
              && (dfs("labresult")("range_low") <= dfs("cdr.unit_refrange_lab_map")("localrange_lower")) &&
              dfs("cdr.unit_refrange_lab_map")("localrange_compare").isNotNull, lit("Y"))
            .when((dfs("cdr.unit_refrange_lab_map")("localrange_compare") === lit("<"))
              && (dfs("labresult")("range_low") < dfs("cdr.unit_refrange_lab_map")("localrange_lower")) &&
              dfs("cdr.unit_refrange_lab_map")("localrange_compare").isNotNull, lit("Y"))
            .when((dfs("cdr.unit_refrange_lab_map")("localrange_compare") === lit("="))
              && (dfs("labresult")("range_low") === dfs("cdr.unit_refrange_lab_map")("localrange_lower")) &&
              dfs("cdr.unit_refrange_lab_map")("localrange_compare").isNotNull, lit("Y"))
            .otherwise(lit("N")) === lit("Y")), "left_outer")
      .join(dfs("cdr.v_metadata_lab"), coalesce(dfs("cdr.unit_refrange_lab_map")("HTS_LAB_CUI"), dfs("cdr.map_unit")("CUI_mu")) === dfs("cdr.v_metadata_lab")("CUI_mdl"), "left_outer")
      .join(dfs("cdr.map_hts_term"), dfs("cdr.map_hts_term")("HTS_CUI") === coalesce(dfs("cdr.unit_refrange_lab_map")("HTS_LAB_CUI"), dfs("cdr.map_lab_codes")("CUI_mlc")), "left_outer")
      .join(dfs("cdr.PATIENT_MPI"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
  }


  afterJoin = (df: DataFrame) => {
    val lgu = table("cdr.unit_remap").filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("lab", "lgu_lab").withColumnRenamed("local_unit", "lgu_local_unit")
      .withColumnRenamed("remap", "lgu_remap").withColumnRenamed("local_lab_code", "lgu_local_lab_code")
      .withColumnRenamed("unit", "lgu_unit").drop("GROUPID")
    val lgulu = table("cdr.unit_remap").filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("lab", "lgulu_lab").withColumnRenamed("local_unit", "lgulu_local_unit")
      .withColumnRenamed("remap", "lgulu_remap").withColumnRenamed("local_lab_code", "lgulu_local_lab_code")
      .withColumnRenamed("unit", "lgulu_unit").drop("GROUPID")
    val ur = table("cdr.unit_remap").filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("lab", "ur_lab").withColumnRenamed("local_unit", "ur_local_unit")
      .withColumnRenamed("remap", "ur_remap").withColumnRenamed("local_lab_code", "ur_local_lab_code")
      .withColumnRenamed("unit", "ur_unit").drop("GROUPID")
    val uc = table("cdr.unit_conversion").withColumnRenamed("src_unit", "uc_src_unit").withColumnRenamed("dest_unit", "uc_dest_unit")
      .withColumnRenamed("hts_test", "uc_hts_test").withColumnRenamed("conv_fact", "uc_conv_fact").withColumnRenamed("function_applied", "uc_function_applied")
      .filter("HTS_TEST = 'CH999999'")
    val ucl = table("cdr.unit_conversion").withColumnRenamed("src_unit", "ucl_src_unit").withColumnRenamed("dest_unit", "ucl_dest_unit")
      .withColumnRenamed("hts_test", "ucl_hts_test").withColumnRenamed("conv_fact", "ucl_conv_fact").withColumnRenamed("function_applied", "ucl_function_applied")
      .filter("HTS_TEST <> 'CH999999'")
    df.join(lgu,
      (coalesce(df("HTS_LAB_CUI"), df("CUI_mlc")) === lgu("lgu_lab")) &&
        (lgu("lgu_local_unit") === lit("NULL")) &&
        (coalesce(df("CUI_mlc"), when(coalesce(df("localunits"), df("localunits_inferred")).isNotNull, lit("CH999990")).otherwise("CH999999")) === lgu("lgu_remap")) &&
        (df("LOCALCODE_lab") === coalesce(lgu("lgu_local_lab_code"), df("LOCALCODE_lab"))) &&
        (coalesce(lgu("lgu_local_lab_code"), lit("NULL")) !== lit("NULL")), "left_outer"
    )
      .join(ur,
        (coalesce(df("HTS_LAB_CUI"), df("CUI_mlc")) === ur("ur_lab")) &&
          (ur("ur_local_unit") === lit("NULL")) &&
          (coalesce(df("CUI_mlc"), when(coalesce(df("localunits"), df("localunits_inferred")).isNotNull, lit("CH999990")).otherwise("CH999999")) === ur("ur_remap")) &&
          (coalesce(ur("ur_local_lab_code"), lit("NULL")) !== lit("NULL")), "left_outer"
      ).join(lgulu,
      (coalesce(df("HTS_LAB_CUI"), df("CUI_mlc")) === lgulu("lgulu_lab")) &&
        (lgulu("lgulu_local_unit") !== lit("NULL")) &&
        (lower(coalesce(df("localunits"), df("localunits_inferred"))) === lgulu("lgulu_local_unit")) &&
        (df("LOCALCODE_lab") === coalesce(lgulu("lgulu_local_lab_code"), df("LOCALCODE_lab"))) &&
        (coalesce(df("LOCALCODE_lab"), lit("a")) === coalesce(lgulu("lgulu_local_lab_code"), lit("a"))), "left_outer"
    )
      .join(uc, (uc("uc_src_unit") === coalesce(lgulu("lgulu_unit"), lgu("lgu_unit"), ur("ur_unit"), df("CUI_mu"))) && (uc("uc_dest_unit") === df("CUI_mdl")), "left_outer")
      .join(ucl, (ucl("ucl_src_unit") === coalesce(lgulu("lgulu_unit"), lgu("lgu_unit"), ur("ur_unit"), df("CUI_mu"))) && (ucl("ucl_dest_unit") === df("CUI_mdl")) &&
        (ucl("ucl_hts_test") === coalesce(df("HTS_LAB_CUI"), df("CUI_mu"))), "left_outer")

  }

  map = Map(
    "GROUPID" -> mapFrom("groupid"),
    "DATASRC" -> mapFrom("datasrc"),
    "CLIENT_DS_ID" -> mapFrom("client_ds_id"),
    "LABRESULTID" -> mapFrom("LABRESULTid"),
    "LABORDERID" -> mapFrom("laborderid"),
    "FACILITYID" -> mapFrom("facilityid"),
    "ENCOUNTERID" -> mapFrom("encounterid"),
    "PATIENTID" -> mapFrom("patientid"),
    "DATECOLLECTED" -> mapFrom("datecollected"),
    "DATEAVAILABLE" -> mapFrom("dateavailable"),
    "RESULTSTATUS" -> mapFrom("resultstatus"),
    "LOCALRESULT" -> mapFrom("localresult"),
    "LOCALCODE" -> mapFrom("LOCALCODE_LAB"),
    "LOCALNAME" -> mapFrom("localname"),
    "NORMALRANGE" -> mapFrom("nrml_rng"),
    "LOCALUNITS" -> mapFrom("localunits"),
    "STATUSCODE" -> mapFrom("statuscode"),
    "LABRESULT_DATE" -> mapFrom("LABRESULT_date"),
    "MAPPEDCODE" -> ((col:String, df:DataFrame) => df.withColumn(col, coalesce(df("hts_lab_cui"), df("cui_mu")))),
    "MAPPEDNAME" -> mapFrom("mappedname"),
    "LOCALRESULT_NUMERIC" -> mapFrom("localresult_numeric"),
    "LOCALRESULT_INFERRED" -> mapFrom("localresult_inferred"),
    "RESULTTYPE" -> mapFrom("resulttype"),
    "RELATIVEINDICATOR" -> mapFrom("relativeindicator"),
    "LOCALUNITS_INFERRED" -> mapFrom("localunits_inferred"),
    "MAPPEDUNITS" -> (( col:String, df:DataFrame) => df.withColumn(col, coalesce(df("lgu_unit"),df("ur_unit"),df("cui_mu")))),
    "NORMALIZEDVALUE" -> (( col:String, df:DataFrame) => {
      val conv_fact = coalesce(df("ucl_conv_fact"), df("uc_conv_fact"))
      df.withColumn(col,
        when(coalesce(df("lgulu_unit"), df("lgu_unit"), df("ur_unit"), df("CUI_MU")) === df("unit_mdl"), df("localresult_inferred"))
          .when(conv_fact.isNotNull,
            {
              val numeric = df("localresult_inferred").multiply(conv_fact)
              val convvalue = apply_unit_conv_function(numeric, coalesce(df("ucl_function_applied"), df("uc_function_applied")))
              convvalue
            })
          .otherwise(null))
    }),
    "LOCALSPECIMENTYPE" -> mapFrom("localspecimentype"),
    "HGPID" -> mapFrom("hgpid"),
    "GRP_MPI" -> mapFrom("grp_mpi"),
    "LOCALTESTNAME" -> mapFrom("localtestname"),
    "LABORDEREDDATE" -> mapFrom("labordereddate"),
    "LOCALLISRESOURCE" -> mapFrom("locallisresource"),
    "MAPPEDLOINC" -> mapFrom("mappedloinc"),
    "LOCAL_LOINC_CODE" -> mapFrom("local_loinc_code"),
    "MAPPED_QUAL_CODE" -> mapFrom("mapped_qual_code")
  )



  def apply_unit_conv_function(v_numeric: Column, p_function_applied: Column) = {
    val value = when(isnull(v_numeric) || (v_numeric === 0), v_numeric)
      .otherwise(
        when(p_function_applied === "LINEAR", v_numeric)
          .when(p_function_applied === "LOG10", log(10, v_numeric))
          .when(p_function_applied === "LOG2", log(2, v_numeric))
          .when(p_function_applied === "TEMPTOC", (v_numeric - 32) * 5 / 9)
          .when(p_function_applied === "60_LOGIC",
            when(v_numeric < 60, v_numeric)
              .otherwise((v_numeric - 32) * 5 / 9)
          )
          .otherwise(v_numeric))
    round(value, 2)



  }


}
