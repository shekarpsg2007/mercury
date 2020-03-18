package com.humedica.mercury.etl.fdr.zzzassessment

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


class AssessmentObservation (config: Map[String, String]) extends EntitySource(config: Map[String, String]){


  tables=List("Observation:"+config("EMR")+"@Observation", "cdr.zcm_obstype_code", "cdr.unit_conversion",
    "cdr.map_smoking_status", "cdr.map_smoking_cessation", "cdr.map_tobacco_use", "cdr.map_tobacco_cessation", "cdr.map_bmi_followup", "cdr.map_bp_followup", "cdr.map_depression_followup",
    "cdr.map_aco_exclusion", "cdr.map_alcohol", "cdr.map_exercise_level", "cdr.map_cad_plan_of_care", "cdr.map_clin_pathways", "cdr.map_diab_eye_exam", "cdr.map_hospice_ind",
    "mpitemp:fdr.mpi.MpiPatient")



  columnSelect = Map(
    "cdr.unit_conversion" -> List("SRC_UNIT", "DEST_UNIT", "CONV_FACT"))


  columns=List("CLIENT_DS_ID", "MPI", "SOURCE", "ASSESSMENT_TYPE", "ASSESSMENT_DTM", "TEXT_VALUE", "NUMERIC_VALUE", "QUALITATIVE_VALUE")



  beforeJoin = Map(
    "cdr.zcm_obstype_code" -> renameColumn("OBSCODE", "LOCALCODE"),
    "cdr.map_smoking_status" -> ((df: DataFrame) => {
      df.filter("GROUPID = '"+config(GROUP)+"'").withColumnRenamed("MNEMONIC", "MNEMONIC_smoking")
        .withColumnRenamed("CUI", "CUI_smoking")
    }),
    "cdr.map_smoking_cessation" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'")
        .withColumnRenamed("CUI", "CUI_cessation").withColumnRenamed("LOCAL_CODE", "LOCAL_CODE_cessation")
    }),
    "cdr.map_tobacco_use" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'")
        .withColumnRenamed("CUI", "CUI_tobacco").withColumnRenamed("LOCALCODE", "LOCALCODE_tobacco")
    }),
    "cdr.map_tobacco_cessation" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_tob_cessation")
        .withColumnRenamed("CUI", "CUI_tob_cessation")
    }),
    "cdr.map_bmi_followup" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_bmi")
        .withColumnRenamed("CUI", "CUI_bmi")
    }),
    "cdr.map_bp_followup" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_bp")
        .withColumnRenamed("CUI", "CUI_bp")
    }),
    "cdr.map_depression_followup" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_depression")
        .withColumnRenamed("CUI", "CUI_depression")
    }),
    "cdr.map_aco_exclusion" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_aco")
        .withColumnRenamed("CUI", "CUI_aco")
    }),
    "cdr.map_alcohol" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("MNEMONIC", "MNEMONIC_alcohol")
        .withColumnRenamed("CUI", "CUI_alcohol")
    }),
    "cdr.map_exercise_level" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("MNEMONIC", "MNEMONIC_exercise")
        .withColumnRenamed("CUI", "CUI_exercise")
    }),
    "cdr.map_cad_plan_of_care" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_cad")
        .withColumnRenamed("CUI", "CUI_cad")
    }),
    "cdr.map_clin_pathways" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCALCODE", "LOCALCODE_clin")
        .withColumnRenamed("CUI", "CUI_clin")
    }),
    "cdr.map_diab_eye_exam" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("LOCAL_CODE", "LOCAL_CODE_diab")
        .withColumnRenamed("HTS_CODE", "HTS_CODE_diab")
    }),
    "cdr.map_hospice_ind" -> ((df: DataFrame) => {
      df.filter("GROUPID = '" + config(GROUP) + "'").withColumnRenamed("MNEMONIC", "MNEMONIC_hospice")
        .withColumnRenamed("CUI", "CUI_hospice")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("Observation")
      .join(dfs("cdr.zcm_obstype_code"), Seq("GROUPID", "OBSTYPE", "DATASRC", "LOCALCODE"), "inner")
      .join(dfs("cdr.map_smoking_status"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_smoking_status")("MNEMONIC_smoking"), "left_outer")
      .join(dfs("cdr.map_smoking_cessation"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_smoking_cessation")("LOCAL_CODE_cessation"), "left_outer")
      .join(dfs("cdr.map_tobacco_use"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_tobacco_use")("LOCALCODE_tobacco"), "left_outer")
      .join(dfs("cdr.map_tobacco_cessation"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_tobacco_cessation")("LOCALCODE_tob_cessation"), "left_outer")
      .join(dfs("cdr.map_bmi_followup"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_bmi_followup")("LOCALCODE_bmi"), "left_outer")
      .join(dfs("cdr.map_bp_followup"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_bp_followup")("LOCALCODE_bp"), "left_outer")
      .join(dfs("cdr.map_depression_followup"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_depression_followup")("LOCALCODE_depression"), "left_outer")
      .join(dfs("cdr.map_aco_exclusion"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_aco_exclusion")("LOCALCODE_aco"), "left_outer")
      .join(dfs("cdr.map_alcohol"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_alcohol")("MNEMONIC_alcohol"), "left_outer")
      .join(dfs("cdr.map_exercise_level"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_exercise_level")("MNEMONIC_exercise"), "left_outer")
      .join(dfs("cdr.map_cad_plan_of_care"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_cad_plan_of_care")("LOCALCODE_cad"), "left_outer")
      .join(dfs("cdr.map_clin_pathways"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_clin_pathways")("LOCALCODE_clin"), "left_outer")
      .join(dfs("cdr.map_diab_eye_exam"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_diab_eye_exam")("LOCAL_CODE_diab"), "left_outer")
      .join(dfs("cdr.map_hospice_ind"), dfs("Observation")("LOCALRESULT") === dfs("cdr.map_hospice_ind")("MNEMONIC_hospice"), "left_outer")
      .join(dfs("mpitemp"), Seq("PATIENTID", "GROUPID", "CLIENT_DS_ID"), "inner")
  }







  afterJoin = (df: DataFrame) => {
    val unit = table("cdr.unit_conversion")
    val unit1 = unit.filter("SRC_UNIT = 'CH001212'").withColumnRenamed("CONV_FACT", "CONV_FACT1").withColumnRenamed("DEST_UNIT", "DEST_UNIT1").select("CONV_FACT1", "DEST_UNIT1")
    val unit2 = unit.filter("SRC_UNIT = 'CH000136'").withColumnRenamed("CONV_FACT", "CONV_FACT2").withColumnRenamed("DEST_UNIT", "DEST_UNIT2").select("CONV_FACT2", "DEST_UNIT2")
    val unit3 = unit.filter("SRC_UNIT = 'CH000143'").withColumnRenamed("CONV_FACT", "CONV_FACT3").withColumnRenamed("DEST_UNIT", "DEST_UNIT3").select("CONV_FACT3", "DEST_UNIT3")
    val unit4 = unit.filter("SRC_UNIT = 'CH000143'").withColumnRenamed("CONV_FACT", "CONV_FACT4").withColumnRenamed("DEST_UNIT", "DEST_UNIT4").select("CONV_FACT4", "DEST_UNIT4")
    val unit5 = unit.filter("SRC_UNIT = 'CH000143'").withColumnRenamed("CONV_FACT", "CONV_FACT5").withColumnRenamed("DEST_UNIT", "DEST_UNIT5").select("CONV_FACT5", "DEST_UNIT5")
    df.join(unit1).join(unit2).join(unit3).join(unit4).join(unit5)
  }


  def apply_unit_conv_function(v_numeric: Column, p_function_applied: Column) = {
    val value = when(isnull(v_numeric) || (v_numeric === 0), v_numeric)
      .otherwise(
        when(p_function_applied === "LINEAR", v_numeric)
          .when(p_function_applied === "LOG10", log(10, v_numeric))
          .when(p_function_applied === "LOG2", log(2, v_numeric))
          .when(p_function_applied === "TEMPTOC", (v_numeric - 32) * 5 / 9)
          .when(p_function_applied === "TEMP_60_LOGIC",
            when(v_numeric < 60, v_numeric)
              .otherwise((v_numeric - 32) * 5 / 9)
          )
          .otherwise(v_numeric))
    round(value, 2)

  }

  def convert_multiple_units(p_obstype : Column, p_localresult : Column, p_local_unit_cui : Column, p_obstype_std_unit : Column, p_dest_unit1 : Column,
                             p_dest_unit2 : Column, p_dest_unit3 : Column, p_dest_unit4 : Column, p_dest_unit5 : Column, p_conv_fact1 : Column,
                             p_conv_fact2 : Column, p_conv_fact3 : Column, p_conv_fact4 : Column
                             ,p_conv_fact5 : Column) = {
    val obs1 = upper(substring(p_localresult,1,100))
    val obs2 = when(p_obstype === lit("HT") && p_local_unit_cui === "CH002722",
    {
      val vhtft = regexp_extract(obs1, "^[^F''f]*", 0)
      val ht_ft_cm = when(vhtft !== obs1,
      {
        val htft = when(vhtft.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), vhtft)
        round(htft.multiply(when(p_dest_unit1 === p_obstype_std_unit, p_conv_fact1)),5)
      }).otherwise(lit(0))
      val vhtin = ltrim(rtrim(regexp_extract(regexp_extract(obs1, "^[^I\"''''i]*", 0), "[^Tt]*$", 0)))
      val vhtin1 = when(vhtin !== obs1, substring(vhtin,1,2)).otherwise(vhtin)
      val htincm = when(vhtin1.isNotNull,
      {
        val htin = when(vhtin1.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), vhtin1)
        round(htin.multiply(when(p_dest_unit2 === p_obstype_std_unit, p_conv_fact2)),5)
      }).otherwise(lit(0))
      ht_ft_cm + htincm
    }).when(p_obstype === lit("WT") && p_local_unit_cui === "CH002723",
    {
      val obs1a = regexp_replace(regexp_replace(obs1, "_", ""), ",", "")
      val obs1b = when(locate("LBS. ", obs1a) > 0, regexp_replace(obs1a, "LBS.", "LBS.")).otherwise(obs1a)
      val vwtlbst = regexp_extract(obs1b, "^[^L''l#Pp]*", 0)
      val wtlbs = when(vwtlbst.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), vwtlbst)
      val wtlbskg = round(wtlbs.multiply(when(p_dest_unit3 === p_obstype_std_unit, p_conv_fact3)),5)
      val vwtoz = ltrim(rtrim(regexp_extract(regexp_extract(obs1b, "[^Ss#bBdD]*$", 0), "^[^O\"''''o]*", 0)))
      val vwtoz1 = when(vwtoz !== obs1b, substring(obs1b,1,2)).otherwise(vwtoz)
      val wtozkg = when(vwtoz1.isNotNull && (vwtoz1 !== obs1b),
      {
        val wtoz = when(vwtoz1.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), vwtoz1)
        round(vwtoz1.multiply(when(p_dest_unit4 === p_obstype_std_unit, p_conv_fact4)),5)
      }).otherwise(lit(0))
      wtlbskg + wtozkg
    }).when(p_obstype.isin("CH002752", "CH002753", "CH002754") && p_local_unit_cui === "CH002773",
    {
      val obs2a = regexp_replace(regexp_replace(obs1, " ", ""), "S", "")
      val gawk = regexp_extract(obs2a, "^[^W''w]*", 0)
      val gada = when((locate("\\/", obs2a) > 0) && (locate("7", regexp_extract(obs2a, "[^/]*$", 0)) > 0),substring(gawk,3,1)).otherwise(lit(0))
      val gawk1 = when((locate("\\/", obs2a) > 0) && (locate("7", regexp_extract(obs2a, "[^/]*$", 0)) > 0),substring(gawk,1,2)).otherwise(gawk)
      val gaspl = when((locate("\\/", obs2a) > 0) && (locate("7", regexp_extract(obs2a, "[^/]*$", 0)) > 0),lit("true"))
      val gada1 = when(locate("\\+", gawk1) > 0, ltrim(rtrim(substring_index(gawk1, "\\+", -1)))).otherwise(gada)
      val gawk2 = when(locate("\\+", gawk1) > 0, ltrim(rtrim(substring_index(gawk1, "\\+", 1)))).otherwise(gawk1)
      val gaspl1 = when(locate("\\+", gawk1) > 0, lit("true")).otherwise(gaspl)
      val gawkn = when(gawk2.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), gawk2).otherwise(lit(0))
      val gada2 = when(gaspl1 !== lit("true"), ltrim(rtrim(regexp_extract(regexp_extract(gada1, "[^WwKk]*$", 0), "^[^D''d]*", 0)))).otherwise(gada1)
      val gada3 = when(gada2 === obs2a, lit(0)).otherwise(gada2)
      val gadan = when(gada3.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$") && (gada3 !== "0"),
        round(gada3.multiply(when(p_dest_unit4 === p_obstype_std_unit, p_conv_fact4)),5)
      ).otherwise(lit(0))
      gawkn + gadan
    })
    obs2
  }


  def std_obsresult1(p_obsregex : Column, p_localresult : Column, p_obs_subs : Column, p_datatype : Column, p_local_unit_cui : Column, p_obstype : Column, p_obstype_std_unit : Column, p_conv_factor : Column
                     ,p_obsconvfactor: Column, p_function_applied : Column, p_begin_range: Column, p_end_range : Column, p_round_prec : Column, p_dest_unit1 : Column,
                     p_dest_unit2 : Column, p_dest_unit3 : Column, p_dest_unit4 : Column, p_dest_unit5 : Column, p_conv_fact1 : Column,
                     p_conv_fact2 : Column, p_conv_fact3 : Column, p_conv_fact4 : Column
                     ,p_conv_fact5 : Column) = {
    val obs1 = substring(p_localresult, 1, 250)
    val obs2 = when(p_obsregex.isNotNull, p_obs_subs).otherwise(obs1)
    val obs3 = when(lower(p_datatype).isin("n", "f"), {
      val obs3a = when(p_local_unit_cui !== lit("CH002773"), regexp_replace(regexp_replace(obs2, "\\+", ""), "\\/", "")).otherwise(obs2)
      val lowrng = when(p_obstype.isin("CH001846", "CH001999", "PAIN"),
        when(substring(substring_index(obs3a, "-", 1), 1, 8).rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), substring(substring_index(obs3a, "-", 1), 1, 8)).otherwise(null)).otherwise(null)
      val hirng = when(p_obstype.isin("CH001846", "CH001999", "PAIN"), {
        val hirnga = substring(reverse(substring_index(reverse(obs3a), "-", 1)), 1, 8)
        hirnga
      }).otherwise(null)
      val numeric = coalesce(hirng, lowrng, when(obs3a.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), obs3a))
      val numeric1 = when(p_local_unit_cui.isNotNull && p_obstype_std_unit.isNotNull, {
        val numeric1a =
          when((p_local_unit_cui !== "CH002722") && (p_local_unit_cui !== "CH002723") && (p_local_unit_cui !== "CH002773"), {
            val numeric1aa = when(p_obstype_std_unit !== p_local_unit_cui, {
              val conf = coalesce(p_conv_factor, p_obsconvfactor)
              val numeric1aaa = numeric.multiply(conf)
              val numeric1aab = apply_unit_conv_function(numeric1aaa, when(isnull(p_function_applied), "LINEAR").otherwise(p_function_applied))
              numeric1aab
            }).otherwise(numeric)
            numeric1aa
          }).when(p_local_unit_cui.isin("CH002722", "CH002723", "CH002773"),
          {
            val numeric1aba = convert_multiple_units(p_obstype, numeric, p_local_unit_cui, p_obstype_std_unit,p_dest_unit1, p_dest_unit2, p_dest_unit3, p_dest_unit4, p_dest_unit5,
              p_conv_fact1, p_conv_fact2, p_conv_fact3, p_conv_fact4, p_conv_fact5)
            numeric1aba
          }).otherwise(numeric)

        numeric1a
      }).otherwise(numeric)
      // expr("round(numeric2, p_round_prec)")
      val numeric2 = round(numeric1, 2)
      numeric2
    }).when(lower(p_datatype) === lit("p"),
    {
      when(obs2.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), obs2*100).otherwise(obs2)
    }).otherwise(obs2)
    obs3
  }


  map = Map(
    "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID"),
    "MPI" -> mapFrom("MPI"),
    "SOURCE" -> mapFrom("DATASRC"),
    "ASSESSMENT_TYPE" -> ((col: String, df: DataFrame) => df.withColumn(col, when(isnull(df("CUI")), "CH999999").otherwise(df("CUI")))),
    "ASSESSMENT_DTM" -> mapFrom("OBSDATE"),
    "TEXT_VALUE" -> ((col: String, df: DataFrame) => df.withColumn(col,substring(df("LOCALRESULT"),1,100))),
    "NUMERIC_VALUE" -> ((col: String, df: DataFrame) => df.withColumn(col,when(std_obsresult1(df("OBSREGEX"), df("LOCALRESULT"), expr("regexp_extract(LOCALRESULT, OBSREGEX, 0)"), df("DATATYPE"), df("LOCALUNIT_CUI")
      , df("OBSTYPE"), df("obstype_std_units"), df("CONV_FACT"), df("OBSCONVFACTOR"), df("FUNCTION_APPLIED"), df("BEGIN_RANGE"), df("END_RANGE"), df("ROUND_PREC"), df("DEST_UNIT1"), df("DEST_UNIT2"),
      df("DEST_UNIT3"), df("DEST_UNIT4"), df("DEST_UNIT5"), df("CONV_FACT1"), df("CONV_FACT2") ,df("CONV_FACT3"), df("CONV_FACT4"), df("CONV_FACT5")).rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
      std_obsresult1(df("OBSREGEX"), df("LOCALRESULT"), expr("regexp_extract(LOCALRESULT, OBSREGEX, 0)"), df("DATATYPE"), df("LOCALUNIT_CUI")
        , df("OBSTYPE"), df("obstype_std_units"), df("CONV_FACT"), df("OBSCONVFACTOR"), df("FUNCTION_APPLIED"), df("BEGIN_RANGE"), df("END_RANGE"), df("ROUND_PREC"), df("DEST_UNIT1"), df("DEST_UNIT2"),
        df("DEST_UNIT3"), df("DEST_UNIT4"), df("DEST_UNIT5"), df("CONV_FACT1"), df("CONV_FACT2") ,df("CONV_FACT3"), df("CONV_FACT4"), df("CONV_FACT5"))
    ).otherwise(null))),
    "QUALITATIVE_VALUE" -> ((col: String, df: DataFrame) => df.withColumn(col,
      when(df("MNEMONIC_smoking").isNotNull, df("CUI_smoking"))
        .when(df("LOCAL_CODE_cessation").isNotNull, df("CUI_cessation"))
        .when(df("LOCALCODE_tobacco").isNotNull, df("CUI_tobacco"))
        .when(df("LOCALCODE_tob_cessation").isNotNull, df("CUI_tob_cessation"))
        .when(df("LOCALCODE_bmi").isNotNull, df("CUI_bmi"))
        .when(df("LOCALCODE_bp").isNotNull, df("CUI_bp"))
        .when(df("LOCALCODE_depression").isNotNull, df("CUI_depression"))
        .when(df("LOCALCODE_aco").isNotNull, df("CUI_aco"))
        .when(df("MNEMONIC_alcohol").isNotNull, df("CUI_alcohol"))
        .when(df("MNEMONIC_exercise").isNotNull, df("CUI_exercise"))
        .when(df("LOCALCODE_cad").isNotNull, df("CUI_cad"))
        .when(df("LOCALCODE_clin").isNotNull, df("CUI_clin"))
        .when(df("LOCAL_CODE_diab").isNotNull, df("HTS_CODE_diab"))
        .when(df("MNEMONIC_hospice").isNotNull, df("CUI_hospice"))
        .otherwise(null)
    ))
  )






}