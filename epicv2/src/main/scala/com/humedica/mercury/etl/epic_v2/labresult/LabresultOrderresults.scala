package com.humedica.mercury.etl.epic_v2.labresult

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



class LabresultOrderresults(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("orderresults",
    "generalorders",
    "zh_clarity_comp",
    "zh_result_flag",
    "zh_result_status",
    "cdr.map_unit",
    "cdr.unit_remap",
    "cdr.metadata_lab",
    "cdr.unit_conversion",
    "zh_spec_type",
    "zh_spec_source")


  columnSelect = Map(
    "orderresults" -> List("ORDER_PROC_ID", "COMPONENT_ID", "RESULT_STATUS_C", "RESULT_FLAG_C","PAT_ENC_CSN_ID","UPDATE_DATE", "PAT_ID",
      "ORD_VALUE", "REFERENCE_UNIT", "REFERENCE_LOW", "REFERENCE_HIGH", "RESULT_TIME", "LINE"),
    "generalorders" -> List("ORDER_PROC_ID", "PAT_ID", "PAT_ENC_CSN_ID", "SPECIMEN_SOURCE_C", "SPECIMEN_TYPE_C", "ORDER_DESCRIPTION", "SPECIMN_TAKEN_TIME", "ORDERING_DATE", "UPDATE_DATE"),
    "zh_clarity_comp" -> List("COMPONENT_ID", "NAME"),
    "zh_result_flag" -> List("RESULT_FLAG_C", "NAME"),
    "zh_result_status" -> List("RESULT_STATUS_C", "NAME"),
    "zh_spec_source" -> List("SPECIMEN_SOURCE_C", "NAME"),
    "zh_spec_type" -> List("SPECIMEN_TYPE_C", "NAME")
  )

  beforeJoin = Map(
    "orderresults" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      df1.filter("RESULT_TIME is not null and order_proc_id is not null")
    }),
    "generalorders" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("ORDER_PROC_ID")).orderBy(df1("UPDATE_DATE").desc)
      df1.withColumn("rn", row_number.over(groups))
        .filter("rn=1 and order_proc_id is not null")
        .withColumnRenamed("PAT_ID", "PAT_ID_g")
        .withColumnRenamed("PAT_ENC_CSN_ID", "PAT_ENC_CSN_ID_g")
        .withColumnRenamed("UPDATE_DATE","UPDATE_DATE_g")
    }),
    "zh_clarity_comp" -> renameColumn("NAME", "NAME_zcc"),
    "zh_result_flag" -> renameColumn("NAME", "NAME_zrf"),
    "zh_result_status" -> renameColumn("NAME", "NAME_zrs"),
    "zh_spec_source" -> renameColumn("NAME", "NAME_zss"),
    "zh_spec_type" -> renameColumn("NAME", "NAME_zst")
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("orderresults")
      .join(dfs("generalorders"),Seq("ORDER_PROC_ID"), "left_outer")
      .join(dfs("zh_result_status"),Seq("RESULT_STATUS_C"), "left_outer")
      .join(dfs("zh_clarity_comp"),Seq("COMPONENT_ID"), "left_outer")
      .join(dfs("zh_result_flag"),Seq("RESULT_FLAG_C"), "left_outer")
      .join(dfs("zh_spec_source"),Seq("SPECIMEN_SOURCE_C"), "left_outer")
      .join(dfs("zh_spec_type"),Seq("SPECIMEN_TYPE_C"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    val df1 = df.repartition(1000)
    val groups = Window.partitionBy(df1("ORDER_PROC_ID"), df1("LINE")).orderBy(df1("UPDATE_DATE").desc, df1("RESULT_TIME").desc)
    val addColumn = df1.withColumn("rn", row_number.over(groups))
      .withColumn("LOCALRESULT_25", when(!df("ORD_VALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
        when(locate(" ", df("ORD_VALUE"), 25) === 0, expr("substr(ORD_VALUE,1,length(ORD_VALUE))"))
          .otherwise(expr("substr(ORD_VALUE,1,locate(' ', ORD_VALUE, 25))"))).otherwise(null))
      .withColumn("LOCALRESULT_NUMERIC", when(df("ORD_VALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), df("ORD_VALUE")).otherwise(null))
    addColumn.filter("rn=1  and (RESULT_STATUS_C is null or RESULT_STATUS_C in ('3', '4', '-1'))  and ((PAT_ID is not null and PAT_ID <> '-1') or (PAT_ID_g is not null and PAT_ID_g <> '-1'))")
  }



  map = Map(
    "DATASRC" -> literal("orderresults"),
    "LABRESULTID" -> concatFrom(Seq("ORDER_PROC_ID", "LINE"), delim="-"),
    "LABORDERID" -> mapFrom("ORDER_PROC_ID"),
    "ENCOUNTERID" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, coalesce(when(df("PAT_ENC_CSN_ID") === lit("-1"), lit(null)).otherwise(df("PAT_ENC_CSN_ID")),
        when(df("PAT_ENC_CSN_ID_g") === lit("-1"), lit(null)).otherwise(df("PAT_ENC_CSN_ID_g"))))
    }),
    "PATIENTID" -> ((col:String,df:DataFrame) => {
      df.withColumn(col, coalesce(when(df("PAT_ID") === lit("-1"), lit(null)).otherwise(df("PAT_ID")),
        when(df("PAT_ID_g") === lit("-1"), lit(null)).otherwise(df("PAT_ID_g"))))
    }),
    "DATECOLLECTED" -> mapFrom("SPECIMN_TAKEN_TIME"),
    "DATEAVAILABLE" ->  mapFrom("RESULT_TIME"),
    "RESULTSTATUS" -> mapFrom("NAME_zrs"),
    "LOCALRESULT" -> mapFrom("ORD_VALUE"),
    "LOCALCODE" -> mapFrom("COMPONENT_ID"),
    "LOCALNAME" -> mapFrom("NAME_zcc"),
    "NORMALRANGE" -> concatFrom(Seq("REFERENCE_LOW", "REFERENCE_HIGH"), delim="-"),
    "LOCALUNITS" -> mapFrom("REFERENCE_UNIT"),
    "STATUSCODE" -> mapFrom("RESULT_STATUS_C"),
    "LABRESULT_DATE" -> mapFrom("RESULT_TIME"),
    "LOCALUNITS_INFERRED" -> labresults_extract_uom(),
    "LOCALRESULT_INFERRED" -> extract_value(),
    "RESULTTYPE" -> labresults_extract_resulttype("ORD_VALUE","NAME_zrf"),
    "LABORDEREDDATE" -> mapFrom("ORDERING_DATE"),
    "RELATIVEINDICATOR" -> labresults_extract_relativeindicator(),
    "LOCALSPECIMENTYPE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("SPECIMEN_SOURCE_C").isNotNull && !df("SPECIMEN_SOURCE_C").isin("-1","0"), concat(lit(config(CLIENT_DS_ID)+"."), df("NAME_zss")))
          .when(df("SPECIMEN_TYPE_C").isNotNull, concat(lit(config(CLIENT_DS_ID)+".type."),df("NAME_zst"))).otherwise(null))
    }),
    "LOCALTESTNAME" -> mapFrom("ORDER_DESCRIPTION")
  )

  afterMap = (df_tmp: DataFrame) => {
    val map_unit = table("cdr.map_unit").withColumnRenamed("CUI", "CUI_MU").withColumnRenamed("LOCALCODE", "LOCALCODE_MU")
    val unit_remap = table("cdr.unit_remap").withColumnRenamed("UNIT", "UNIT_UR").filter("GROUPID='"+config(GROUP)+"'").drop("GROUPID")
    val metadata_lab = table("cdr.metadata_lab").withColumnRenamed("UNIT", "UNIT_ML")
    val unit_conversion = table("cdr.unit_conversion")

    val df = df_tmp.repartition(1000)
    val df_with_cdr = df.join(map_unit, lower(coalesce(df("LOCALUNITS"), df("LOCALUNITS_INFERRED"))) === map_unit("LOCALCODE_MU"), "left_outer")
      .join(unit_remap, df("MAPPEDCODE") === unit_remap("LAB") && (coalesce(map_unit("CUI_MU"), lit("CH999999")) === unit_remap("REMAP")), "left_outer")
      .join(metadata_lab, df("MAPPEDCODE") === metadata_lab("CUI"), "left_outer")
      .join(unit_conversion, unit_conversion("SRC_UNIT") === coalesce(unit_remap("UNIT_UR"), map_unit("CUI_MU")) && (unit_conversion("DEST_UNIT") === metadata_lab("UNIT_ML")), "left_outer")

    df_with_cdr.withColumn("MAPPEDUNITS", when(df("LOCALRESULT_NUMERIC").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), null).otherwise(coalesce(df_with_cdr("UNIT_UR"), df_with_cdr("CUI_MU"))))
  }

  afterJoinExceptions = Map(
    "H641171_EP2_P" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("ORDER_PROC_ID"), df1("LINE")).orderBy(df1("UPDATE_DATE").desc, df1("UPDATE_DATE_g").desc)
      val addColumn = df1.withColumn("rn", row_number.over(groups))
        .withColumn("LOCALRESULT_25", when(!df("ORD_VALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
          when(locate(" ", df("ORD_VALUE"), 25) === 0, expr("substr(ORD_VALUE,1,length(ORD_VALUE))"))
            .otherwise(expr("substr(ORD_VALUE,1,locate(' ', ORD_VALUE, 25))"))).otherwise(null))
        .withColumn("LOCALRESULT_NUMERIC", when(df("ORD_VALUE").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), df("ORD_VALUE")).otherwise(null))
      addColumn.filter("rn=1  and (RESULT_STATUS_C is null or RESULT_STATUS_C in ('3', '4', '-1'))  and ((PAT_ID is not null and PAT_ID <> '-1') or (PAT_ID_g is not null and PAT_ID_g <> '-1'))")
    })
  )

  afterMapExceptions = Map(
    "H302436_EP2" -> ((df_tmp: DataFrame) => {
      val map_unit = table("cdr.map_unit").withColumnRenamed("CUI", "CUI_MU").withColumnRenamed("LOCALCODE", "LOCALCODE_MU")
      val unit_remap = table("cdr.unit_remap").withColumnRenamed("UNIT", "UNIT_UR").filter("GROUPID='"+config(GROUP)+"'").drop("GROUPID")
      val metadata_lab = table("cdr.metadata_lab").withColumnRenamed("UNIT", "UNIT_ML")
      val unit_conversion = table("cdr.unit_conversion")

      val df = df_tmp.repartition(1000)      
      val df_with_cdr = df.join(map_unit, lower(coalesce(df("LOCALUNITS"), df("LOCALUNITS_INFERRED"))) === map_unit("LOCALCODE_MU"), "left_outer")
        .join(unit_remap, df("MAPPEDCODE") === unit_remap("LAB") && (coalesce(map_unit("CUI_MU"), lit("CH999999")) === unit_remap("REMAP")), "left_outer")
        .join(metadata_lab, df("MAPPEDCODE") === metadata_lab("CUI"), "left_outer")
        .join(unit_conversion, unit_conversion("SRC_UNIT") === coalesce(unit_remap("UNIT_UR"), map_unit("CUI_MU")) && (unit_conversion("DEST_UNIT") === metadata_lab("UNIT_ML")), "left_outer")

      df_with_cdr.withColumn("MAPPEDUNITS", when(df("LOCALRESULT_NUMERIC").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), null).otherwise(coalesce(df_with_cdr("UNIT_UR"), df_with_cdr("CUI_MU"))))
        .filter("date_format(labresult_date, 'yyyyMMdd') > '20150216'")
    })
  )
}

// test
// val l = new LabresultOrderresults(cfg) ; val lab = build(l)  ; lab.show