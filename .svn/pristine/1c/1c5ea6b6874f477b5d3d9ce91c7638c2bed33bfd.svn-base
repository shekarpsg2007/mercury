package com.humedica.mercury.etl.cerner_v2.labresult

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class LabresultClinicalevent(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("clinical_event",
    "tempeventset:cerner_v2.labmapperdict.LabmapperdictEventsettemp",
    "ce_specimen_coll",
    "order_action",
    "zh_code_value",
    "cdr.map_predicate_values",
    "cdr.map_unit",
    "cdr.unit_remap",
    "cdr.metadata_lab",
    "cdr.unit_conversion"
  )

  columnSelect = Map(
    "clinical_event" -> List("EVENT_ID","EVENT_CD","RESULT_VAL","EVENT_TAG","PERSON_ID","VERIFIED_DT_TM","CLINSIG_UPDT_DT_TM"
      ,"ENCNTR_ID","ORDER_ID","NORMAL_LOW","NORMAL_HIGH","RESULT_STATUS_CD","EVENT_END_DT_TM","RESOURCE_CD","CATALOG_CD","RESULT_UNITS_CD"
      ,"NORMALCY_CD","UPDT_DT_TM","VALID_UNTIL_DT_TM","RESULT_STATUS_CD"),
    "tempeventset" -> List("EVENT_CD","EVENT_CD_DISP","UPDT_DT_TM"),
    "ce_specimen_coll" -> List("COLLECT_DT_TM", "SOURCE_TYPE_CD","EVENT_ID","UPDT_DT_TM"),
    "order_action" -> List("ORDER_ID","ORDER_DT_TM","ACTION_TYPE_CD"),
    "zh_code_value" -> List("CODE_VALUE","CODE_SET","DESCRIPTION")
  )

  beforeJoin = Map(
    "clinical_event" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      df1.filter("VALID_UNTIL_DT_TM > current_date()")
    }),
    "tempeventset" -> renameColumn("UPDT_DT_TM", "UPDT_DT_TM_ES"),
    "order_action" -> ((df: DataFrame) => { df.repartition(1000) }),
    "ce_specimen_coll" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      df1.withColumnRenamed("UPDT_DT_TM", "UPDT_DT_TM_SPEC")
         .withColumnRenamed("EVENT_ID","EVENT_ID_SPEC")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("clinical_event")
      .join(dfs("tempeventset"), Seq("EVENT_CD"), "inner")
      .join(dfs("ce_specimen_coll"), dfs("clinical_event")("EVENT_ID") === dfs("ce_specimen_coll")("EVENT_ID_SPEC"), "left_outer")
      .join(dfs("order_action"), Seq("ORDER_ID"), "left_outer")
      .join(dfs("zh_code_value"), dfs("clinical_event")("resource_cd") === dfs("zh_code_value")("code_value"),"left_outer")
  }
  
  afterJoin = (df: DataFrame) => {
    val list_result_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "LABRESULT", "CLINICAL_EVENT", "RESULT_STATUS_CD")
    val groups = Window.partitionBy(df("EVENT_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last, df("UPDT_DT_TM_ES").desc_nulls_last, df("UPDT_DT_TM_SPEC").desc_nulls_last)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    val tbl1 = addColumn.filter("rn = 1 and PERSON_ID is not null and EVENT_ID is not null and EVENT_CD is not null and " +
      "(result_status_cd is null or result_status_cd not in (" + list_result_status_cd + "))")

    val zh_resource = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESOURCE")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESOURCE")

    val zh_catalog = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_CAT")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_CAT")

    val zh_res_unit = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESUNIT")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESUNIT")

    val zh_res_status = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESSTATUS")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESSTATUS")

    val zh_normalcy = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_NORMALCY")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_NORMALCY")

    val list_code_set = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SPECIMENTYPE", "LABRESULT", "ZH_CODE_VALUE", "CODE_SET")
    val zh_source = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
      .withColumnRenamed("CODE_VALUE", "CODE_VALUE_SOURCE")
      .withColumnRenamed("DESCRIPTION", "DESCRIPTION_SOURCE")
      .filter("CODE_SET in (" + list_code_set + ")")

    val joined = tbl1.join(zh_resource, tbl1("RESOURCE_CD") === zh_resource("CODE_VALUE_RESOURCE"),"left_outer")
                     .join(zh_catalog, tbl1("CATALOG_CD") === zh_catalog("CODE_VALUE_CAT"),"left_outer")
                     .join(zh_res_unit, tbl1("RESULT_UNITS_CD") === zh_res_unit("CODE_VALUE_RESUNIT"),"left_outer")
                     .join(zh_res_status, tbl1("RESULT_STATUS_CD") === zh_res_status("CODE_VALUE_RESSTATUS"),"left_outer")
                     .join(zh_normalcy, tbl1("NORMALCY_CD") === zh_normalcy("CODE_VALUE_NORMALCY"),"left_outer")
                     .join(zh_source, tbl1("SOURCE_TYPE_CD") === zh_source("CODE_VALUE_SOURCE"),"left_outer")

    val addColumn2 = joined.withColumn("LOCALRESULT_pre", joined("RESULT_VAL"))
    addColumn2.withColumn("LOCALRESULT_25", when(!addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
      when(locate(" ", addColumn2("LOCALRESULT_pre"), 25) === 0, expr("substr(LOCALRESULT_pre,1,length(LOCALRESULT_pre))"))
      .otherwise(expr("substr(LOCALRESULT_pre,1,locate(' ', LOCALRESULT_pre, 25))"))).otherwise(null))
      .withColumn("LOCALRESULT_NUMERIC", when(addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), addColumn2("LOCALRESULT_pre")).otherwise(null))
  }

  map = Map(
    "DATASRC" -> literal("clinical_event"),
    "LABRESULTID" -> mapFrom("EVENT_ID"),
    "LOCALCODE" -> mapFrom("EVENT_CD"),
    "PATIENTID" -> mapFrom("PERSON_ID"),
    "ENCOUNTERID" -> mapFrom("ENCNTR_ID"),
    "LOCALLISRESOURCE" -> mapFrom("DESCRIPTION_RESOURCE"),
    "LOCALNAME" -> mapFrom("EVENT_CD_DISP"),
    "LOCALSPECIMENTYPE" -> mapFrom("DESCRIPTION_SOURCE"), 
    "LOCALTESTNAME" -> mapFrom("DESCRIPTION_CAT"),
    "LOCALUNITS" -> mapFrom("DESCRIPTION_RESUNIT"),
    "NORMALRANGE" -> ((col, df) => df.withColumn(col, concat_ws("", df("NORMAL_LOW"), lit(" - "), df("NORMAL_HIGH")))),
    "RESULTTYPE" -> labresults_extract_resulttype("LOCALRESULT_25","DESCRIPTION_NORMALCY"),
    "STATUSCODE" -> mapFrom("RESULT_STATUS_CD"),
    "DATECOLLECTED" -> cascadeFrom(Seq("COLLECT_DT_TM", "EVENT_END_DT_TM")),
    "LABORDEREDDATE" -> ((col: String, df: DataFrame) => {
        val list_action_type_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "NEW_ORDER_CD", "LABRESULT", "ORDER_ACTION", "ACTION_TYPE_CD")
        df.withColumn(col, when(concat_ws("", lit("'"),df("ACTION_TYPE_CD"),lit("'")).isin(list_action_type_cd), df("ORDER_DT_TM")).otherwise(null))
      }),
    "DATEAVAILABLE" -> cascadeFrom(Seq("VERIFIED_DT_TM", "CLINSIG_UPDT_DT_TM")), 
    "LABORDERID" -> mapFrom("ORDER_ID"),
    "LOCALRESULT" -> mapFrom("LOCALRESULT_pre"),
    "RESULTSTATUS" -> mapFrom("DESCRIPTION_RESSTATUS"),
    "LOCALUNITS_INFERRED" -> labresults_extract_uom(),
    "LOCALRESULT_INFERRED" -> extract_value(),
    "LABRESULT_DATE" -> cascadeFrom(Seq("DATEAVAILABLE","DATECOLLECTED")),
    "RELATIVEINDICATOR" ->labresults_extract_relativeindicator()
  )

  afterJoinExceptions = Map(
    "H984945_CR2" -> ((df: DataFrame) => {
      val list_result_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "LABRESULT", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      val groups = Window.partitionBy(df("EVENT_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last, df("UPDT_DT_TM_ES").desc_nulls_last, df("UPDT_DT_TM_SPEC").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val tbl1 = addColumn.filter("rn = 1 and PERSON_ID is not null and EVENT_ID is not null and EVENT_CD is not null and " +
        "(result_status_cd is null or result_status_cd not in (" + list_result_status_cd + "))")

      val zh_resource = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESOURCE")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESOURCE")

      val zh_catalog = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_CAT")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_CAT")

      val zh_res_unit = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESUNIT")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESUNIT")

      val zh_res_status = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESSTATUS")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESSTATUS")

      val zh_normalcy = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_NORMALCY")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_NORMALCY")

      val list_code_set = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SPECIMENTYPE", "LABRESULT", "ZH_CODE_VALUE", "CODE_SET")
      val zh_source = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_SOURCE")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_SOURCE")
        .filter("CODE_SET in (" + list_code_set + ")")

      val joined = tbl1.join(zh_resource, tbl1("RESOURCE_CD") === zh_resource("CODE_VALUE_RESOURCE"),"left_outer")
                     .join(zh_catalog, tbl1("CATALOG_CD") === zh_catalog("CODE_VALUE_CAT"),"left_outer")
                     .join(zh_res_unit, tbl1("RESULT_UNITS_CD") === zh_res_unit("CODE_VALUE_RESUNIT"),"left_outer")
                     .join(zh_res_status, tbl1("RESULT_STATUS_CD") === zh_res_status("CODE_VALUE_RESSTATUS"),"left_outer")
                     .join(zh_normalcy, tbl1("NORMALCY_CD") === zh_normalcy("CODE_VALUE_NORMALCY"),"left_outer")
                     .join(zh_source, tbl1("SOURCE_TYPE_CD") === zh_source("CODE_VALUE_SOURCE"),"left_outer")

      val addColumn2 = joined.withColumn("LOCALRESULT_pre", coalesce(joined("RESULT_VAL"),joined("EVENT_TAG")))
      addColumn2.withColumn("LOCALRESULT_25", when(!addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
        when(locate(" ", addColumn2("LOCALRESULT_pre"), 25) === 0, expr("substr(LOCALRESULT_pre,1,length(LOCALRESULT_pre))"))
        .otherwise(expr("substr(LOCALRESULT_pre,1,locate(' ', LOCALRESULT_pre, 25))"))).otherwise(null))
        .withColumn("LOCALRESULT_NUMERIC", when(addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), addColumn2("LOCALRESULT_pre")).otherwise(null))
    }),
    "H984186_CR2" -> ((df: DataFrame) => {
      val list_result_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "LABRESULT", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      val groups = Window.partitionBy(df("EVENT_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last, df("UPDT_DT_TM_ES").desc_nulls_last, df("UPDT_DT_TM_SPEC").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val tbl1 = addColumn.filter("rn = 1 and PERSON_ID is not null and EVENT_ID is not null and EVENT_CD is not null and " +
        "(result_status_cd is null or result_status_cd not in (" + list_result_status_cd + "))")

      val zh_resource = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESOURCE")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESOURCE")

      val zh_catalog = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_CAT")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_CAT")

      val zh_res_unit = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESUNIT")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESUNIT")

      val zh_res_status = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESSTATUS")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESSTATUS")

      val zh_normalcy = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_NORMALCY")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_NORMALCY")

      val list_code_set = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SPECIMENTYPE", "LABRESULT", "ZH_CODE_VALUE", "CODE_SET")
      val zh_source = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_SOURCE")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_SOURCE")
        .filter("CODE_SET in (" + list_code_set + ")")

      val joined = tbl1.join(zh_resource, tbl1("RESOURCE_CD") === zh_resource("CODE_VALUE_RESOURCE"),"left_outer")
                     .join(zh_catalog, tbl1("CATALOG_CD") === zh_catalog("CODE_VALUE_CAT"),"left_outer")
                     .join(zh_res_unit, tbl1("RESULT_UNITS_CD") === zh_res_unit("CODE_VALUE_RESUNIT"),"left_outer")
                     .join(zh_res_status, tbl1("RESULT_STATUS_CD") === zh_res_status("CODE_VALUE_RESSTATUS"),"left_outer")
                     .join(zh_normalcy, tbl1("NORMALCY_CD") === zh_normalcy("CODE_VALUE_NORMALCY"),"left_outer")
                     .join(zh_source, tbl1("SOURCE_TYPE_CD") === zh_source("CODE_VALUE_SOURCE"),"left_outer")

      val addColumn2 = joined.withColumn("LOCALRESULT_pre", coalesce(joined("RESULT_VAL"),joined("EVENT_TAG")))
      addColumn2.withColumn("LOCALRESULT_25", when(!addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
        when(locate(" ", addColumn2("LOCALRESULT_pre"), 25) === 0, expr("substr(LOCALRESULT_pre,1,length(LOCALRESULT_pre))"))
        .otherwise(expr("substr(LOCALRESULT_pre,1,locate(' ', LOCALRESULT_pre, 25))"))).otherwise(null))
        .withColumn("LOCALRESULT_NUMERIC", when(addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), addColumn2("LOCALRESULT_pre")).otherwise(null))
    }),
    "H984197_CR2" -> ((df: DataFrame) => {
      val list_result_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "LABRESULT", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      val groups = Window.partitionBy(df("EVENT_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last, df("UPDT_DT_TM_ES").desc_nulls_last, df("UPDT_DT_TM_SPEC").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val tbl1 = addColumn.filter("rn = 1 and PERSON_ID is not null and EVENT_ID is not null and EVENT_CD is not null and " +
        "(result_status_cd is null or result_status_cd not in (" + list_result_status_cd + "))")

      val zh_resource = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESOURCE")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESOURCE")

      val zh_catalog = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_CAT")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_CAT")

      val zh_res_unit = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESUNIT")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESUNIT")

      val zh_res_status = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESSTATUS")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESSTATUS")

      val zh_normalcy = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_NORMALCY")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_NORMALCY")

      val list_code_set = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SPECIMENTYPE", "LABRESULT", "ZH_CODE_VALUE", "CODE_SET")
      val zh_source = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_SOURCE")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_SOURCE")
        .filter("CODE_SET in (" + list_code_set + ")")

      val joined = tbl1.join(zh_resource, tbl1("RESOURCE_CD") === zh_resource("CODE_VALUE_RESOURCE"),"left_outer")
                     .join(zh_catalog, tbl1("CATALOG_CD") === zh_catalog("CODE_VALUE_CAT"),"left_outer")
                     .join(zh_res_unit, tbl1("RESULT_UNITS_CD") === zh_res_unit("CODE_VALUE_RESUNIT"),"left_outer")
                     .join(zh_res_status, tbl1("RESULT_STATUS_CD") === zh_res_status("CODE_VALUE_RESSTATUS"),"left_outer")
                     .join(zh_normalcy, tbl1("NORMALCY_CD") === zh_normalcy("CODE_VALUE_NORMALCY"),"left_outer")
                     .join(zh_source, tbl1("SOURCE_TYPE_CD") === zh_source("CODE_VALUE_SOURCE"),"left_outer")

      val addColumn2 = joined.withColumn("LOCALRESULT_pre", coalesce(joined("RESULT_VAL"),joined("EVENT_TAG")))
      addColumn2.withColumn("LOCALRESULT_25", when(!addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
        when(locate(" ", addColumn2("LOCALRESULT_pre"), 25) === 0, expr("substr(LOCALRESULT_pre,1,length(LOCALRESULT_pre))"))
        .otherwise(expr("substr(LOCALRESULT_pre,1,locate(' ', LOCALRESULT_pre, 25))"))).otherwise(null))
        .withColumn("LOCALRESULT_NUMERIC", when(addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), addColumn2("LOCALRESULT_pre")).otherwise(null))
    }),
    "H984442_CR2" -> ((df: DataFrame) => {
      val list_result_status_cd = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CLINICAL_EVENT", "LABRESULT", "CLINICAL_EVENT", "RESULT_STATUS_CD")
      val groups = Window.partitionBy(df("EVENT_ID")).orderBy(df("UPDT_DT_TM").desc_nulls_last, df("UPDT_DT_TM_ES").desc_nulls_last, df("UPDT_DT_TM_SPEC").desc_nulls_last)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val tbl1 = addColumn.filter("rn = 1 and PERSON_ID is not null and EVENT_ID is not null and EVENT_CD is not null and " +
        "(result_status_cd is null or result_status_cd not in (" + list_result_status_cd + "))")

      val zh_resource = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESOURCE")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESOURCE")

      val zh_catalog = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_CAT")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_CAT")

      val zh_res_unit = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESUNIT")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESUNIT")

      val zh_res_status = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_RESSTATUS")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_RESSTATUS")

      val zh_normalcy = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_NORMALCY")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_NORMALCY")

      val list_code_set = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SPECIMENTYPE", "LABRESULT", "ZH_CODE_VALUE", "CODE_SET")
      val zh_source = table("zh_code_value").select("CODE_VALUE", "DESCRIPTION")
        .withColumnRenamed("CODE_VALUE", "CODE_VALUE_SOURCE")
        .withColumnRenamed("DESCRIPTION", "DESCRIPTION_SOURCE")
        .filter("CODE_SET in (" + list_code_set + ")")

      val joined = tbl1.join(zh_resource, tbl1("RESOURCE_CD") === zh_resource("CODE_VALUE_RESOURCE"),"left_outer")
                     .join(zh_catalog, tbl1("CATALOG_CD") === zh_catalog("CODE_VALUE_CAT"),"left_outer")
                     .join(zh_res_unit, tbl1("RESULT_UNITS_CD") === zh_res_unit("CODE_VALUE_RESUNIT"),"left_outer")
                     .join(zh_res_status, tbl1("RESULT_STATUS_CD") === zh_res_status("CODE_VALUE_RESSTATUS"),"left_outer")
                     .join(zh_normalcy, tbl1("NORMALCY_CD") === zh_normalcy("CODE_VALUE_NORMALCY"),"left_outer")
                     .join(zh_source, tbl1("SOURCE_TYPE_CD") === zh_source("CODE_VALUE_SOURCE"),"left_outer")

      val addColumn2 = joined.withColumn("LOCALRESULT_pre", coalesce(joined("RESULT_VAL"),joined("EVENT_TAG")))
      addColumn2.withColumn("LOCALRESULT_25", when(!addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"),
        when(locate(" ", addColumn2("LOCALRESULT_pre"), 25) === 0, expr("substr(LOCALRESULT_pre,1,length(LOCALRESULT_pre))"))
        .otherwise(expr("substr(LOCALRESULT_pre,1,locate(' ', LOCALRESULT_pre, 25))"))).otherwise(null))
        .withColumn("LOCALRESULT_NUMERIC", when(addColumn2("LOCALRESULT_pre").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), addColumn2("LOCALRESULT_pre")).otherwise(null))
    })
  )
  
  mapExceptions = Map(
        ("H565676_CR2", "DATEAVAILABLE") -> mapFrom("CLINSIG_UPDT_DT_TM")
  )  
}