package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

class PatientcustomattributeEpisode (config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patassessment", "episode", "zh_pat_pcp", "zh_providerattr", "cdr.map_predicate_values"
               ,"pat_attr_crosswalk")

  columnSelect = Map(
    "patassessment" -> List("PAT_ENC_CSN_ID", "MEAS_VALUE", "FLO_MEAS_ID", "PAT_ID", "RECORDED_TIME", "ENTRY_TIME","FSD_ID"),
    "episode" -> List("PAT_LINK_ID", "EPISODE_ID", "START_DATE", "END_DATE", "L_UPDATE_INST_DTTM", "PAT_LINK_ID", "SUM_BLK_TYPE_ID", "STATUS_C"),
    "pat_attr_crosswalk" -> List("MHC_MAPPED_VALUE","MHC_SOURCE_VALUE","ATTRIBUTE")
  )


  beforeJoin = Map(
    "patassessment" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val flo_meas_id_1 = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "EPISODE", "PATIENT_ATTRIBUTE", "EPISODE", "FLO_MEAS_ID_CH002950")
      val flo_meas_id_2 = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "EPISODE", "PATIENT_ATTRIBUTE", "EPISODE", "FLO_MEAS_ID_CH002949")
      val fil = df1.filter("FLO_MEAS_ID in (" + flo_meas_id_1 + "," + flo_meas_id_2 + ")")

      val list_flo_meas_id_1 = mpvList1(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "EPISODE", "PATIENT_ATTRIBUTE", "EPISODE", "FLO_MEAS_ID_CH002950")
      val list_flo_meas_id_2 = mpvList1(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "EPISODE", "PATIENT_ATTRIBUTE", "EPISODE", "FLO_MEAS_ID_CH002949")

      val addColumn = fil.withColumn("MEAS_VAL_ORD", when(lower(fil("MEAS_VALUE")).isin("rising risk","high risk"), lit("1")).otherwise(lit("0")) )
                         .withColumn("FLOMEASID", when(fil("FLO_MEAS_ID").isin(list_flo_meas_id_1: _*), lit("ATTR6"))
                                                  .when(fil("FLO_MEAS_ID").isin(list_flo_meas_id_2: _*), lit("ATTR5")))
      val groups = Window.partitionBy(addColumn("PAT_ID"),addColumn("FLOMEASID")).orderBy(addColumn("MEAS_VAL_ORD").desc_nulls_last, addColumn("ENTRY_TIME").desc_nulls_last, addColumn("RECORDED_TIME").desc_nulls_last)
      val addColumn1 = addColumn.withColumn("pa1_rn", row_number.over(groups))
      val tbl1 = addColumn1.filter("pa1_rn = 1").drop("pa1_rn")
                           .groupBy("PAT_ID","FSD_ID","PAT_ENC_CSN_ID").pivot("FLOMEASID", Seq("ATTR5","ATTR6")).agg(max("MEAS_VALUE"))
                           .withColumnRenamed("FSD_ID","FSD_ID_1")
                           .select("PAT_ID","PAT_ENC_CSN_ID","ATTR5","ATTR6","FSD_ID_1") // attr6 : program_name; attr5 : status

      val df2 = table("patassessment").repartition(1000)
      val list_flo_meas_id_3 = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "EPISODE", "PATIENT_ATTRIBUTE", "EPISODE", "FLO_MEAS_ID_CH002950B")
      val fil2 = df2.filter("FLO_MEAS_ID in (" + list_flo_meas_id_3 + ")")
      val groups2 = Window.partitionBy(fil2("PAT_ID"),fil2("FLO_MEAS_ID")).orderBy(fil2("ENTRY_TIME").desc_nulls_last, fil2("RECORDED_TIME").desc_nulls_last)
      val t = fil2.withColumn("pa2_rn", row_number.over(groups2))
      val tbl2 = t.filter("pa2_rn = 1").drop("pa2_rn")
        .withColumnRenamed("MEAS_VALUE", "ATTR6_STRAT")
        .withColumnRenamed("FSD_ID","FSD_ID_2")
        .select("ATTR6_STRAT","FSD_ID_2") //attr6_strat : STRATIFICATION

      val joined = tbl1.join(tbl2, tbl1("FSD_ID_1") === tbl2("FSD_ID_2"),"left_outer")

      val tbl3 = joined.select("PAT_ID","PAT_ENC_CSN_ID","ATTR5","ATTR6","ATTR6_STRAT")

      val pat_attr5 = table("pat_attr_crosswalk").filter("attribute = 'STATUS'").withColumnRenamed("MHC_MAPPED_VALUE","MHC_MAPPED_VALUE_ATTR5").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR5")
      val pat_attr6 = table("pat_attr_crosswalk").filter("attribute = 'PROGRAM_NAME'").withColumnRenamed("MHC_MAPPED_VALUE","MHC_MAPPED_VALUE_ATTR6").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR6")
      val pat_attr6_strat = table("pat_attr_crosswalk").filter("attribute = 'STRATIFICATION'").withColumnRenamed("MHC_MAPPED_VALUE","MHC_MAPPED_VALUE_ATTR6_STRAT").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR6_STRAT")

      val join1 = tbl3.join(pat_attr5, lower(tbl3("ATTR5")) === lower(pat_attr5("MHC_SOURCE_VALUE_ATTR5")), "left_outer")
        .join(pat_attr6, lower(tbl3("ATTR6")) === lower(pat_attr6("MHC_SOURCE_VALUE_ATTR6")),"left_outer")
        .join(pat_attr6_strat, lower(tbl3("ATTR6_STRAT")) === lower(pat_attr6_strat("MHC_SOURCE_VALUE_ATTR6_STRAT")),"left_outer")

      val addColumn2 = join1.withColumn("ATTR6_2", when(join1("MHC_MAPPED_VALUE_ATTR6").isNotNull, join1("MHC_MAPPED_VALUE_ATTR6")).otherwise(join1("ATTR6")))
        .withColumn("ATTR6_STRAT_2", when(join1("MHC_MAPPED_VALUE_ATTR6_STRAT").isNotNull, join1("MHC_MAPPED_VALUE_ATTR6_STRAT")).otherwise(join1("ATTR6_STRAT")))
        .withColumn("ATTR5_2", when(join1("MHC_MAPPED_VALUE_ATTR5").isNotNull, join1("MHC_MAPPED_VALUE_ATTR5")).otherwise(join1("ATTR5")))
        .select("ATTR5_2","ATTR6_2","ATTR6_STRAT_2","PAT_ENC_CSN_ID","PAT_ID").distinct

      val sel1 = addColumn2.select("PAT_ENC_CSN_ID","PAT_ID","ATTR5_2")

      val sel2 = addColumn2.select("PAT_ID","ATTR6_2","ATTR6_STRAT_2")

      val fpiv1 = unpivot(
        Seq("ATTR6_2","ATTR6_STRAT_2"),
        Seq("CH002950","CH002950B"), typeColumnName = "CUI"
      )

      val fpiv2 = fpiv1("ATTR6_3", sel2)
      val fpiv3 = fpiv2.filter("ATTR6_3 is not null and cui = 'CH002950'").select("PAT_ID").withColumnRenamed("PAT_ID","PAT_ID_1").distinct
      val addColumn3 = fpiv2.join(fpiv3, fpiv3("PAT_ID_1") === fpiv2("PAT_ID"), "inner").withColumn("ATTR6_4",concat_ws(", ", collect_list(fpiv2("ATTR6_3")).over(Window.partitionBy("PAT_ID").orderBy("CUI"))))
      val addColumn4 = addColumn3.groupBy("PAT_ID_1").agg(max(addColumn3("ATTR6_4")).as("ATTR6_5"))
      val sel3 = addColumn4.select("PAT_ID_1","ATTR6_5").distinct

      sel1.join(sel3, sel3("PAT_ID_1") === sel1("PAT_ID"), "inner")
        .select("PAT_ID","PAT_ENC_CSN_ID","ATTR5_2","ATTR6_5")
        .filter("pat_id is not null")
    }),
    "episode" -> ((df: DataFrame) => {
      val list_start_end_dt = translate(lit(mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "EPISODE", "PATIENT_ATTRIBUTE", "EPISODE", "START_END_DATES")), "'", "")
      val df1 = df.repartition(500)
      val fil = df1.filter("PAT_LINK_ID IS NOT NULL AND SUM_BLK_TYPE_ID in ('42','43') and STATUS_C <> '3'")
      val addColumn =  fil.withColumn("ENDDATE", when(fil("END_DATE").isNull, lit("CURRENT")).otherwise(substring(fil("END_DATE"),1,10)))
        .withColumn("STARTDATE",when(fil("START_DATE").isNull, lit("CURRENT")).otherwise(substring(fil("START_DATE"),1,10)))
        .withColumn("use_start_end", list_start_end_dt)
      val groups = Window.partitionBy(addColumn("PAT_LINK_ID")).orderBy(addColumn("L_UPDATE_INST_DTTM").desc)
      val t = addColumn.withColumn("ep_rownumber", row_number.over(groups))
      t.filter("ep_rownumber = 1 and use_start_end = 'Y'")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("episode")
      .join(dfs("patassessment"), dfs("episode")("PAT_LINK_ID") === dfs("patassessment")("PAT_ID"), "full_outer")
  }


  afterJoin = (df: DataFrame) => {
    val fpiv1 = unpivot(
      Seq("STARTDATE","ENDDATE", "ATTR5_2","ATTR6_5"),
      Seq("CH002786", "CH002787", "CH002949","CH002950"), typeColumnName = "ATTRIBUTEVALUETYPE"
    )
    val fpiv2 = fpiv1("ATTRIBUTEVALUE", df)
   fpiv2.filter("ATTRIBUTEVALUE <> ''")
     .withColumn("PATIENTID", coalesce(fpiv2("PAT_LINK_ID"), fpiv2("PAT_ID")))
     .select("PATIENTID","ATTRIBUTEVALUETYPE","ATTRIBUTEVALUE")
     .distinct()

  }

  map = Map(
   "DATASRC" -> literal("episode"),
   "PATIENTID" -> mapFrom("PATIENTID"),
    "ATTRIBUTE_TYPE_CUI" -> mapFrom("ATTRIBUTEVALUETYPE"),
    "ATTRIBUTE_VALUE" -> mapFrom("ATTRIBUTEVALUE")
   )

}