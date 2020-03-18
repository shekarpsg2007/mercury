package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._


/**
  * Created by abendiganavale on 6/11/18.
  */

class PatientcustomattributeEpisodeoh(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("episode", "smrtdta_elem"
                ,"pat_attr_crosswalk","cdr.map_predicate_values")


  columnSelect = Map(
    "episode" -> List("PAT_LINK_ID","EPISODE_ID","START_DATE","END_DATE","SUM_BLK_TYPE_ID","L_UPDATE_INST_DTTM","STATUS_C"),
    "smrtdta_elem" -> List("PAT_LINK_ID","RECORD_ID_VARCHAR","RECORD_ID_NUMERIC","ELEMENT_ID","SMRTDTA_ELEM_VALUE"
                           ,"CUR_VALUE_DATETIME","UPDATE_DATE"),
    "pat_attr_crosswalk" -> List("MHC_MAPPED_VALUE","MHC_SOURCE_VALUE","ATTRIBUTE")
  )


  beforeJoin = Map(
    "episode" -> ((df: DataFrame) => {
      val df1 = df.repartition(1000)
      val groups = Window.partitionBy(df1("PAT_LINK_ID"),df1("SUM_BLK_TYPE_ID")).orderBy(df1("START_DATE").asc_nulls_last)
      val groups1 = Window.partitionBy(df1("PAT_LINK_ID"),df1("SUM_BLK_TYPE_ID")).orderBy(df1("L_UPDATE_INST_DTTM").desc_nulls_last, df1("END_DATE").desc_nulls_last)
      val addColumn1 = df1.withColumn("START_DT", first("START_DATE").over(groups))
                         .withColumn("END_DT", first("END_DATE").over(groups1))
                         .withColumn("EPISODETYPE", when(df1("SUM_BLK_TYPE_ID") === lit("19"), lit("Chronic"))
                                                   .when(df1("SUM_BLK_TYPE_ID") === lit("20"), lit("Transition"))
                                                   .when(df1("SUM_BLK_TYPE_ID") === lit("21"), lit("Acute")))
      val groups2 = Window.partitionBy(addColumn1("PAT_LINK_ID"),addColumn1("EPISODE_ID")).orderBy(addColumn1("L_UPDATE_INST_DTTM").desc_nulls_last)
      val addColumn2 = addColumn1.withColumn("epi_rn", row_number.over(groups2))
      val list_status_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "INCLUSION_EPISODE","PATIENT_ATTRIBUTE","EPISODE","STATUS_C")
      val epi = addColumn2.filter("epi_rn = 1 AND SUM_BLK_TYPE_ID IN ('19','20','21') AND STATUS_C IN (" + list_status_c + ")").drop("epi_rn")
                .withColumnRenamed("L_UPDATE_INST_DTTM","UPDATE_DATE_EPI")

      val df2 = table("smrtdta_elem").repartition(1000)

      val addColumn3 = df2.withColumn("JOINID", coalesce(df2("RECORD_ID_VARCHAR"),df2("RECORD_ID_NUMERIC")))
                          .withColumn("UPDATE_DATE_SMRT", when(df2("CUR_VALUE_DATETIME") > df2("UPDATE_DATE"), df2("CUR_VALUE_DATETIME")).otherwise(df2("UPDATE_DATE")))
      val groups3 = Window.partitionBy(addColumn3("PAT_LINK_ID"),addColumn3("ELEMENT_ID"),addColumn3("JOINID")).orderBy(addColumn3("CUR_VALUE_DATETIME").desc_nulls_last,addColumn3("UPDATE_DATE").desc_nulls_last)
      val addColumn4 = addColumn3.withColumn("smrt_rn", row_number.over(groups3))

      val list_element_id_CH002950A = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CH002950A","PATIENT_ATTRIBUTE","SMRTDTA_ELEM","ELEMENT_ID")
      val list_element_id_CH002950_DEC = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CH002950_DEC","PATIENT_ATTRIBUTE","SMRTDTA_ELEM","ELEMENT_ID")
      val list_element_id_CH002949_GRADA = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CH002949_GRADA","PATIENT_ATTRIBUTE","SMRTDTA_ELEM","ELEMENT_ID")
      val list_element_id_CH002949_GRADB = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CH002949_GRADB","PATIENT_ATTRIBUTE","SMRTDTA_ELEM","ELEMENT_ID")
      val list_element_id_CH002949_UNENR = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "CH002949_UNENR","PATIENT_ATTRIBUTE","SMRTDTA_ELEM","ELEMENT_ID")

      val smrt = addColumn4.filter("smrt_rn = 1 AND ELEMENT_ID IN (" + list_element_id_CH002950A + "," + list_element_id_CH002950_DEC + "," + list_element_id_CH002949_GRADA + "," + list_element_id_CH002949_GRADB + "," + list_element_id_CH002949_UNENR + ")").drop("smrt_rn")

      //ACTIVE
      val tbl1 = epi.filter("status_c = '1'").withColumn("ATTR5", lit("Enrolled")).withColumnRenamed("PAT_LINK_ID", "PATIENTID").withColumnRenamed("UPDATE_DATE_EPI","UPDATE_DT")
        .select("PATIENTID","EPISODE_ID","START_DT","END_DT","EPISODETYPE","UPDATE_DT","ATTR5")
      //RESOLVED
      val tbl2 = epi.filter("status_c = '2'").withColumn("ATTR5", lit("Resolved - No Status")).withColumnRenamed("PAT_LINK_ID", "PATIENTID").withColumnRenamed("UPDATE_DATE_EPI","UPDATE_DT")
        .select("PATIENTID","EPISODE_ID","START_DT","END_DT","EPISODETYPE","UPDATE_DT","ATTR5")

      val ep3 = epi.withColumnRenamed("PAT_LINK_ID","PAT_LINK_ID_EPI3")
      val smrt1 = smrt.filter("ELEMENT_ID IN (" + list_element_id_CH002949_GRADA + "," + list_element_id_CH002949_GRADB + ")").withColumnRenamed("PAT_LINK_ID","PAT_LINK_ID_SMRT1")
      val join3 = ep3.join(smrt1, ep3("PAT_LINK_ID_EPI3") === smrt1("PAT_LINK_ID_SMRT1") && ep3("EPISODE_ID") === smrt1("JOINID"), "inner")
      val tbl3 = join3.withColumn("ATTR5", when((concat_ws("", lit("'"),join3("ELEMENT_ID"),lit("'")) === list_element_id_CH002949_GRADA) && (lower(join3("SMRTDTA_ELEM_VALUE")) like "%yes%"), "Graduated" )
        .when((concat_ws("", lit("'"),join3("ELEMENT_ID"),lit("'")) === list_element_id_CH002949_GRADB) && join3("SMRTDTA_ELEM_VALUE").isNotNull, "Graduated"))
        .withColumnRenamed("PAT_LINK_ID_EPI3","PATIENTID").withColumnRenamed("UPDATE_DATE_SMRT","UPDATE_DT")
        .select("PATIENTID","EPISODE_ID","START_DT","END_DT","EPISODETYPE","UPDATE_DT","ATTR5")

      val ep4 = epi.withColumnRenamed("PAT_LINK_ID","PAT_LINK_ID_EPI4")
      val smrt2 = smrt.filter("ELEMENT_ID in (" + list_element_id_CH002949_UNENR + ")").withColumnRenamed("PAT_LINK_ID","PAT_LINK_ID_SMRT2")
      val join4 = ep4.join(smrt2, ep4("PAT_LINK_ID_EPI4") === smrt2("PAT_LINK_ID_SMRT2") && ep4("EPISODE_ID") === smrt2("JOINID"), "inner")
      val tbl4 = join4.withColumn("ATTR5", when((concat_ws("", lit("'"),join4("ELEMENT_ID"),lit("'")) === list_element_id_CH002949_UNENR) && join4("SMRTDTA_ELEM_VALUE").isNotNull, concat_ws("", lit("Unenrolled - "),join4("SMRTDTA_ELEM_VALUE"))))
        .withColumnRenamed("PAT_LINK_ID_EPI4","PATIENTID").withColumnRenamed("UPDATE_DATE_SMRT","UPDATE_DT")
        .select("PATIENTID","EPISODE_ID","START_DT","END_DT","EPISODETYPE","UPDATE_DT","ATTR5")

      val smrt3 = smrt.filter("ELEMENT_ID in (" + list_element_id_CH002950_DEC + ")").withColumnRenamed("UPDATE_DATE_SMRT","UPDATE_DT").withColumnRenamed("PAT_LINK_ID","PATIENTID")
      val tbl5 = smrt3.withColumn("EPISODETYPE", when((concat_ws("", lit("'"),smrt3("ELEMENT_ID"),lit("'")) === list_element_id_CH002950_DEC) && (lower(smrt3("SMRTDTA_ELEM_VALUE")) like "%acute%"), "Acute")
        .when((concat_ws("", lit("'"),smrt3("ELEMENT_ID"),lit("'")) === list_element_id_CH002950_DEC) && (lower(smrt3("SMRTDTA_ELEM_VALUE")) like "%chronic%"), "Chronic")
        .when((concat_ws("", lit("'"),smrt3("ELEMENT_ID"),lit("'")) === list_element_id_CH002950_DEC) && (lower(smrt3("SMRTDTA_ELEM_VALUE")) like "%transition%"), "Transition"))
        .withColumn("ATTR5", lit("Declined"))
        .withColumn("EPISODE_ID",lit(null))
        .withColumn("START_DT",lit(null))
        .withColumn("END_DT",lit(null))
        .select("PATIENTID","EPISODE_ID","START_DT","END_DT","EPISODETYPE","UPDATE_DT","ATTR5")

      val uni1 = tbl1.union(tbl2).union(tbl3).union(tbl4).union(tbl5)

      val riskdeter = smrt.filter("ELEMENT_ID IN (" + list_element_id_CH002950A + ")")
        .withColumnRenamed("SMRTDTA_ELEM_VALUE", "ATTR6_STRAT")
        .withColumnRenamed("PAT_LINK_ID","PAT_LINK_ID_SMRT")
      val join5 = uni1.join(riskdeter, uni1("PATIENTID") === riskdeter("PAT_LINK_ID_SMRT") && uni1("EPISODE_ID") === riskdeter("JOINID"), "left_outer")

      val groups4 = Window.partitionBy(join5("PATIENTID"),join5("EPISODETYPE")).orderBy(join5("UPDATE_DT").desc_nulls_last,join5("ATTR5").asc_nulls_last)
      val addColumn5 = join5.filter("ATTR5 is not null").withColumn("attr5_rn", row_number.over(groups4))
      val fil = addColumn5.filter("attr5_rn = 1")
        .withColumnRenamed("EPISODETYPE", "ATTR6")
        .withColumn("ENDDATE", when(addColumn5("END_DT").isNull, lit("CURRENT")).otherwise(substring(addColumn5("END_DT"),1,10)))
        .withColumn("STARTDATE", substring(addColumn5("START_DT"),1,10))
        .select("PATIENTID","EPISODE_ID","STARTDATE","ENDDATE","ATTR6","ATTR6_STRAT","ATTR5","UPDATE_DATE")

      //Incorporating MHC ranking for cases where patients are affiliated with 3 different programs.
      // get rank1

      val attr5_ord = fil.filter("ATTR6 in ('Chronic','Acute')")
                         .withColumn("RANK", lit("1"))
                         .withColumn("ATTR5_ORDBY", when(fil("ATTR5") === lit("Enrolled"), lit("1"))
                                                    .when(fil("ATTR5") === lit("Graduated"), lit("2"))
                                                    .when(fil("ATTR5") === lit("Declined"), lit("3"))
                                                    .otherwise(lit("4")))

      val groups5 = Window.partitionBy(attr5_ord("PATIENTID")).orderBy(attr5_ord("ATTR5_ORDBY").asc,attr5_ord("STARTDATE").desc_nulls_last)
      val rnk1 = attr5_ord.withColumn("rnk1", row_number.over(groups5))

      val rank1 = rnk1.filter("rnk1 = 1").drop("rnk1")
        .select("PATIENTID","STARTDATE","ENDDATE","ATTR5","ATTR6","ATTR6_STRAT","RANK")

      //get rank2
      val rank2 = fil.filter("ATTR6 in ('Transition')").withColumn("RANK", lit("2"))
        .select("PATIENTID","STARTDATE","ENDDATE","ATTR5","ATTR6","ATTR6_STRAT","RANK")

      //get final dataset
      val uni5 = rank1.union(rank2)
      val groups6 = Window.partitionBy(uni5("PATIENTID")).orderBy(uni5("RANK").asc)
      val addColumn6 = uni5.withColumn("rnk", row_number.over(groups6))
      val tbl6 = addColumn6.filter("rnk = 1").drop("rnk")
        .withColumn("ATTR_5", regexp_replace(addColumn6("ATTR5"),"\"",""))
        .withColumn("ATTR_6", regexp_replace(addColumn6("ATTR6"),"\"",""))
        .withColumn("ATTR_6_STRAT", regexp_replace(addColumn6("ATTR6_STRAT"),"\"",""))
        .select("PATIENTID","STARTDATE","ENDDATE","ATTR_5","ATTR_6","ATTR_6_STRAT")

      val pat_attr5 = table("pat_attr_crosswalk").filter("attribute = 'STATUS'").withColumnRenamed("MHC_MAPPED_VALUE","MHC_MAPPED_VALUE_ATTR5").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR5")
      val pat_attr6 = table("pat_attr_crosswalk").filter("attribute = 'PROGRAM_NAME'").withColumnRenamed("MHC_MAPPED_VALUE","MHC_MAPPED_VALUE_ATTR6").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR6")
      val pat_attr6_strat = table("pat_attr_crosswalk").filter("attribute = 'STRATIFICATION'").withColumnRenamed("MHC_MAPPED_VALUE","MHC_MAPPED_VALUE_ATTR6_STRAT").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR6_STRAT")

      val join6 = tbl6.join(pat_attr5, lower(tbl6("ATTR_5")) === lower(pat_attr5("MHC_SOURCE_VALUE_ATTR5")), "left_outer")
        .join(pat_attr6, lower(tbl6("ATTR_6")) === lower(pat_attr6("MHC_SOURCE_VALUE_ATTR6")),"left_outer")
        .join(pat_attr6_strat, lower(tbl6("ATTR_6_STRAT")) === lower(pat_attr6_strat("MHC_SOURCE_VALUE_ATTR6_STRAT")),"left_outer")

      val addColumn7 = join6.withColumn("ATTR6_2", when(join6("MHC_MAPPED_VALUE_ATTR6").isNotNull, join6("MHC_MAPPED_VALUE_ATTR6")).otherwise(join6("ATTR_6")))
        .withColumn("ATTR6_STRAT_2", when(join6("MHC_MAPPED_VALUE_ATTR6_STRAT").isNotNull, join6("MHC_MAPPED_VALUE_ATTR6_STRAT")).otherwise(join6("ATTR_6_STRAT")))
        .withColumn("ATTR5_2", when(join6("MHC_MAPPED_VALUE_ATTR5").isNotNull, join6("MHC_MAPPED_VALUE_ATTR5")).otherwise(join6("ATTR_5")))
        .select("PATIENTID","STARTDATE","ENDDATE","ATTR_5","ATTR5_2","ATTR6_2","ATTR6_STRAT_2").distinct

      val addColumn8 = addColumn7.withColumn("ATTR6_3", when(addColumn7("ATTR6_STRAT_2").isNotNull, concat_ws("",addColumn7("ATTR6_2"),lit(" - "),addColumn7("ATTR6_STRAT_2"))).otherwise(addColumn7("ATTR6_2")))
        .select("PATIENTID","STARTDATE","ENDDATE","ATTR_5","ATTR5_2","ATTR6_3")


      val fpiv1 = unpivot(
        Seq("STARTDATE","ENDDATE","ATTR5_2","ATTR6_3"),
        Seq("CH002786","CH002787","CH002949","CH002950"),typeColumnName = "ATTRIBUTEVALUETYPE"
      )

      fpiv1("ATTRIBUTEVALUE", addColumn8).select("PATIENTID","ATTRIBUTEVALUE","ATTRIBUTEVALUETYPE")

    })
  )

  join = noJoin()

  map = Map(
    "DATASRC" -> literal("episode_oh"),
    "PATIENTID" -> mapFrom("PATIENTID"),
    "ATTRIBUTE_TYPE_CUI" -> mapFrom("ATTRIBUTEVALUETYPE"),
    "ATTRIBUTE_VALUE" -> mapFrom("ATTRIBUTEVALUE")
  )

  afterMap = (df: DataFrame) => {
    df.filter("ATTRIBUTE_VALUE <> '' and PATIENTID IS NOT NULL")
  }

}

