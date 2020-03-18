package com.humedica.mercury.etl.epic_v2.patientcustomattribute

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
  * Created by abendiganavale on 4/26/18.
  */
class PatientcustomattributeSmrtdtaelem(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("smrtdta_elem",
    "cdr.map_predicate_values",
    "pat_attr_crosswalk")

  columnSelect = Map(
    "smrtdta_elem" -> List("PAT_LINK_ID", "SMRTDTA_ELEM_VALUE", "ELEMENT_ID", "VALUE_LINE", "CONTACT_SERIAL_NUM", "UPDATE_DATE"),
    "pat_attr_crosswalk" -> List("MHC_MAPPED_VALUE", "MHC_SOURCE_VALUE", "ATTRIBUTE")
  )

  beforeJoin = Map(
    "smrtdta_elem" -> ((df: DataFrame) => {
      val df1 = df.repartition()

      val groups = Window.partitionBy(df1("ELEMENT_ID"), df1("PAT_LINK_ID"), df1("VALUE_LINE"), df1("CONTACT_SERIAL_NUM")).orderBy(df1("UPDATE_DATE").desc)
      val t = df1.withColumn("rn", row_number.over(groups))

      val list_element_id_1 = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SMRTDTA_ELEM", "PATIENT_ATTRIBUTE", "SMRTDTA_ELEM", "ELEMENT_ID_CH002950")
      val fil1 = t.filter("rn = 1 AND PAT_LINK_ID IS NOT NULL AND ELEMENT_ID in (" + list_element_id_1 + ")")
      val addColumn1 = fil1.withColumn("ELEM_VALUE_1", regexp_replace(fil1("SMRTDTA_ELEM_VALUE"), "\"", ""))
      val tbl1 = addColumn1.filter("elem_value_1 is not null").withColumnRenamed("ELEM_VALUE_1", "ATTR6")
                           .select("PAT_LINK_ID", "ATTR6")

      val list_element_id_2 = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SMRTDTA_ELEM", "PATIENT_ATTRIBUTE", "SMRTDTA_ELEM", "ELEMENT_ID_CH002949")
      val fil2 = t.filter("rn = 1 AND PAT_LINK_ID IS NOT NULL AND ELEMENT_ID in (" + list_element_id_2 + ")")
      val addColumn2 = fil2.withColumn("ELEM_VALUE_2",regexp_replace(fil2("SMRTDTA_ELEM_VALUE"), "\"", ""))
      val tbl2 = addColumn2.filter("elem_value_2 is not null").withColumnRenamed("ELEM_VALUE_2", "ATTR5")
                           .withColumnRenamed("PAT_LINK_ID","PAT_LINK_ID2")
                           .select("PAT_LINK_ID2", "ATTR5")

      val list_element_id_3 = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "SMRTDTA_ELEM", "PATIENT_ATTRIBUTE", "SMRTDTA_ELEM", "ELEMENT_ID_CH002950B")
      val fil3 = t.filter("rn = 1 AND PAT_LINK_ID IS NOT NULL AND ELEMENT_ID in (" + list_element_id_3 + ")").drop("rn")
      val addColumn3 = fil3.withColumn("ELEM_VALUE_3",regexp_replace(fil3("SMRTDTA_ELEM_VALUE"), "\"", ""))
      val tbl3 = addColumn3.filter("elem_value_3 is not null").withColumnRenamed("ELEM_VALUE_3", "ATTR6_STRAT")
                           .withColumnRenamed("PAT_LINK_ID","PAT_LINK_ID3")
                           .select("PAT_LINK_ID3", "ATTR6_STRAT")

      val joined = tbl1.join(tbl2, tbl1("PAT_LINK_ID") === tbl2("PAT_LINK_ID2"), "full_outer")
          .join(tbl3, tbl1("PAT_LINK_ID") === tbl3("PAT_LINK_ID3"),"left_outer")

      val tbl4 = joined.select("PAT_LINK_ID", "ATTR5", "ATTR6", "ATTR6_STRAT").distinct()

      val pat_attr5 = table("pat_attr_crosswalk").filter("attribute = 'STATUS'").withColumnRenamed("MHC_MAPPED_VALUE", "MHC_MAPPED_VALUE_ATTR5").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR5")
      val pat_attr6 = table("pat_attr_crosswalk").filter("attribute = 'PROGRAM_NAME'").withColumnRenamed("MHC_MAPPED_VALUE", "MHC_MAPPED_VALUE_ATTR6").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR6")
      val pat_attr6_strat = table("pat_attr_crosswalk").filter("attribute = 'STRATIFICATION'").withColumnRenamed("MHC_MAPPED_VALUE", "MHC_MAPPED_VALUE_ATTR6_STRAT").withColumnRenamed("MHC_SOURCE_VALUE", "MHC_SOURCE_VALUE_ATTR6_STRAT")

      val join1 = tbl4.join(pat_attr5, lower(tbl4("ATTR5")) === lower(pat_attr5("MHC_SOURCE_VALUE_ATTR5")), "left_outer")
        .join(pat_attr6, lower(tbl4("ATTR6")) === lower(pat_attr6("MHC_SOURCE_VALUE_ATTR6")), "left_outer")
        .join(pat_attr6_strat, lower(tbl4("ATTR6_STRAT")) === lower(pat_attr6_strat("MHC_SOURCE_VALUE_ATTR6_STRAT")), "left_outer")

      val addColumn4 = join1.withColumn("ATTR6_2", when(join1("MHC_MAPPED_VALUE_ATTR6").isNotNull, join1("MHC_MAPPED_VALUE_ATTR6")).otherwise(join1("ATTR6")))
        .withColumn("ATTR6_STRAT_2", when(join1("MHC_MAPPED_VALUE_ATTR6_STRAT").isNotNull, join1("MHC_MAPPED_VALUE_ATTR6_STRAT")).otherwise(join1("ATTR6_STRAT")))
        .withColumn("ATTR5_2", when(join1("MHC_MAPPED_VALUE_ATTR5").isNotNull, join1("MHC_MAPPED_VALUE_ATTR5")).otherwise(join1("ATTR5")))
        .select("ATTR5_2", "ATTR6_2", "ATTR6_STRAT_2", "PAT_LINK_ID").distinct

      val sel1 = addColumn4.select("PAT_LINK_ID", "ATTR5_2")

      val sel2 = addColumn4.select("PAT_LINK_ID", "ATTR6_2", "ATTR6_STRAT_2")

      val fpiv1 = unpivot(
        Seq("ATTR6_2", "ATTR6_STRAT_2"),
        Seq("CH002950", "CH002950B"), typeColumnName = "CUI"
      )

      val fpiv2 = fpiv1("ATTR6_3", sel2)
      val fpiv3 = fpiv2.filter("ATTR6_3 is not null and cui = 'CH002950'").select("PAT_LINK_ID").withColumnRenamed("PAT_LINK_ID", "PAT_ID_1").distinct
      val addColumn5 = fpiv2.join(fpiv3, fpiv3("PAT_ID_1") === fpiv2("PAT_LINK_ID"), "inner").withColumn("ATTR6_4", concat_ws(", ", collect_list(fpiv2("ATTR6_3")).over(Window.partitionBy("PAT_LINK_ID").orderBy("CUI"))))
      val addColumn6 = addColumn5.groupBy("PAT_ID_1").agg(max(addColumn5("ATTR6_4")).as("ATTR6_5"))
      val sel3 = addColumn6.select("PAT_ID_1", "ATTR6_5").distinct

      val join2 = sel1.join(sel3, sel3("PAT_ID_1") === sel1("PAT_LINK_ID"), "inner").select("PAT_LINK_ID", "ATTR5_2", "ATTR6_5")

      val fpiv4 = unpivot(
        Seq("ATTR5_2", "ATTR6_5"),
        Seq("CH002949", "CH002950"), typeColumnName = "ATTRIBUTEVALUETYPE"
      )
      val fpiv5 = fpiv4("ATTRIBUTEVALUE", join2)

      fpiv5.filter("ATTRIBUTEVALUE <> '' and PAT_LINK_ID IS NOT NULL").select("PAT_LINK_ID", "ATTRIBUTEVALUETYPE", "ATTRIBUTEVALUE").distinct()
    })
  )

  map = Map(
    "DATASRC" -> literal("smrtdta_elem"),
    "PATIENTID" -> mapFrom("PAT_LINK_ID"),
    "ATTRIBUTE_TYPE_CUI" -> mapFrom("ATTRIBUTEVALUETYPE"),
    "ATTRIBUTE_VALUE" -> mapFrom("ATTRIBUTEVALUE")
  )

}