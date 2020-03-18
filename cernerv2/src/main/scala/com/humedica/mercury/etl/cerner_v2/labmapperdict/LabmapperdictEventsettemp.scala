package com.humedica.mercury.etl.cerner_v2.labmapperdict

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Created by abendiganavale on 9/13/18.
  */
class LabmapperdictEventsettemp(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_v500_event_set_explode"
    ,"zh_v500_event_code"
    ,"cdr.zcm_obstype_code"
    ,"cdr.map_predicate_values"
  )

  cacheMe = true

  columns = List("EVENT_CD","EVENT_CD_DISP","EVENT_CD_DESCR","UPDT_DT_TM")

  columnSelect = Map(
    "zh_v500_event_set_explode" -> List("EVENT_CD","UPDT_DT_TM","EVENT_SET_CD"),
    "zh_v500_event_code" -> List("EVENT_CD","EVENT_CD_DISP","EVENT_CD_DESCR","UPDT_DT_TM"),
    "cdr.zcm_obstype_code" -> List("GROUPID", "DATASRC", "OBSCODE", "OBSTYPE")
  )

  beforeJoin = Map(
    "zh_v500_event_set_explode" -> ((df: DataFrame) => {
      val list_event_set_cd = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "CLINICAL_EVENT", "LABRESULT", "ZH_V500_EVENT_SET_EXPLODE", "EVENT_SET_CD")
      val zcm = table("cdr.zcm_obstype_code").filter("GROUPID='"+ config(GROUP) +"' AND OBSTYPE = 'LABRESULT'").select("OBSCODE")
      val fil1 = df.join(zcm, df("EVENT_CD") === zcm("OBSCODE"), "left_outer")
                   .filter("EVENT_SET_CD in (" + list_event_set_cd + ") or EVENT_CD = OBSCODE")
      val zh_code1 = table("zh_v500_event_code").withColumnRenamed("EVENT_CD","EVENT_CODE").withColumnRenamed("UPDT_DT_TM", "UPDATE_DATE")
      val join1 = fil1.join(zh_code1, fil1("EVENT_CD") === zh_code1("EVENT_CODE"), "left_outer")
      val groups1 = Window.partitionBy(join1("EVENT_CD")).orderBy(join1("UPDT_DT_TM").desc_nulls_last, join1("UPDATE_DATE").desc_nulls_last)
      val tbl1 = join1.withColumn("rownum", row_number.over(groups1))
                      .select("EVENT_CD","EVENT_CD_DISP","EVENT_CD_DESCR","UPDT_DT_TM","rownum")

      val list_event_cd = mpvClause(table ("cdr.map_predicate_values"), config (GROUP), config (CLIENT_DS_ID), "CLINICAL_EVENT", "LABRESULT", "ZH_V500_EVENT_CODE", "EVENT_CD")
      val zh_code2 = table("zh_v500_event_code").filter("EVENT_CD in (" + list_event_cd + ")")
      val zh_event2 = table("zh_v500_event_set_explode").withColumnRenamed("EVENT_CD","EVENT_CD1").withColumnRenamed("UPDT_DT_TM","UPDT_DT_TM1")
      //val join2 = zh_code2.join(zh_event2, zh_code2("EVENT_CD") === zh_event2("EVENT_CD1") && zh_event2("EVENT_CD1").isNull , "left_outer")
      val join2 = zh_code2.join(zh_event2, zh_code2("EVENT_CD") === zh_event2("EVENT_CD1"), "left_anti")
      val groups2 = Window.partitionBy(join2("EVENT_CD")).orderBy(join2("UPDT_DT_TM").desc_nulls_last)
      val tbl2 = join2.withColumn("rownum", row_number.over(groups2))
                      .select("EVENT_CD","EVENT_CD_DISP","EVENT_CD_DESCR","UPDT_DT_TM","rownum")

      tbl1.union(tbl2)
          .filter("rownum = 1").drop("rownum")
          .select("EVENT_CD","EVENT_CD_DISP","EVENT_CD_DESCR","UPDT_DT_TM")

    })
  )

  map = Map(
    "EVENT_CD" -> mapFrom("EVENT_CD"),
    "EVENT_CD_DISP" -> mapFrom("EVENT_CD_DISP"),
    "EVENT_CD_DESCR" -> mapFrom("EVENT_CD_DESCR"),
    "UPDT_DT_TM" -> mapFrom("UPDT_DT_TM")
  )

}
