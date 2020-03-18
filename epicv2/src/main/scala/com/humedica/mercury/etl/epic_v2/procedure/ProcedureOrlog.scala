package com.humedica.mercury.etl.epic_v2.procedure

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ProcedureOrlog(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("or_log_all_proc",
                   "zh_or_proc",
                   "or_log",
                   "cdr.map_custom_proc")


      columnSelect = Map(
        "or_log_all_proc" -> List("OR_PROC_ID", "LRB_C","LOG_ID"),
        "zh_or_proc" -> List("PROC_NAME","OR_PROC_ID","EXTRACT_DATE"),
        "or_log" -> List("PAT_ID", "SURGERY_DATE", "REFER_PROV_ID", "PRIMARY_PHYS_ID","STATUS_C","LOG_ID", "UPDATE_DATE")
      )

      beforeJoin = Map(
        "or_log" -> ((df: DataFrame) => {
          df.filter("STATUS_C in ('2', '5') AND PAT_ID is not null and SURGERY_DATE is not null")
        }),
        "zh_or_proc" -> ((df: DataFrame) => {
          val groups = Window.partitionBy(df("OR_PROC_ID")).orderBy(df("EXTRACT_DATE").desc)
          df.withColumn("rw", row_number.over(groups))
            .filter("rw=1")
        }),
        "or_log_all_proc" -> ((df: DataFrame) => {
          df.filter("Or_Proc_Id is not null and Lrb_C is not null").withColumnRenamed("OR_PROC_ID", "OR_PROC_ID_orap").withColumnRenamed("LOG_ID", "LOG_ID_orap")
        }),
        "cdr.map_custom_proc" -> ((df: DataFrame) => {
          df.filter("GROUPID = '"+config(GROUP)+"' AND DATASRC = 'or_log'").drop("GROUPID").drop("DATASRC")
        })
      )

      join = (dfs: Map[String, DataFrame]) => {
        dfs("or_log")
          .join(dfs("or_log_all_proc")
              .join(dfs("zh_or_proc"), dfs("or_log_all_proc")("OR_PROC_ID_orap") === dfs("zh_or_proc")("OR_PROC_ID"), "inner")
              .join(dfs("cdr.map_custom_proc"), concat(lit(config(CLIENT_DS_ID)+"."),dfs("or_log_all_proc")("OR_PROC_ID_orap"), lit("."),dfs("or_log_all_proc")("Lrb_C")) === dfs("cdr.map_custom_proc")("LOCALCODE"))
            ,dfs("or_log")("LOG_ID") === dfs("or_log_all_proc")("LOG_ID_orap"), "inner")
          }

      afterJoin = (df: DataFrame) => {
        //bestRowPerGroup(List("PAT_ID","LOG_ID","OR_PROC_ID_orap"), "UPDATE_DATE")(df)
        val groups = Window.partitionBy(df("PAT_ID"),df("LOG_ID"),df("OR_PROC_ID_orap")).orderBy(df("UPDATE_DATE").desc)
        df.withColumn("rw", row_number.over(groups))
          .filter("rw=1")
      }

      map = Map(
        "DATASRC" -> literal("or_log"),
        "LOCALCODE" -> concatFrom(Seq("OR_PROC_ID_orap", "LRB_C"), delim = ".", prefix = config(CLIENT_DS_ID)),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "PROCEDUREDATE" -> mapFrom("SURGERY_DATE"),
        "LOCALNAME" -> mapFrom("PROC_NAME"),
        "REFERPROVIDERID" -> mapFrom("REFER_PROV_ID"),
        "HOSP_PX_FLAG" -> literal("Y"),
        "PERFORMINGPROVIDERID" -> mapFrom("PRIMARY_PHYS_ID"),
        "CODETYPE" -> literal("CUSTOM"),
        "MAPPEDCODE" -> mapFrom("MAPPEDVALUE")
      )

 }

// test
//  val or = new ProcedureOrlog(cfg); val b = build(or) ; b.show(false) ; b.count