package com.humedica.mercury.etl.epic_v2.encountercarearea

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  *
  */
class EncountercareareaEncountervisit(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List(
    "Encountervisit:epic_v2.clinicalencounter.ClinicalencounterEncountervisit",
    "adtevents",
    "zh_claritydept")

  columnSelect = Map(
    "Encountervisit" -> List("ENCOUNTERID","ARRIVALTIME"),
    "adtevents" -> List("PAT_ENC_CSN_ID", "DEPARTMENT_ID","PAT_ID", "EFFECTIVE_TIME", "EVENT_TYPE_C", "PAT_SERVICE_C", "DELETE_TIME", "EVENT_TIME"),
    "zh_claritydept"-> List("DEPARTMENT_ID", "DEPARTMENT_NAME") )


  beforeJoin = Map(
    "adtevents" -> ((df: DataFrame) => {
      val addColumn = df.withColumn("OUTPAT_START_TIME", when(df("EVENT_TYPE_C") === "7", df("EVENT_TIME")))
        .withColumn("START_TIME", when(df("EVENT_TYPE_C")isin("1", "3"), df("EFFECTIVE_TIME")))
        .withColumn("END_TIME", when(df("EVENT_TYPE_C")isin("2", "4"), df("EFFECTIVE_TIME")))
        .withColumn("SERVICECODE",
          when(df("PAT_SERVICE_C").isNotNull, concat(lit(config(CLIENT_DS_ID)+"."),df("PAT_SERVICE_C")))
            .otherwise(null))
        .withColumnRenamed("PAT_ENC_CSN_ID", "ENCOUNTERID").withColumnRenamed("PAT_ID", "PATIENTID")
      val fil = addColumn.filter("EVENT_TYPE_C in (1,2,3,4,7) and DELETE_TIME is null and PATIENTID is not null")
      fil.groupBy("DEPARTMENT_ID", "ENCOUNTERID", "PATIENTID", "SERVICECODE")
        .agg(coalesce(max("OUTPAT_START_TIME"),max("START_TIME"),max("END_TIME")).as("START_TIME"), max("END_TIME").as("END_TIME"))
    }
      )
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("Encountervisit")
      .join(dfs("adtevents")
        .join(dfs("zh_claritydept"), Seq("DEPARTMENT_ID"),  "left_outer"),
        Seq("ENCOUNTERID"), "left_outer")
  }


  afterJoin = (df: DataFrame) => {
    df.filter("PATIENTID is not null")
  }


  map = Map(
    "DATASRC" -> literal("encountervisit"),
    "ENCOUNTERTIME"     -> mapFrom("ARRIVALTIME"),
    "PATIENTID"         -> mapFrom("PATIENTID"),
    "CAREAREAENDTIME"   -> mapFrom("END_TIME"),
    "CAREAREASTARTTIME" -> mapFrom("START_TIME"),
    "LOCALCAREAREACODE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("DEPARTMENT_ID").isNotNull, concat(lit(config(CLIENT_DS_ID) + "."), df("DEPARTMENT_ID"))).otherwise(null))
    }),
    "LOCALLOCATIONNAME" -> mapFrom("DEPARTMENT_NAME")
  )

/*
  mapExceptions = Map(
    ("H101623_EP2", "LOCALCAREAREACODE") -> mapFrom("DEPARTMENT_ID", nullIf=Seq("-1")),
    ("H328218_EP2", "CAREAREASTARTTIME") -> mapFrom("EFFECTIVE_TIME"),

    */

}
