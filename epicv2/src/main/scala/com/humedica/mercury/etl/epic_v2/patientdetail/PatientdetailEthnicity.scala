package com.humedica.mercury.etl.epic_v2.patientdetail

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 01/27/2017
 */


class PatientdetailEthnicity(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("patreg", "pat_race")

  
  columnSelect = Map(
    "patreg" -> List("ETHNIC_GROUP_C", "UPDATE_DATE", "PAT_ID"),
    "pat_race" -> List("PATIENT_RACE_C", "PAT_ID", "UPDATE_DATE")
  )

  beforeJoin = Map(
    "patreg" -> ((df: DataFrame) => {
      val df1 = df.filter("pat_id is not null")
      val groups = Window.partitionBy(df1("PAT_ID"),df1("ETHNIC_GROUP_C")).orderBy(df1("UPDATE_DATE").desc_nulls_last)
      df1.withColumn("rn_pat", row_number.over(groups))
          .withColumnRenamed("UPDATE_DATE","UPDATE_DATE_PAT")
          .withColumnRenamed("PAT_ID","PAT_ID_PAT")
          .filter("rn_pat = 1")
          .drop("rn_pat")
    }),
    "pat_race" -> ((df: DataFrame) => {
      val df1 = df.filter("pat_id is not null")
      val groups = Window.partitionBy(df1("PAT_ID"),df1("PATIENT_RACE_C")).orderBy(df1("UPDATE_DATE").desc_nulls_last)
      df1.withColumn("rn_race", row_number.over(groups))
          .withColumnRenamed("UPDATE_DATE","UPDATE_DATE_RACE")
          .withColumnRenamed("PAT_ID","PAT_ID_RACE")
          .filter("rn_race = 1")
          .drop("rn_race")
    })
  )


  join = (dfs: Map[String, DataFrame]) => {
    dfs("patreg")
      .join(dfs("pat_race"), dfs("pat_race")("PAT_ID_RACE") === dfs("patreg")("PAT_ID_PAT"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("patreg"),
    "PATIENTID" -> mapFrom("PAT_ID_PAT"),
    "PATIENTDETAILTYPE" -> literal("ETHNICITY"),
    "PATDETAIL_TIMESTAMP" -> ((col:String, df:DataFrame)=> {
      df.withColumn(col, when(df("ETHNIC_GROUP_C").isNotNull && (df("ETHNIC_GROUP_C") !== "-1"), df("UPDATE_DATE_PAT"))
        .otherwise(df("UPDATE_DATE_RACE")))
    }),
    "LOCALVALUE"-> ((col:String, df:DataFrame)=> {
      df.withColumn(col, when(df("ETHNIC_GROUP_C").isNotNull && (df("ETHNIC_GROUP_C") !== "-1"), concat(lit(config(CLIENT_DS_ID)), lit("e."), df("ETHNIC_GROUP_C")))
          .when(df("PATIENT_RACE_C").isNotNull, concat(lit(config(CLIENT_DS_ID)), lit("r."), df("PATIENT_RACE_C")))
          .otherwise(null))
    })

  )
  
  afterMap = (df: DataFrame) => {
    val df1 = df.filter("localvalue is not null and patdetail_timestamp is not null")
    val groups = Window.partitionBy(df1("PATIENTID"), lower(df1("LOCALVALUE"))).orderBy(df1("PATDETAIL_TIMESTAMP").desc_nulls_last)
    val df2 = df1.withColumn("rn", row_number.over(groups))
    val pats_e = df2.filter("instr(localvalue, 'e.') > 0").select("PATIENTID").distinct
    val pats_r = df2.filter("instr(localvalue, 'r.') > 0").select("PATIENTID").distinct
    val both_pats = pats_e.join(pats_r, "PATIENTID").withColumnRenamed("PATIENTID","PATIENTID_pats")
    val df3 = df2.join(both_pats, both_pats("PATIENTID_pats") === df2("PATIENTID"), "inner").filter("instr(localvalue, 'r.') > 0").drop("PATIENTID_pats")
    val df4 = df3.withColumnRenamed("GROUPID","GROUPID1")
        .withColumnRenamed("DATASRC","DATASRC1")
        .withColumnRenamed("FACILITYID","FACILITYID1")
        .withColumnRenamed("PATIENTID","PATIENTID1")
        .withColumnRenamed("ENCOUNTERID","ENCOUNTERID1")
        .withColumnRenamed("PATIENTDETAILTYPE","PATIENTDETAILTYPE1")
        .withColumnRenamed("PATIENTDETAILQUAL","PATIENTDETAILQUAL1")
        .withColumnRenamed("LOCALVALUE","LOCALVALUE1")
        .withColumnRenamed("PATDETAIL_TIMESTAMP","PATDETAIL_TIMESTAMP1")
        .withColumnRenamed("CLIENT_DS_ID","CLIENT_DS_ID1")
        .withColumnRenamed("HGPID","HGPID1")
        .withColumnRenamed("GRP_MPI","GRP_MPI1")
        .withColumnRenamed("ROW_SOURCE","ROW_SOURCE1")
        .withColumnRenamed("MODIFIED_DATE","MODIFIED_DATE1")
        .withColumnRenamed("rn","rn1")
    val minus = df2.except(df4)
    minus.filter("rn = 1").drop("rn")
  }

}