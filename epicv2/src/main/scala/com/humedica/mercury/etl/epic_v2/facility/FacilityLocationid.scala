package com.humedica.mercury.etl.epic_v2.facility

import org.apache.spark.sql.DataFrame
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class FacilityLocationid(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_claritydept", "encountervisit", "cdr.map_predicate_values")

  columnSelect = Map(
    "zh_claritydept" -> List("DEPARTMENT_ID", "REV_LOC_ID", "DEPARTMENT_NAME", "LOC_NAME"),
    "encountervisit" -> List("DEPARTMENT_ID","ENCOUNTER_PRIMARY_LOCATION")
  )

  beforeJoin = Map(
    "zh_claritydept" -> (( df: DataFrame) => {

      val zh1 = table("zh_claritydept")
      val fil1 = zh1.filter("DEPARTMENT_ID <> '-1'").withColumn("FACILITYID", trim(zh1("DEPARTMENT_ID"))).withColumn("DEPT_ID", trim(zh1("DEPARTMENT_ID")))
      val groupsfil1 = Window.partitionBy(fil1("FACILITYID")).orderBy(fil1("DEPARTMENT_NAME").asc)
      val un1 = fil1.withColumn("un1_rw", row_number.over(groupsfil1)).withColumn("FACILITYNAME", trim(fil1("DEPARTMENT_NAME")))
        .filter("un1_rw=1")
        .select("DEPARTMENT_ID","REV_LOC_ID","DEPARTMENT_NAME","LOC_NAME","FACILITYID","FACILITYNAME","LOC_NAME","DEPT_ID")


      val zh2 = table("zh_claritydept")
      val fil2 = zh2.filter("REV_LOC_ID <> '-1'").withColumn("FACILITYID", concat(lit(config(CLIENT_DS_ID) + "loc."), trim(zh2("REV_LOC_ID")))).withColumn("DEPT_ID", lit("-z"))
      val groupsfil2 = Window.partitionBy(fil2("FACILITYID")).orderBy(fil2("LOC_NAME").asc)
      val un2 = fil2.withColumn("un2_rw", row_number.over(groupsfil2)).withColumn("FACILITYNAME", trim(fil2("LOC_NAME")))
        .filter("un2_rw=1")
        .select("DEPARTMENT_ID","REV_LOC_ID","DEPARTMENT_NAME","LOC_NAME","FACILITYID","FACILITYNAME","LOC_NAME","DEPT_ID")

      un1.union(un2).distinct()

    }),

    "encountervisit" -> (( df: DataFrame) => {
      df.filter("DEPARTMENT_ID <> '-1'").distinct().withColumnRenamed("DEPARTMENT_ID","DEPARTMENT_ID_enc")
    })
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("zh_claritydept")
      .join(dfs("encountervisit"), dfs("zh_claritydept")("DEPT_ID") === dfs("encountervisit")("DEPARTMENT_ID_enc"), "left_outer")
  }

  afterJoin = (df1: DataFrame) => {
    val facility_col = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "ENCOUNTERVISIT", "CLINICALENCOUNTER", "ENCOUNTERVISIT", "FACILITYID")
    val df = df1.repartition(1000)
    df.withColumn("Facility_Id", when(lit("'Dept'") === facility_col || lit("'Loc'") === facility_col || lit("'coalesceLocDept'") === facility_col,
      when(lit("'Dept'") === facility_col, df("DEPARTMENT_ID"))
        .when(lit("'Loc'") === facility_col, df("REV_LOC_ID"))
        .when(lit("'coalesceLocDept'") === facility_col, coalesce(df("REV_LOC_ID"),df("DEPARTMENT_ID")))
        .otherwise(coalesce(df("DEPARTMENT_ID"), when(df("ENCOUNTER_PRIMARY_LOCATION").isNotNull, concat_ws("", lit(config(CLIENT_DS_ID)+ "loc."), df("ENCOUNTER_PRIMARY_LOCATION"))).otherwise(null)))).otherwise(
      when(lit("'coalesceDeptPrimary'") === facility_col, df("FACILITYID")).otherwise(df("FACILITYID")))
    )

      .withColumn("Facility_Name", when(lit("'Dept'") === facility_col || lit("'Loc'") === facility_col || lit("'coalesceLocDept'") === facility_col, when(lit("'Dept'") === facility_col, df("DEPARTMENT_NAME"))
        .when(lit("'Loc'") === facility_col, df("LOC_NAME"))
        .when(lit("'coalesceLocDept'") === facility_col, coalesce(df("LOC_NAME"),df("DEPARTMENT_NAME")))
        .otherwise(coalesce(df("DEPARTMENT_NAME"),df("LOC_NAME")))).otherwise(when(lit("'coalesceDeptPrimary'") === facility_col, df("FACILITYNAME")).otherwise(df("FACILITYNAME"))))
  }

  map = Map(
    "FACILITYID" -> mapFrom("Facility_Id"),
    "FACILITYNAME" -> mapFrom("Facility_Name")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("FACILITYID <> '-1' and FACILITYID is not null")
    val groups = Window.partitionBy(fil("FACILITYID")).orderBy(fil("FACILITYNAME").desc)
    fil.withColumn("rw", row_number.over(groups))
      .filter("rw=1")

  }
}

// test
// val f = new FacilityLocationid(cfg) ; val fac = build(f,allColumns=true) ;  fac.show; fac.count

