package com.humedica.mercury.etl.crossix.rxorderphi

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

/**
  * Created by bhenriksen on 5/17/17.
  */
class RxorderphiASTempPatient(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

    columns = List( "DATASRC", "PATIENTID", "FACILITYID","DATEOFBIRTH","MEDICALRECORDNUMBER","HGPID","DATEOFDEATH", "GRP_MPI","INACTIVE_FLAG")

    tables = List("as_patdem", "cdr.patient_phi")

    columnSelect = Map(
        "as_patdem" -> List(),
        "cdr.patient_mpi" -> List("PATIENTID","HGPID","GRP_MPI")
    )

    join = (dfs: Map[String, DataFrame])  => {
        var PATDEM = dfs("as_patdem")
        var PATIENT_MPI1 = dfs("cdr.patient_phi")

        var FILTER_PATDEM = PATDEM.filter((PATDEM("PATIENT_LAST_NAME").rlike(".*%.*") and PATDEM("PATIENT_FIRST_NAME").isNull) or
                (PATDEM("PATIENT_LAST_NAME").rlike("^TEST |TEST$|ZZTEST") or PATDEM("PATIENT_FIRST_NAME").rlike("TEST")
                        or upper(PATDEM("PATIENT_FIRST_NAME")) === "TEST")===false)
        var TEMP_PATIENT1 = FILTER_PATDEM.withColumn("DATASRC", lit("patdem")).withColumn("PATIENTID", FILTER_PATDEM("PATIENT_MRN")).withColumn("MEDICALRECORDNUMBER", FILTER_PATDEM("PATIENT_MRN") )
                .withColumnRenamed("LAST_UPDATED_DATE","UPDATE_TS")
        TEMP_PATIENT1 = TEMP_PATIENT1.select("DATASRC", "PATIENTID", "MEDICALRECORDNUMBER", "PATIENT_DATE_OF_BIRTH", "UPDATE_TS", "INACTIVE_FLAG")

        val group1 = Window.partitionBy(TEMP_PATIENT1("PATIENTID")).orderBy(TEMP_PATIENT1("UPDATE_TS").desc)
        var TEMP_PATIENT_3 = TEMP_PATIENT1.withColumn("ROWNMBR", row_number().over(group1)).withColumn("FACILITYID", lit("NULL")).filter(TEMP_PATIENT1("ROWNMBR")===1)
        var TEMP_PATIENT = TEMP_PATIENT_3.select("DATASRC", "PATIENTID", "FACILITYID", "PATIENT_DATE_OF_BIRTH", "MEDICALRECORDNUMBER", "INACTIVE_FLAG", "ROWNMBR").distinct()
        var TEMP_PAT_JOIN = TEMP_PATIENT.join(PATIENT_MPI1, Seq("PATIENTID"), "left_outer")
        TEMP_PAT_JOIN.withColumn("DATEOFDEATH", lit("NULL"))
    }

    map = Map(
        "PATIENTID" ->  mapFrom("PATIENTID_pat"),
        "GROUPID" ->  mapFrom("GROUPID"),
        "CLIENT_DS_ID" ->  mapFrom("CLIENT_DS_ID"),
        "DATEOFBIRTH" ->  mapFrom("PATIENT_DATE_OF_BIRTH"),
        "DATEOFDEATH" -> literal("NULL"),
        "FACILITYID" -> ((col: String, df: DataFrame) => {df.withColumn(col,  when(df("FACILITYID").isNotNull,  df("FACILITYID")))})
    )
}

//build(new RxordersandprescriptionsErx(cfg), allColumns=true).withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID").write.parquet(cfg("EMR_DATA_ROOT")+"/RXOUT/ASRXORDER")