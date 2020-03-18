package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Auto-generated on 02/01/2017
 */


class ObservationPatenc3(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("pat_enc_3", "cdr.zcm_obstype_code")

      beforeJoin = Map(
        "pat_enc_3" -> ((df: DataFrame) => {
          df.filter("COUNSELING_GIVEN_C = '1' AND PAT_ID is not null AND coalesce ( SMK_CESS_DTTM, CONTACT_DATE, BEGIN_CHECKIN_DTTM ) is not null")}),
          "cdr.zcm_obstype_code" -> ((df: DataFrame) => { df.filter("DATASRC = 'pat_enc3' AND GROUPID='"+config(GROUP)+"' AND OBSCODE ='TOB_CESS' ")

        }
        )
      )
      join = (dfs: Map[String, DataFrame]) => {
        dfs("pat_enc_3")
          .join(dfs("cdr.zcm_obstype_code"),
            lit("TOB_CESS") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "cross")
      }


      map = Map(
        "DATASRC" -> literal("pat_enc3"),
        "LOCALCODE" -> literal("TOB_CESS"),
        "OBSDATE" -> cascadeFrom(Seq("SMK_CESS_DTTM", "CONTACT_DATE", "BEGIN_CHECKIN_DTTM")),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN"),
        "FACILITYID" -> literal(null) ,
        "LOCALRESULT" -> mapFrom("COUNSELING_GIVEN_C", prefix = config(CLIENT_DS_ID)+".", nullIf = Seq("-1")),
        "OBSRESULT" -> literal(null),
        "OBSTYPE" -> mapFrom("OBSTYPE")
      )

  afterMap = (df: DataFrame) => {
    val groups1 = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("OBSDATE"),df("OBSTYPE")).orderBy(df("UPDATE_DATE").desc)
    val addColumn = df.withColumn("rn", row_number.over(groups1))
    addColumn.filter("PATIENTID is not null AND OBSDATE is not null and rn = 1")
  }

 }