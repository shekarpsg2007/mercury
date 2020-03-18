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


class ObservationPatenc3_LOOKUP(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("pat_enc_3", "cdr.zcm_obstype_code")

      beforeJoin = Map(
        "pat_enc_3" -> includeIf("COUNSELING_GIVEN_C = '1'")
      )

      map = Map(
        "DATASRC" -> literal("pat_enc3"),
        "LOCALCODE" -> literal("TOB_CESS"),
        "OBSTYPE" -> ((col: String, df: DataFrame) =>
           df.withColumn(col, lit(lookupValue(table("cdr.zcm_obstype_code"), "GROUPID='"+config(GROUP)+"' AND DATASRC='pat_enc3' AND OBSCODE='TOB_CESS'", "OBSTYPE")))),
        "OBSDATE" -> cascadeFrom(Seq("SMK_CESS_DTTM", "CONTACT_DATE", "BEGIN_CHECKIN_DTTM")),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN"),
        "LOCALRESULT" -> mapFrom("COUNSELING_GIVEN_C", prefix = config(CLIENT_DS_ID)+".", nullIf = Seq("-1"))
      )


      afterMap = (df: DataFrame) => {
        val groups1 = Window.partitionBy(df("PATIENTID"), df("ENCOUNTERID"), df("OBSDATE"),df("OBSTYPE")).orderBy(df("UPDATE_DATE").desc)
        val addColumn = df.withColumn("rn", row_number.over(groups1))
        addColumn.filter("PATIENTID is not null AND OBSDATE is not null and rn = 1 AND OBSTYPE is not null")
      }

 }