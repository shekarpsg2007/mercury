package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._


class ObservationEncountervisitavs(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("encountervisit",
                    "cdr.zcm_obstype_code")

      columnSelect = Map(
        "encountervisit" -> List("AVS_PRINT_TM", "PAT_ID", "PAT_ENC_CSN_ID", "APPT_STATUS_C", "ENC_TYPE_C", "AVS_PRINT_TM", "UPDATE_DATE"),
        "cdr.zcm_obstype_code" -> List("DATASRC", "OBSCODE", "OBSTYPE", "GROUPID")
      )

      beforeJoin = Map(
        "encountervisit" -> ((df: DataFrame) => {
        val df1 = df.withColumn("LOCALCODE",lit("AVS")).withColumn("LOCALRESULT",lit("AVS"))
        val groups = Window.partitionBy(df("PAT_ID"),df("AVS_PRINT_TM")).orderBy(df("UPDATE_DATE").desc)        
        val addColumn = df1.withColumn("rn", row_number.over(groups))
        addColumn.filter("rn = 1 and nvl(APPT_STATUS_C,'0') not in ('3','4','5') and nvl(ENC_TYPE_C, '0') not in ('5') and AVS_PRINT_TM is not null and PAT_ID is not null and PAT_ID <> '-1' and PAT_ENC_CSN_ID <> '-1'")
                .drop("rn")
    }),
        "cdr.zcm_obstype_code" -> ((df: DataFrame) => {
          df.filter("DATASRC = 'encountervisit_avs' and OBSTYPE is not null and GROUPID='"+config(GROUP)+"'").drop("GROUPID")
        })
      )


    join = (dfs: Map[String, DataFrame]) => {
      dfs("encountervisit")
      .join(dfs("cdr.zcm_obstype_code"), dfs("encountervisit")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
    }


      map = Map(
        "DATASRC" -> literal("encountervisit_avs"),
        "LOCALCODE" -> mapFrom("LOCALCODE"),
        "OBSDATE" -> mapFrom("AVS_PRINT_TM"),
        "PATIENTID" -> mapFrom("PAT_ID"),
        "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
        "LOCALRESULT" -> mapFrom("LOCALRESULT"),
        "OBSTYPE" -> mapFrom("OBSTYPE")
      )

 }
 