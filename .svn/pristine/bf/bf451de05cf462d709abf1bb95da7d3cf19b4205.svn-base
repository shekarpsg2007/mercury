package com.humedica.mercury.etl.epic_v2.observation

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.{Engine, EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._


/**
 * Auto-generated on 02/01/2017
 */


class ObservationSochistory(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

      tables = List("sochistory", "encountervisit:epic_v2.clinicalencounter.ClinicalencounterEncountervisit", "cdr.zcm_obstype_code")

                    //"cdr.zcm_obstype_code")

  columnSelect = Map(
    "sochistory" -> List("PAT_ENC_CSN_ID", "CONTACT_DATE", "ALCOHOL_OZ_PER_WK", "ALCOHOL_COMMENT","ALCOHOL_USE_C","SMOKING_TOB_USE_C","SMOKELESS_TOB_USE_C","TOBACCO_USER_C", "TOBACCO_PAK_PER_DY", "TOBACCO_USED_YEARS"),
    "encountervisit" -> List("ENCOUNTERID", "PATIENTID")
  )

  beforeJoin = Map(
   "sochistory" -> ((df: DataFrame) => {
     val session = Engine.spark
     import session.implicits._
     val list_code_order = List(1,2,3,4,5,6,7)
     val df1 = list_code_order.map((_,1)).toDF("CODE_ORDER", "FOO").drop("FOO")
     val groups = Window.partitionBy(df("PAT_ENC_CSN_ID")).orderBy(df("CONTACT_DATE").desc)
     val dedup = df.withColumn("rn", row_number.over(groups))
     val fil = dedup.filter("rn = 1")
     val fil1 = fil.join(df1)
     fil1.withColumn("LOCALCODE", when(fil1("CODE_ORDER") === 1, "ALCOHOL_OZ_PER_WK")
       .when(fil1("CODE_ORDER") === 2, "ALCOHOL_COMMENT")
       .when(fil1("CODE_ORDER") === 3, "SMOKING_TOB_USE_C")
       .when(fil1("CODE_ORDER") === 4, "SMOKELESS_TOB_USE_C")
       .when(fil1("CODE_ORDER") === 5, "TOBACCO_USER_C")
       .when(fil1("CODE_ORDER") === 6, "Pack Years Calculated")
       .when(fil1("CODE_ORDER") === 7, "ALCOHOL_USE_C"))
         .withColumn("PACKYEARS",df("TOBACCO_PAK_PER_DY").multiply(df("TOBACCO_USED_YEARS")))
   }),
    "cdr.zcm_obstype_code" -> includeIf("DATASRC = 'sochistory' and GROUPID='"+config(GROUP)+"'")
  )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("sochistory")
      .join(dfs("encountervisit"), dfs("sochistory")("PAT_ENC_CSN_ID") === dfs("encountervisit")("ENCOUNTERID"), "inner")
      .join(dfs("cdr.zcm_obstype_code"), dfs("sochistory")("LOCALCODE") === dfs("cdr.zcm_obstype_code")("OBSCODE"), "inner")
  }

      map = Map(
        "DATASRC" -> literal("sochistory"),
        "OBSDATE" -> mapFrom("CONTACT_DATE"),
        "PATIENTID" -> mapFrom("PATIENTID"),
        "ENCOUNTERID" -> mapFrom("ENCOUNTERID"),
        "OBSTYPE" -> mapFrom("OBSTYPE"),
        "LOCALRESULT" -> ((col: String, df: DataFrame) => {
          df.withColumn(col,
            when(df("CODE_ORDER") === 1, when(df("ALCOHOL_OZ_PER_WK").isNotNull && (df("ALCOHOL_OZ_PER_WK") !== "0"), "Y"))
              .when(df("CODE_ORDER") === 2, df("ALCOHOL_COMMENT"))
              .when(df("CODE_ORDER") === 3, when(df("SMOKING_TOB_USE_C") === "-1", null).otherwise(concat(lit(config(CLIENT_DS_ID) + ".use."), df("SMOKING_TOB_USE_C"))))
              .when(df("CODE_ORDER") === 4, when(df("SMOKELESS_TOB_USE_C") === "-1", null).otherwise(concat(lit(config(CLIENT_DS_ID) + ".smokeless."), df("SMOKELESS_TOB_USE_C"))))
              .when(df("CODE_ORDER") === 5, when(df("TOBACCO_USER_C") === "-1", null).otherwise(concat(lit(config(CLIENT_DS_ID) + ".tobuser."), df("TOBACCO_USER_C"))))
              .when(df("CODE_ORDER") === 6, when(df("PACKYEARS") === "0", null).otherwise(df("PACKYEARS")))
              .when(df("CODE_ORDER") === 7, when(df("ALCOHOL_USE_C").isin("0", "-1") || isnull(df("ALCOHOL_USE_C")), null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), df("ALCOHOL_USE_C")))))
        })
      )


  afterMap = (df: DataFrame) => {
    df.filter("OBSDATE is not null and PATIENTID is not null and LOCALRESULT is not null")
  }

}

// test
// val soc = new ObservationSochistory(cfg); val os = build(soc) ; os.show(false) ; os.count
