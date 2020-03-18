package com.humedica.mercury.etl.epic_v2.rxordersandprescriptions

import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Types._

/**
  * Auto-generated on 02/01/2017
  */

class RxordersandprescriptionsMedorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("zh_med_unit",
    "medorders",
    "cdr.map_ordertype",
    "cdr.map_predicate_values",
    "encountervisit:epic_v2.clinicalencounter.ClinicalencounterEncountervisit")

  columnSelect = Map(
    "zh_med_unit" -> List("DISP_QTYUNIT_C", "NAME"),
    "medorders" -> List("ORDER_STATUS_C", "AUTHRZING_PROV_ID", "MED_ROUTE_C", "DISCON_TIME",
      "RSN_FOR_DISCON_C", "ORDER_END_TIME", "ORDER_INST", "ORDERING_MODE_C",
      "PAT_ID", "ORDER_MED_ID", "PAT_ENC_CSN_ID",
      "DISP_AS_WRITTEN_YN", "HV_DISCR_FREQ_ID", "NAME", "FORM", "GENERIC_NAME",
      "GPI", "MEDICATION_ID", "MED_PRESC_PROV_ID", "STRENGTH",
      "REFILLS", "QUANTITY", "SIG", "UPDATE_DATE", "ORDER_CLASS_C", "ORDER_START_TIME", "ORDERING_DATE", "END_DATE", "ROUTE",
      "ORDERING_MODE", "HV_DOSE_UNIT_C", "PEND_APPROVE_FLAG_C","FILEID"),
    "encountervisit" -> List("ENCOUNTERID")
  )


  beforeJoin =
    Map("medorders" -> ((df: DataFrame) =>
    {
      val groups = Window.partitionBy(df("ORDER_MED_ID")).orderBy(df("UPDATE_DATE").desc,df("FILEID").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val dd = addColumn.filter("rn = 1 AND (ORDER_STATUS_C <> '4' OR ORDER_STATUS_C is null) AND MEDICATION_ID <> '-1' AND MEDICATION_ID IS NOT NULL and PAT_ID is not null").drop("rn")
      dd.withColumn("LOCALORDERTYPECODE",
        when(dd("ORDER_CLASS_C") === "3", concat(lit(config(CLIENT_DS_ID) + "."),  dd("ORDER_CLASS_C")))
          .when(isnull(dd("ORDERING_MODE_C")) && isnull(dd("ORDERING_MODE")),null)
          // .when(dd("ORDERING_MODE_C").isNotNull, dd("ORDERING_MODE_C"))
          // .when(dd("ORDERING_MODE").isNotNull, dd("ORDERING_MODE"))
          .otherwise(when(isnull(coalesce(dd("ORDERING_MODE_C"),dd("ORDERING_MODE"))), null).otherwise(concat(lit(config(CLIENT_DS_ID) + "."), coalesce(dd("ORDERING_MODE"),dd("ORDERING_MODE_C"))))))
    }
      ),
      "cdr.map_ordertype" -> includeIf("GROUPID='"+config(GROUP)+"'"),
      "zh_med_unit" ->   ((df: DataFrame) => {
        df.withColumnRenamed("NAME","NAME_mu")
      })
    )

  join = (dfs: Map[String, DataFrame]) => {
    val fil = dfs("cdr.map_predicate_values").filter("GROUPID = '"+config(GROUP)+"' AND CLIENT_DS_ID = '"+config(CLIENT_DS_ID)+"' AND DATA_SRC = 'MEDORDERS' " +
      "AND entity = 'RXORDER' AND TABLE_NAME = 'MEDORDERS' AND COLUMN_NAME = 'ORDER_CLASS_C'")
    val fil2 = fil.withColumnRenamed("GROUPID","GROUPID_mpv").drop("DTS_VERSION")
    dfs("medorders")
      .join(dfs("encountervisit"), dfs("encountervisit")("ENCOUNTERID") === dfs("medorders")("PAT_ENC_CSN_ID"), "left_outer")
      .join(dfs("zh_med_unit"), dfs("medorders")("HV_DOSE_UNIT_C") === dfs("zh_med_unit")("DISP_QTYUNIT_C"), "left_outer")
      .join(fil2, dfs("medorders")("ORDER_CLASS_C") === fil2("column_value"), "left_outer")
      .join(dfs("cdr.map_ordertype"), dfs("cdr.map_ordertype")("LOCALCODE") === dfs("medorders")("LOCALORDERTYPECODE"), "left_outer")
  }

  map = Map(
    "DATASRC" -> literal("medorders"),
    "ISSUEDATE" -> cascadeFrom(Seq("ORDER_INST", "ORDER_START_TIME", "ORDERING_DATE")),
    "ORDERVSPRESCRIPTION" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ORDERING_MODE_C") === "1", "P")
        .when(df("ORDERING_MODE_C") === "2", "O").otherwise(null))
    }),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "RXID" -> mapFrom("ORDER_MED_ID"),
    "DISCONTINUEDATE" -> cascadeFrom(Seq("DISCON_TIME", "END_DATE")),
    "DISCONTINUEREASON" -> mapFrom("RSN_FOR_DISCON_C", nullIf=Seq("-1", null), prefix=config(CLIENT_DS_ID) + "."),
    "LOCALDAW" -> mapFrom("DISP_AS_WRITTEN_YN"),
    "LOCALDOSEFREQ" -> mapFrom("HV_DISCR_FREQ_ID", nullIf=Seq(null), prefix = config(CLIENT_DS_ID) + "."),
    "LOCALDESCRIPTION" -> mapFrom("NAME"),
    "LOCALFORM" -> mapFrom("FORM"),
    "LOCALGENERICDESC" -> mapFrom("GENERIC_NAME"),
    "LOCALGPI" -> mapFrom("GPI"),
    "LOCALMEDCODE" -> mapFrom("MEDICATION_ID"),
    "LOCALPROVIDERID" -> mapFrom("MED_PRESC_PROV_ID", nullIf = Seq("-1")),
    "LOCALROUTE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        concat(lit(config(CLIENT_DS_ID) + "."), when(df("MED_ROUTE_C").isNotNull && (df("MED_ROUTE_C") =!= "-1"),df("MED_ROUTE_C")).otherwise(df("ROUTE"))))
    }),
    "LOCALSTRENGTHPERDOSEUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, regexp_extract(df("STRENGTH"), "[-\\.0-9]*", 0)
      )
    }),
    "LOCALSTRENGTHUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, regexp_extract(df("STRENGTH"), "[^-\\.0-9]+", 0)
      )
    }),
    "FILLNUM" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when((df("REFILLS") leq 100) &&
        (df("REFILLS").rlike("^[+-]?(\\.\\d+|\\d+\\.?\\d*)$") || isnull(df("REFILLS"))), df("REFILLS")).otherwise(null))
    }),
    "ORDERSTATUS" -> mapFrom("ORDER_STATUS_C", nullIf = Seq("-1"), prefix = config(CLIENT_DS_ID) + "."),
    "QUANTITYPERFILL" ->  ((col: String, df: DataFrame) => {
      df.withColumn(col, substring(df("QUANTITY"), 1, 200))
    }),
    "SIGNATURE" -> mapFrom("SIG"),
    "VENUE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("ORDERING_MODE_C") === "1", "1")
        .when(df("ORDERING_MODE_C") === "2", "0").otherwise(null))
    }),
    "ORDERTYPE" -> mapFrom("CUI"),
    "LOCALDOSEUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("HV_DOSE_UNIT_C") === "-1", lit(null)).otherwise(df("NAME_mu")))
    })

  )

  

  afterMap = (df: DataFrame) => {
    val pend_approve_flag_c = mpvClause(table("cdr.map_predicate_values"), config(GROUP), config(CLIENT_DS_ID), "MEDORDERS", "RXORDER", "MEDORDERS", "PEND_APPROVE_FLAG_C")
    df.filter("(PEND_APPROVE_FLAG_C not in (" + pend_approve_flag_c + ") OR PEND_APPROVE_FLAG_C IS NULL OR 'NO_MPV_MATCHES' in (" + pend_approve_flag_c
    + ") )  AND COLUMN_VALUE is null AND COALESCE(ISSUEDATE,DISCONTINUEDATE) is not null and RXID is not null ")
  }

  

  mapExceptions = Map(
    ("H458934_EP2", "LOCALPROVIDERID") -> mapFrom("AUTHRZING_PROV_ID", nullIf=Seq("-1")),
    ("H846629_EP2", "DISCONTINUEDATE") -> cascadeFrom(Seq("ORDER_END_TIME", "DISCON_TIME"))
  )

}

// val r = new RxordersandprescriptionsMedorders(cfg); val rr=build(r) ; rr.show; rr.count; rr.select("ENCOUNTERID").distinct.count