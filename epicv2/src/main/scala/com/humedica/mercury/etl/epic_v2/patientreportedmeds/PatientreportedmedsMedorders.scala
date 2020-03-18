package com.humedica.mercury.etl.epic_v2.patientreportedmeds

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.engine.Functions._

/**
 * Auto-generated on 01/27/2017
 */


class PatientreportedmedsMedorders(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

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
      "ORDERING_MODE", "HV_DOSE_UNIT_C"),
    "encountervisit" -> List("ENCOUNTERID", "ARRIVALTIME")
  )


  beforeJoin =
    Map("medorders" -> ((df: DataFrame) =>
    {
      val groups = Window.partitionBy(df("ORDER_MED_ID")).orderBy(df("UPDATE_DATE").desc)
      val addColumn = df.withColumn("rn", row_number.over(groups))
      val dd = addColumn.filter("rn = 1 AND (ORDER_STATUS_C <> '4' OR ORDER_STATUS_C is null) AND MEDICATION_ID <> '-1' AND MEDICATION_ID IS NOT NULL and PAT_ID is not null").drop("rn")
      dd.withColumn("LOCALORDERTYPECODE",
        when(dd("ORDER_CLASS_C") === "3", config(CLIENT_DS_ID) + lit(".") + dd("ORDER_CLASS_C"))
          .when(isnull(dd("ORDERING_MODE_C")) && isnull(dd("ORDERING_MODE")),null)
          .otherwise(when(isnull(dd("ORDERING_MODE_C")),config(CLIENT_DS_ID) + lit(".") + dd("ORDERING_MODE")).otherwise(config(CLIENT_DS_ID) + lit(".") + dd("ORDERING_MODE_C"))))
    }
      ),
      "cdr.map_predicate_values" -> ((df: DataFrame) => {
        val fil = df.filter("GROUPID = '"+config(GROUP)+"' AND CLIENT_DS_ID = '"+config(CLIENT_DS_ID)+"' AND DATA_SRC = 'MEDORDERS' " +
          "AND entity = 'RXORDER' AND TABLE_NAME = 'MEDORDERS' AND COLUMN_NAME = 'ORDER_CLASS_C'")
        fil.withColumnRenamed("GROUPID", "GROUPID_mpv").drop("DTS_VERSION")
      }),
      "cdr.map_ordertype" -> includeIf("GROUPID='"+config(GROUP)+"'"),
      "zh_med_unit" -> renameColumn("NAME", "NAME_zhmu")
    )

  join = (dfs: Map[String, DataFrame]) => {
    dfs("medorders")
      .join(dfs("encountervisit"), dfs("encountervisit")("ENCOUNTERID") === dfs("medorders")("PAT_ENC_CSN_ID"), "left_outer")
      .join(dfs("zh_med_unit"), dfs("medorders")("HV_DOSE_UNIT_C") === dfs("zh_med_unit")("DISP_QTYUNIT_C"), "left_outer")
      .join(dfs("cdr.map_predicate_values"), dfs("medorders")("ORDER_CLASS_C") === dfs("cdr.map_predicate_values")("column_value"), "left_outer")
      .join(dfs("cdr.map_ordertype"), dfs("cdr.map_ordertype")("LOCALCODE") === dfs("medorders")("LOCALORDERTYPECODE"), "left_outer")
  }


  map = Map(
    "DATASRC" -> literal("medorders"),
    "PATIENTID" -> mapFrom("PAT_ID"),
    "ENCOUNTERID" -> mapFrom("PAT_ENC_CSN_ID"),
    "LOCALDRUGDESCRIPTION" -> mapFrom("NAME"),
    "LOCALDOSEUNIT" -> mapFromSecondColumnValueIsNotIn("NAME_zhmu", "HV_DOSE_UNIT_C",Seq("-1")),
    "LOCALFORM" -> mapFrom("FORM"),
    "LOCALPROVIDERID" -> mapFrom("MED_PRESC_PROV_ID"),
    "LOCALGPI" -> mapFrom("GPI"),
    "LOCALMEDCODE" -> mapFrom("MEDICATION_ID"),
    "LOCALROUTE" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        concat(lit(config(CLIENT_DS_ID) + "."), when(df("MED_ROUTE_C").isNotNull && (df("MED_ROUTE_C") !== "-1"),df("MED_ROUTE_C")).otherwise(df("ROUTE"))))
    }),
    "LOCALSTRENGTHPERDOSEUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, regexp_extract(df("STRENGTH"), "[-\\.0-9]*", 0)
      )
    }),
    "LOCALSTRENGTHUNIT" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, regexp_extract(df("STRENGTH"), "[^-\\.0-9]+", 0)
      )
    }),
    "REPORTEDMEDID" -> mapFrom("ORDER_MED_ID"),
    "MEDREPORTEDTIME" -> mapFrom("ARRIVALTIME"),
    "DISCONTINUEDATE" -> cascadeFrom(Seq("DISCON_TIME","end_date")),
    "DISCONTINUEREASON" -> mapFrom("RSN_FOR_DISCON_C", prefix=config(CLIENT_DS_ID)+".", nullIf=Seq("-1"))
  )


  afterMap = (df: DataFrame) => {
    df.filter("COLUMN_VALUE is not null and coalesce(MEDREPORTEDTIME, DISCONTINUEDATE) is not null")
  }




  mapExceptions = Map(
    ("H846629_EP2", "DISCONTINUEDATE") -> cascadeFrom(Seq("ORDER_END_TIME", "DISCON_TIME")),
    ("H458934_EP2", "LOCALPROVIDERID") -> mapFrom("authrzing_prov_id", nullIf=Seq("-1"))
  )

}

// val x = new PatientreportedmedsMedorders(cfg) ; val prm = build(x) ; prm.show(false) ; prm.count ; prm.select("ENCOUNTERID").distinct.count