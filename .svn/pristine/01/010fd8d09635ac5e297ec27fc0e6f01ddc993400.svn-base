package com.humedica.mercury.etl.hl7_v2.facility

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 5/26/17.
  */
class FacilityPv1(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("temptable:hl7_v2.temptable.TemptableAll")

  columnSelect = Map(
    "temptable" -> List("PATIENTID","LASTUPDATEDATE","MESSAGEID","MSH_F4_C1","PV1_F3_C4","PV1_F3_C5")
  )

  join = noJoin()

  map = Map(
    "DATASRC" -> literal("hl7_segment_pv1_a"),
    "FACILITYID" -> mapFrom("PV1_F3_C4"),
    "FACILITYNAME" -> mapFrom("PV1_F3_C4"),
    "FACILITYPOSTALCD" -> standardizeZip("PV1_F3_C5")
  )

  afterMap = (df: DataFrame) => {
    val fil = df.filter("FACILITYID IS NOT NULL AND PATIENTID IS NOT NULL")
    val groups = Window.partitionBy(fil("FACILITYID")).orderBy(fil("MESSAGEID").desc)
    val addColumn = fil.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn")
  }

  //TODO add other client exceptions too.
  mapExceptions = Map(
    ("H247885_HL7", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H247885_HL7", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H257275_HL7", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H257275_HL7", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H135772_HL7_MDT_STJ", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H135772_HL7_MDT_STJ", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H303173_HL7_EL", "FACILITYID") -> mapFrom("PV1_F3_C1"),
    ("H303173_HL7_EL", "FACILITYNAME") -> mapFrom("PV1_F3_C1"),
    ("H451171_HL7_EP2", "FACILITYID") -> cascadeFrom(Seq("PV1_F3_C4", "MSH_F4_C1")),
    ("H451171_HL7_EP2", "FACILITYNAME") -> cascadeFrom(Seq("PV1_F3_C4", "MSH_F4_C1")),
    ("H451171_HL7_MDT", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H591965_HL7_CFHA", "FACILITYID") -> mapFrom("PV1_f3_C1"),
    ("H641171_HL7_PM", "FACILITYPOSTALCD") -> nullValue(),
    ("H704847_HL7_CCD","FACILITYID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PV1_F3_C4").isNotNull, concat(df("MSH_F4_C1"), lit("."), df("PV1_F3_C4")))
        .otherwise(df("MSH_F4_C1")))
    }),
    ("H704847_HL7_CCD","FACILITYNAME") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PV1_F3_C4").isNotNull, concat(df("MSH_F4_C1"), lit("."), df("PV1_F3_C4")))
        .otherwise(df("MSH_F4_C1")))
    }),
    ("H704847_HL7_OCIE","FACILITYID") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PV1_F3_C4").isNotNull, concat(df("MSH_F4_C1"), lit("."), df("PV1_F3_C4")))
        .otherwise(df("MSH_F4_C1")))
    }),
    ("H704847_HL7_OCIE","FACILITYNAME") -> ((col: String, df: DataFrame) => {
      df.withColumn(col,when(df("PV1_F3_C4").isNotNull, concat(df("MSH_F4_C1"), lit("."), df("PV1_F3_C4")))
        .otherwise(df("MSH_F4_C1")))
    }),
    ("H770635_HL7_NG", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H770635_HL7_NG", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H942522_HL7", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CERNER_ALBIR", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CERNER_ALBIR", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CERNER_FLJAC", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CERNER_FLJAC", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CERNER_FLPEN", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CERNER_FLPEN", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CN_WIAPP", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_CN_WIAPP", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_INVIS_ALMOB", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_INVIS_INEVA", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_INVIS_INEVA", "FACILITYNAME") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_MCK_ALBIR", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984216_HL7_MS4_FLPEN", "FACILITYID") -> mapFrom("MSH_F4_C1"),
    ("H984833_HL7_MEDENT", "SPECIALTY") -> mapFrom("PV1_F3_C7")
     //TODO ("H591965_HL7_CFHA", "FACILITYNAME") -> mapFrom("DEPT")
  )

}

