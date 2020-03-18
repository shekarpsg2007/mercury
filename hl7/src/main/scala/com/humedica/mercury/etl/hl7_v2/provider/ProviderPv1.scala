package com.humedica.mercury.etl.hl7_v2.provider

import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by abendiganavale on 7/10/17.
  */
class ProviderPv1(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List(
    "temptable:hl7_v2.temptable.TemptableAll"
  )

  columnSelect = Map(
    "temptable" -> List("MESSAGEID","MSH_F4_C1","PV1_F7_C1","PV1_F7_C2","PV1_F7_C3","PV1_F7_C4","PV1_F7_C6"
      ,"PV1_F8_C1","PV1_F8_C2","PV1_F8_C3","PV1_F8_C4","PV1_F8_C6","PV1_F9_C1","PV1_F9_C2","PV1_F9_C3"
      ,"PV1_F9_C4","PV1_F9_C6","PV1_F17_C1","PV1_F17_C2","PV1_F17_C3","PV1_F17_C4","PV1_F17_C6","LASTUPDATEDATE")
  )

  beforeJoin = Map(
    "temptable" -> ((df : DataFrame) => {
      val df0 = df.drop("LOCALPROVIDERID")
      val fpiv = unpivot(
        Seq("PV1_F7_C1", "PV1_F8_C1", "PV1_F9_C1", "PV1_F17_C1"),
        Seq("F7","F8","F9","F19"), typeColumnName = "PROVCOL")

      val df1 = fpiv("LOCALPROVIDERID",df0)
      df1.filter("LOCALPROVIDERID is not null and LOCALPROVIDERID not in ('\"\"','00000')")
    })
  )

  beforeJoinExceptions = Map(
    "H704847_HL7_OCIE" -> Map(
      "temptable" -> ((df: DataFrame) => {
        val df0 = df.drop("LOCALPROVIDERID")
        val fpiv = unpivot(
          Seq("PV1_F7_C1", "PV1_F8_C1", "PV1_F9_C1", "PV1_F17_C1"),
          Seq("F7","F8","F9","F17"),typeColumnName = "PROVCOL"
        )

        val prov = fpiv("LOCALPROVIDERID",df0)
        val df1 = prov.withColumn("LOCALPROVIDERID", concat(prov("MSH_F4_C1"), lit("."), prov("LOCALPROVIDERID")))

        df1.filter("LOCALPROVIDERID is not null and LOCALPROVIDERID not in ('\"\"','00000')")
      })),

    "H704847_HL7_CCD" -> Map(
      "temptable" -> ((df: DataFrame) => {
        val df0 = df.drop("LOCALPROVIDERID")
        val fpiv = unpivot(
          Seq("PV1_F7_C1", "PV1_F8_C1", "PV1_F9_C1", "PV1_F17_C1"),
          Seq("F7","F8","F9","F17"),typeColumnName = "PROVCOL"
        )

        val prov = fpiv("LOCALPROVIDERID",df0)
        val df1 = prov.withColumn("LOCALPROVIDERID", concat(prov("MSH_F4_C1"), lit("."), prov("LOCALPROVIDERID")))

        df1.filter("LOCALPROVIDERID is not null and LOCALPROVIDERID not in ('\"\"','00000')")
      }))
  )


  join = noJoin()

  map = Map(
    "DATASRC" -> literal("hl7_segment_pv1_a"),
    "FIRST_NAME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROVCOL") === "F7", df("PV1_F7_C3"))
        .when(df("PROVCOL") === "F8", df("PV1_F8_C3"))
        .when(df("PROVCOL") === "F9", df("PV1_F9_C3"))
        .when(df("PROVCOL") === "F17", df("PV1_F17_C3"))
        .otherwise(null))
    }),
    "LAST_NAME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROVCOL") === "F7", df("PV1_F7_C2"))
        .when(df("PROVCOL") === "F8", df("PV1_F8_C2"))
        .when(df("PROVCOL") === "F9", df("PV1_F9_C2"))
        .when(df("PROVCOL") === "F17", df("PV1_F17_C2"))
        .otherwise(null))
    }),
    "MIDDLE_NAME" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROVCOL") === "F7", df("PV1_F7_C4"))
        .when(df("PROVCOL") === "F8", df("PV1_F8_C4"))
        .when(df("PROVCOL") === "F9", df("PV1_F9_C4"))
        .when(df("PROVCOL") === "F17", df("PV1_F17_C4"))
        .otherwise(null))
    }),
    "CREDENTIALS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROVCOL") === "F7", df("PV1_F7_C6"))
        .when(df("PROVCOL") === "F8", df("PV1_F8_C6"))
        .when(df("PROVCOL") === "F9", df("PV1_F9_C6"))
        .when(df("PROVCOL") === "F17", df("PV1_F17_C6"))
        .otherwise(null))
    })

  )

  afterMap = (df: DataFrame) => {
    val groups = Window.partitionBy(df("LOCALPROVIDERID")).orderBy(df("LASTUPDATEDATE").desc,length(df("LAST_NAME")).desc)
    val addColumn = df.withColumn("rn", row_number.over(groups))
    addColumn.filter("rn = 1").drop("rn").drop("PROVCOL")
  }

  //TODO exceptions
  mapExceptions = Map(
    ("H704847_HL7_CCD", "MIDDLE_NAME") -> nullValue(),
    ("H704847_HL7_OCIE", "MIDDLE_NAME") -> nullValue(),
    ("H704847_HL7_CCD", "CREDENTIALS") -> nullValue(),
    ("H704847_HL7_OCIE", "CREDENTIALS") -> nullValue(),
    ("H557454_HL7","MIDDLE_NAME") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROVCOL") === "F7", regexp_extract(df("PV1_F7_C1"), "^\\w+(\\s?(\\w+)?)*", 1))
        .when(df("PROVCOL") === "F8", regexp_extract(df("PV1_F8_C6"), "^\\w+(\\s?(\\w+)?)*", 1))
        .when(df("PROVCOL") === "F9", regexp_extract(df("PV1_F9_C6"), "^\\w+(\\s?(\\w+)?)*", 1))
        .when(df("PROVCOL") === "F17", regexp_extract(df("PV1_F17_C6"), "^\\w+(\\s?(\\w+)?)*", 1))
        .otherwise(null))
    }),
    ("H557454_Hl7", "CREDENTIALS") -> ((col: String, df: DataFrame) => {
      df.withColumn(col, when(df("PROVCOL") === "F7", regexp_extract(df("PV1_F7_C1"), "\\((.*)\\)", 1))
        .when(df("PROVCOL") === "F8", regexp_extract(df("PV1_F8_C6"), "\\((.*)\\)", 1))
        .when(df("PROVCOL") === "F9", regexp_extract(df("PV1_F9_C6"), "\\((.*)\\)", 1))
        .when(df("PROVCOL") === "F17", regexp_extract(df("PV1_F17_C6"), "\\((.*)\\)", 1))
        .otherwise(null))
    })
  )

}
