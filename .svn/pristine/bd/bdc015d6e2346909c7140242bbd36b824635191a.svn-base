package com.humedica.mercury.etl.asent.facility

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.expressions.Window

class FacilityZclocation(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("as_zc_location_de")

  columnSelect = Map(
    "as_zc_location_de" -> List("ID", "EFFECTIVEDT", "ISCURRENTFLAG", "ENTRYNAME", "ENTRYMNEMONIC")
  )


  beforeJoin = Map(
    "as_zc_location_de" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("ID")).orderBy(df("EFFECTIVEDT").desc_nulls_last)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn = 1 and ISCURRENTFLAG = 'Y'").drop("rn")
        .withColumn("facName", when(df("ENTRYNAME").contains(df("ENTRYMNEMONIC")), df("ENTRYNAME")).otherwise(concat(df("ENTRYNAME"), lit(" - "), df("ENTRYMNEMONIC"))))
    })
  )

  map = Map(
    "FACILITYID" -> mapFrom("ID"),
    "FACILITYNAME" -> mapFrom("facName")

  )
}


// Test
//   val bfl = new FacilityZclocation(cfg) ; val fl = build(bfl); fl.show(false); fl.count
