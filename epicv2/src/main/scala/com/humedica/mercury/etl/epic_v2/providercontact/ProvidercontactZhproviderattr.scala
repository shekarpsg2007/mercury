package com.humedica.mercury.etl.epic_v2.providercontact

import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class ProvidercontactZhproviderattr(config: Map[String,String]) extends EntitySource(config: Map[String,String]) {

  tables = List("zh_providerattr")

  beforeJoin = Map(
    "zh_providerattr" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PROV_ID")).orderBy(df("EXTRACT_DATE").desc)
      df.withColumn("rn", row_number.over(groups))
        .filter("rn =1")
    })
  )

  
  join = noJoin()


  map = Map(
    "DATASRC" -> literal("zh_providerattr"),
    "LOCAL_PROVIDER_ID" -> mapFrom("PROV_ID", nullIf=Seq("-1")),
    "LOCAL_CONTACT_TYPE" -> literal("PROVIDER"),
    "WORK_PHONE" -> mapFrom("OFFICE_PHONE_NUM"),
    "UPDATE_DATE" -> mapFrom("EXTRACT_DATE"),
    "EMAIL_ADDRESS" -> ((col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("EMAIL").like("%@%.%"), df("EMAIL")).otherwise(null))
    })
  )


  afterMap = (df: DataFrame) => {
    df.filter("LOCAL_PROVIDER_ID is not null and EXTRACT_DATE is not null and (WORK_PHONE is not null or EMAIL_ADDRESS is not null)")
  }

}