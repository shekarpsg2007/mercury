package com.humedica.mercury.toolkit.impl

import com.humedica.mercury.etl.core.schema.OracleJdbcParameters
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper}

object MetadataEntityToTableMappings {

  def retrieve(metadataParams: OracleJdbcParameters, spark: SparkSession): Map[String, String] = {
    val cdrTablesSql =
      """
        |(
        |select lower(regexp_replace(a.entity_name, '[[:space:]]+')) as entity_name, upper(b.cdr_table) as cdr_table
        |from metadata.cdr_entity a left outer join (
        |  select distinct entity_id, lower(regexp_substr(cdr_location, '[^.]*')) as cdr_table
        |  from metadata.cdr_entity_attr
        |  where cdr_location is not null
        |) b on (a.entity_id = b.entity_id)
        |) entityToTable
      """.stripMargin
    val cdrTablesDf = spark.read.jdbc(metadataParams.getJdbcUrl, cdrTablesSql, metadataParams.getConnectionProperties)
    Map(cdrTablesDf
      .filter(col("CDR_TABLE").isNotNull)
      .select(cdrTablesDf.columns.map(c => upper(col(c)).alias(c)): _*)
      .distinct().collect()
      .map(row => row.getAs[String](0) -> row.getAs[String](1)): _*
    )
  }

}
