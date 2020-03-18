package com.humedica.mercury.etl.core.schema

import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

case class OracleJdbcParameters(jdbcId: String,
                                jdbcUser: String, jdbcPassword: String,
                                jdbcHost: String, jdbcPort: Integer,
                                jdbcService: String, jdbcFetchSize: Int = 10000) {
  def validate(): Unit = {
    require(jdbcId != null, "Missing jdbc connection id")
    require(jdbcUser != null, s"$jdbcId - Missing jdbc user")
    require(jdbcPassword != null, s"$jdbcId - Missing jdbc password")
    require(jdbcHost != null, s"$jdbcId - Missing jdbc host")
    require(jdbcPort != null, s"$jdbcId - Missing jdbc port")
    require(jdbcService != null, s"$jdbcId - Missing jdbc service id")
  }
  def getJdbcUrl: String = { s"jdbc:oracle:thin:@$jdbcHost:$jdbcPort/$jdbcService" }
  def getConnectionProperties: Properties = {
    val connProps = new Properties()
    connProps.put("user", jdbcUser)
    connProps.put("password", jdbcPassword)
    connProps.put(JDBCOptions.JDBC_BATCH_FETCH_SIZE, jdbcFetchSize.toString)
    connProps.put(JDBCOptions.JDBC_DRIVER_CLASS, "oracle.jdbc.OracleDriver")
    connProps
  }
}

object OracleJdbcParametersIn {
  def read: Map[String, OracleJdbcParameters] = {
    Map(
      System.getProperty("jdbc.ids").split(",").map(jdbcId =>
        jdbcId -> OracleJdbcParameters(jdbcId,
          System.getProperty(s"$jdbcId.user"),
          System.getProperty(s"$jdbcId.password"),
          System.getProperty(s"$jdbcId.host"),
          System.getProperty(s"$jdbcId.port").toInt,
          System.getProperty(s"$jdbcId.service")
        )
      ): _*
    )
  }
}
