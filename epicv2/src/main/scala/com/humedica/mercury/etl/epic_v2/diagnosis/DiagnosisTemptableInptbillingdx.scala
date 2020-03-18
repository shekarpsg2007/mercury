package com.humedica.mercury.etl.epic_v2.diagnosis
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.EntitySource
import com.humedica.mercury.etl.core.engine.Functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.expressions.Window


class DiagnosisTemptableInptbillingdx(config: Map[String, String]) extends EntitySource(config: Map[String, String]) {

  tables = List("inptbilling_txn","hsp_acct_pat_csn","inptbilling_acct", "excltxn:epic_v2.claim.ClaimExcltxn")

  columnSelect = Map(
    "inptbilling_txn" -> List("PAT_ENC_CSN_ID", "TX_POST_DATE","HSP_ACCOUNT_ID","TX_ID","ORIG_REV_TX_ID","TX_TYPE_HA_C","SERVICE_DATE","FILEID"),
    "hsp_acct_pat_csn" -> List("PAT_ENC_CSN_ID", "HSP_ACCOUNT_ID", "PAT_ENC_DATE","FILEID"),
    "inptbilling_acct" -> List("PRIM_ENC_CSN_ID", "HSP_ACCOUNT_ID", "ADM_DATE_TIME","INST_OF_UPDATE","HSP_ACCOUNT_NAME","PAT_ID"))


  cacheMe = true

  columns=List("HSP_ACCOUNT_ID_UN","PAT_ENC_CSN_ID_UN","SERVICE_DATE_UN")

  beforeJoin = Map(
    "inptbilling_acct" -> ((df: DataFrame) => {
      val groups = Window.partitionBy(df("PAT_ID"),df("HSP_ACCOUNT_ID")).orderBy(df("INST_OF_UPDATE").desc)
      df.withColumn("acct_rw", row_number.over(groups))
        .withColumn("ADM_DATE_TIME", substring(df("ADM_DATE_TIME"),1,10))
        .withColumnRenamed("PRIM_ENC_CSN_ID","PAT_ENC_CSN_ID_UN")
        .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_UN")
        .withColumnRenamed("ADM_DATE_TIME","SERVICE_DATE_UN")
        .filter("acct_rw =1 and PAT_ENC_CSN_ID_UN is not null and lower(HSP_ACCOUNT_NAME) NOT like 'zzz%'")
        .select("HSP_ACCOUNT_ID_UN","PAT_ENC_CSN_ID_UN","SERVICE_DATE_UN")
    }),
    "hsp_acct_pat_csn" -> ((df: DataFrame) =>{
      val groups = Window.partitionBy(df("HSP_ACCOUNT_ID"),df("PAT_ENC_CSN_ID")).orderBy(df("FILEID").desc)
      df.withColumn("hsp_rw", row_number.over(groups))
        .withColumn("PAT_ENC_DATE", substring(df("PAT_ENC_DATE"),1,10))
        .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_UN")
        .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_UN")
        .withColumnRenamed("PAT_ENC_DATE","SERVICE_DATE_UN")
        .filter("hsp_rw = 1 and PAT_ENC_CSN_ID_UN is not null")
        .select("HSP_ACCOUNT_ID_UN","PAT_ENC_CSN_ID_UN","SERVICE_DATE_UN")
    }),
    "inptbilling_txn" -> ((df: DataFrame) => {
      val fil = df.filter("(PAT_ENC_CSN_ID is not null or PAT_ENC_CSN_ID <> '-1') and (ORIG_REV_TX_ID is null or ORIG_REV_TX_ID = '-1') and TX_TYPE_HA_C = '1'").drop("ORIG_REV_TX_ID")
      val gtt = table("excltxn").withColumnRenamed("ORIG_REV_TX_ID","ORIG_REV_TX_ID_excl")
      val joined = fil.join(gtt, fil("TX_ID") === gtt("ORIG_REV_TX_ID_excl"), "left_outer")
      val fil2 = joined.filter("ORIG_REV_TX_ID_excl is null")
      val df1 = fil2.repartition(1000)
      val groups = Window.partitionBy(df1("HSP_ACCOUNT_ID"), df1("PAT_ENC_CSN_ID")).orderBy(df1("TX_POST_DATE").desc_nulls_last,df1("SERVICE_DATE").desc_nulls_last,df1("FILEID").desc)
      df1.withColumn("txn_rn", row_number.over(groups))
        .withColumn("SERVICE_DATE", substring(df("SERVICE_DATE"),1,10))
        .withColumnRenamed("PAT_ENC_CSN_ID","PAT_ENC_CSN_ID_UN")
        .withColumnRenamed("HSP_ACCOUNT_ID","HSP_ACCOUNT_ID_UN")
        .withColumnRenamed("SERVICE_DATE","SERVICE_DATE_UN")
        .filter("txn_rn =1 and PAT_ENC_CSN_ID_UN IS NOT NULL")
        .select("HSP_ACCOUNT_ID_UN","PAT_ENC_CSN_ID_UN","SERVICE_DATE_UN")
    })
  )

  afterMap = (df: DataFrame) => {
      val acc = table("inptbilling_acct")
      val hsp = table("hsp_acct_pat_csn")
      val txn = table("inptbilling_txn")
      val un1 = acc.union(hsp)
      val unn = un1.union(txn).distinct()
      val groups = Window.partitionBy(unn("HSP_ACCOUNT_ID_UN"), unn("PAT_ENC_CSN_ID_UN")).orderBy(unn("SERVICE_DATE_UN").desc_nulls_last)
      unn.withColumn("rw", row_number.over(groups))
         .filter("rw=1")
    }
}

