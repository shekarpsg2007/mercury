package com.humedica.mercury.etl.crossix.util

import com.humedica.mercury.etl.crossix.orxmembercoverage.OrxmembercoverageOrxmembercoverage
import com.humedica.mercury.etl.crossix.orxmembercoveragepartd.OrxmembercoveragepartdOrxmembercoveragepartd
import com.humedica.mercury.etl.crossix.orxmemberphi.OrxmemberphiOrxmemberphi
import com.humedica.mercury.etl.crossix.orxpharmacyclaim.OrxpharmacyclaimOrxpharmacyclaim
import com.humedica.mercury.etl.crossix.orxpharmacyclaimpartd.OrxpharmacyclaimpartdOrxpharmacyclaimpartd
import org.apache.hadoop.util.StringUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



import com.databricks.spark.avro.SchemaConverters
import java.io._
import org.apache.avro.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType


/**
  * Created by rbabu on 7/26/17.
  **/

//   prepareWeeklyORXInputData(sqlContext, "20180503,20180504,20180505,20180506,20180507,20180508,20180509", cfg)

object CrossixORXOpUtility {

    def prepareWeeklyMemberPHInputData(sqlContext: SQLContext, datesStr: String, config: Map[String, String]) = {

        var srcLocation = "/optum/crossix/data/optumrx/incremental/exports/%s/%s"
        var targetLocation = "/user/rbabu/data/orx/weekly/MEMBER_PHI/%s/%s"
        var dates = datesStr.split(",")
        val targetDir: String = "" + dates(0) + "_" + dates(dates.length - 1)
        var tableName = "MEMBER"
        for (elem <- dates) {
            var src = srcLocation.format(elem, tableName)
            var dest = targetLocation.format(targetDir, tableName)
            try {
                sqlContext.read.parquet(src).write.mode("append").parquet(dest)
                println("SUCCESS:  => " + src + " ==> " + dest)
            } catch {
                case ex: Exception => {
                    println("FAILED:  => " + src + " ==> " + dest + " : " + ex.getMessage)
                }
            }
        }
    }


    //    prepareWeeklyORXInputData(sqlContext, "20180419,20180420,20180421,20180422,20180423,20180424,20180425", cfg)

    def prepareWeeklyORXInputData(sqlContext: SQLContext, datesStr: String, config: Map[String, String]) = {
        var home_dir = config.get("HOME_DIR").get
        var srcLocation = config.get("ORX_INPUT_DATA_SRC_LOC").get
        var targetLocation = home_dir+"/data/orx/weekly/%s/%s"

        var dates = datesStr.split(",")
        val targetDir: String = dates(0) + "_" + dates(dates.length - 1)

        var mbrMcrPartdHistory = home_dir+"/data/orx/MBR_MCR_PARTD_HISTORY"
        var memberEligibilityHistory = home_dir+"/data/orx/MEMBER_ELIGIBILITY_HISTORY"

        processWeeklyORX(sqlContext, dates, "MBR_MCR_PARTD", srcLocation, targetLocation)
        processWeeklyORX(sqlContext, dates, "CLAIM", srcLocation, targetLocation)
        processWeeklyORX(sqlContext, dates, "MEMBER", srcLocation, targetLocation)
        processWeeklyORX(sqlContext, dates, "MEMBER_ELIGIBILITY", srcLocation, targetLocation)

        var baseDir = home_dir+"/data/orx/weekly/%s".format(targetDir)

        sqlContext.read.parquet(baseDir + "/MBR_MCR_PARTD/*").write.mode("append").parquet(mbrMcrPartdHistory)
        sqlContext.read.parquet(baseDir + "/MEMBER_ELIGIBILITY/*").write.mode("append").parquet(memberEligibilityHistory)

        sqlContext.read.parquet(memberEligibilityHistory)
                .select("FLCPDA", "FLCQDA", "FLAACD", "FLACCD", "FLADCD", "FLABCD")
                .distinct().write.parquet(baseDir + "/MEMBER_ELIGIBILITY_HISTORY")

        sqlContext.read.parquet(mbrMcrPartdHistory)
                .select("A0OCHG", "A0ODHG", "A0AACD", "A0ACCD", "A0ADCD", "A0ABCD")
                .distinct().write.parquet(baseDir + "/MBR_MCR_PARTD_HISTORY")


        println("COPYING " + config.get("NDC_WEEKLY_MASTER").get + " To " + baseDir + "/NDC")
        sqlContext.read.parquet(config.get("NDC_WEEKLY_MASTER").get)
                .select("AHFS_THERAPEUTIC_CLASS", "BRAND_NAME", "DRUG_PRICE_TYPE_CODE", "DRUG_STRENGTH_DESC", "GENERIC", "SPECIFIC_THERAPEUTIC_CLASS", "CODE", "EFF_DATE", "END_DATE")
                .distinct().write.parquet(baseDir + "/NDC")

        println("COPYING " + config.get("PHARMACY_WEEKLY_MASTER").get + " To " + baseDir + "/PHARMACY")
        sqlContext.read.parquet(config.get("PHARMACY_WEEKLY_MASTER").get)
                .select("LSRD_NABP_NBR", "DISPENSE_TYPE_CODE", "NC_SPECIALTY_PHMCY", "NABP_NBR")
                .distinct().write.parquet(baseDir + "/PHARMACY")

        println("COPYING " + config.get("MEMBERPHI_WEEKLY_MASTER").get + " To " + baseDir + "/MEMBER_PHI")
        sqlContext.read.parquet(config.get("MEMBERPHI_WEEKLY_MASTER").get)
                .select("LSRD_INDIVIDUAL_ID", "SOURCE_MEMBER_ID")
                .distinct().write.parquet(baseDir + "/MEMBER_PHI")

    }

    def processWeeklyORX(sqlContext: SQLContext, dates: Array[String], tableName: String, srcLocation: String, targetLocation: String): Unit = {
        val targetDir: String = dates(0) + "_" + dates(dates.length - 1)
        for (elem <- dates) {
            var src = srcLocation.format(elem, tableName)
            var dest = targetLocation.format(targetDir, tableName)
            try {
                var count = sqlContext.read.parquet(src).count
                println("SUCCESS:  => Copying " +count+ " Rows "+ src + " ==> " + dest)
                sqlContext.read.parquet(src).write.mode("append").parquet(dest)
            }catch {
                case ex:Exception => {
                    println("FAILED:  => " + src + " ==> " + dest + " : " + ex.getMessage)
                }
            }
        }
    }


    /*

    def processWeeklyORX(sqlContext: SQLContext, dates: Array[String], tableName: String, srcLocation: String, targetLocation: String): Unit = {
        val targetDir: String = dates(0) + "_" + dates(dates.length - 1)
        for (elem <- dates) {
            var src = srcLocation.format(elem, tableName)
            var dest = targetLocation.format(targetDir, tableName)
            try {
                sqlContext.read.parquet(src).write.mode("append").parquet(dest)
                println("SUCCESS:  => " + src + " ==> " + dest)
            } catch {
                case ex: Exception => {
                    println("FAILED:  => " + src + " ==> " + dest + " : " + ex.getMessage)
                }
            }
        }
    }

    */
    def findClaimsMembersPardDAndNonPartDStats(sqlContext: SQLContext, dateStr: String, config: Map[String, String]) = {
        findClaimsNotInPardDAndNonPartD(sqlContext, dateStr, config)
        findClaimsInBothPardDAndNonPartD(sqlContext, dateStr, config)
        findMembersNotInPardDAndNonPartD(sqlContext, dateStr, config)
        findMembersInBothPardDAndNonPartD(sqlContext, dateStr, config)
    }

    def findClaimsNotInPardDAndNonPartD(sqlContext: SQLContext, dateStr: String, config: Map[String, String]) = {
        var home_dir = config.get("HOME_DIR").get

        var dest = home_dir + "/data/orx/weekly/" + dateStr + "/QC_CLAIMS_NOTINBOTH_PARTD_AND_NONPARTD"
        var claim = sqlContext.read.parquet(home_dir+"/data/orx/weekly/" + dateStr + "/CLAIM").select("CARRIERID", "GROUPID", "MEMBERID", "ACCOUNTID").distinct
        var nonpartd = sqlContext.read.parquet(home_dir+"/data/orx/weekly/" + dateStr + "/MEMBER_ELIGIBILITY_HISTORY").select("FLAACD", "FLADCD", "FLABCD", "FLACCD").distinct
        var partd = sqlContext.read.parquet(home_dir+"/data/orx/weekly/" + dateStr + "/MBR_MCR_PARTD_HISTORY").select("A0AACD", "A0ADCD", "A0ABCD", "A0ACCD").distinct

        var j1 = claim.join(nonpartd, claim("CARRIERID") === nonpartd("FLAACD") && claim("GROUPID") === nonpartd("FLADCD") && claim("MEMBERID") === nonpartd("FLABCD") && claim("ACCOUNTID") === nonpartd("FLACCD"), "left_outer")
                .join(partd, claim("CARRIERID") === partd("A0AACD") && claim("GROUPID") === partd("A0ADCD") && claim("MEMBERID") === partd("A0ABCD") && claim("ACCOUNTID") === partd("A0ACCD"), "left_outer")
        var result = j1.filter("FLAACD IS NULL and A0AACD IS NULL").select("CARRIERID", "GROUPID", "MEMBERID", "ACCOUNTID").distinct()
        result = result.withColumn("ORX_INDIVIDUAL_ID", sha2(concat(result("CARRIERID"), result("ACCOUNTID"), result("GROUPID").substr(1, 8), result("MEMBERID")), 256))

        result.write.parquet(dest)
        println("Week : " + dateStr + " : QC_CLAIMS_NOTINBOTH_PARTD_AND_NONPARTD : COUNT " + result.count + "  :  LOCATION :" + dest)
        result.show
    }


    def findClaimsInBothPardDAndNonPartD(sqlContext: SQLContext, dateStr: String, config: Map[String, String]) = {
        var home_dir = config.get("HOME_DIR").get

        var dest = home_dir+"/data/orx/weekly/" + dateStr + "/QC_CLAIMS_INBOTH_PARTD_AND_NONPARTD"

        var claim = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/CLAIM").select("CARRIERID", "GROUPID", "MEMBERID", "ACCOUNTID").distinct
        var nonpartd = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/MEMBER_ELIGIBILITY_HISTORY").select("FLAACD", "FLADCD", "FLABCD", "FLACCD").distinct
        var partd = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/MBR_MCR_PARTD_HISTORY").select("A0AACD", "A0ADCD", "A0ABCD", "A0ACCD").distinct

        var j1 = claim.join(nonpartd, claim("CARRIERID") === nonpartd("FLAACD") && claim("GROUPID") === nonpartd("FLADCD") && claim("MEMBERID") === nonpartd("FLABCD") && claim("ACCOUNTID") === nonpartd("FLACCD"), "inner")
                .join(partd, claim("CARRIERID") === partd("A0AACD") && claim("GROUPID") === partd("A0ADCD") && claim("MEMBERID") === partd("A0ABCD") && claim("ACCOUNTID") === partd("A0ACCD"), "inner")
                .select("CARRIERID", "GROUPID", "MEMBERID", "ACCOUNTID").distinct()
        j1 = j1.withColumn("ORX_INDIVIDUAL_ID", sha2(concat(j1("CARRIERID"), j1("ACCOUNTID"), j1("GROUPID").substr(1, 8), j1("MEMBERID")), 256))

        j1.write.parquet(dest)
        println("Week : " + dateStr + " : QC_CLAIMS_INBOTH_PARTD_AND_NONPARTD : COUNT " + j1.count + "  :  LOCATION :" + dest)
        j1.show
    }


    def findMembersNotInPardDAndNonPartD(sqlContext: SQLContext, dateStr: String, config: Map[String, String]) = {
        var home_dir = config.get("HOME_DIR").get

        var dest = home_dir + "/data/orx/weekly/" + dateStr + "/QC_MEMBERS_NOTINBOTH_PARTD_AND_NONPARTD"

        var member = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/MEMBER").select("ABAACD", "ABADCD", "ABACCD", "ABABCD").distinct
        var nonpartd = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/MEMBER_ELIGIBILITY_HISTORY").select("FLAACD", "FLADCD", "FLABCD", "FLACCD").distinct
        var partd = sqlContext.read.parquet(home_dir+"/data/orx/weekly/" + dateStr + "/MBR_MCR_PARTD_HISTORY").select("A0AACD", "A0ADCD", "A0ABCD", "A0ACCD").distinct

        var j1 = member.join(nonpartd, member("ABAACD") === nonpartd("FLAACD") && member("ABADCD") === nonpartd("FLADCD") && member("ABACCD") === nonpartd("FLABCD") && member("ABABCD") === nonpartd("FLACCD"), "left_outer")
                .join(partd, member("ABAACD") === partd("A0AACD") && member("ABADCD") === partd("A0ADCD") && member("ABACCD") === partd("A0ACCD") && member("ABABCD") === partd("A0ABCD"), "left_outer")
        var result = j1.filter("FLAACD IS NULL and A0AACD IS NULL").select("ABAACD", "ABADCD", "ABACCD", "ABABCD").distinct()
        result.write.parquet(dest)
        println("Week : " + dateStr + " : QC_MEMBERS_NOTINBOTH_PARTD_AND_NONPARTD : COUNT " + result.count + "  :  LOCATION :" + dest)
        result.show
    }


    def findMembersInBothPardDAndNonPartD(sqlContext: SQLContext, dateStr: String, config: Map[String, String]) = {
        var home_dir = config.get("HOME_DIR").get

        var dest = home_dir + "/data/orx/weekly/" + dateStr + "/QC_MEMBERS_INBOTH_PARTD_AND_NONPARTD"

        var member = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/MEMBER").select("ABAACD", "ABADCD", "ABACCD", "ABABCD").distinct
        var nonpartd = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/MEMBER_ELIGIBILITY_HISTORY").select("FLAACD", "FLADCD", "FLABCD", "FLACCD").distinct
        var partd = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/MBR_MCR_PARTD_HISTORY").select("A0AACD", "A0ADCD", "A0ABCD", "A0ACCD").distinct

        var j1 = member.join(nonpartd, member("ABAACD") === nonpartd("FLAACD") && member("ABADCD") === nonpartd("FLADCD") && member("ABACCD") === nonpartd("FLACCD") && member("ABABCD") === nonpartd("FLABCD"), "inner")
                .join(partd, member("ABAACD") === partd("A0AACD") && member("ABADCD") === partd("A0ADCD") && member("ABACCD") === partd("A0ACCD") && member("ABABCD") === partd("A0ABCD"), "inner")
                .select("ABAACD", "ABADCD", "ABACCD", "ABABCD").distinct()

        j1.write.parquet(dest)
        println("Week : " + dateStr + " : QC_MEMBERS_INBOTH_PARTD_AND_NONPARTD : COUNT " + j1.count + "  :  LOCATION :" + dest)
        j1.show
    }

    def preValidateORXTClaimsColumns(sqlContext: SQLContext, dateStr: String, config: Map[String, String]): Unit = {
        var home_dir = config.get("HOME_DIR").get

        var baseDir = home_dir + "/data/orx/weekly/%s".format(dateStr)

        sqlContext.read.parquet(baseDir + "/CLAIM").withColumn("ROWID", monotonically_increasing_id()).write.parquet(baseDir + "/PREVALIDATE/CLAIM")
        var claim = sqlContext.read.parquet(baseDir + "/PREVALIDATE/CLAIM")
        var member_eligibility_history = sqlContext.read.parquet(baseDir + "/MEMBER_ELIGIBILITY_HISTORY").select("FLAACD", "FLACCD", "FLADCD", "FLABCD")
        var j1 = claim
                .join(member_eligibility_history, member_eligibility_history("FLAACD") === claim("CARRIERID") &&
                        member_eligibility_history("FLADCD") === claim("GROUPID") &&
                        member_eligibility_history("FLABCD") === claim("MEMBERID") &&
                        member_eligibility_history("FLACCD") === claim("ACCOUNTID"), "inner")
        println("CLAIM COUNT " + claim.count())
        println("MEH COUNT " + member_eligibility_history.count())
        println("CLAIM+MEH COUNT " + j1.count())


        var mbr_mcr_partd_history = sqlContext.read.parquet(baseDir + "/MBR_MCR_PARTD_HISTORY").select("A0ABCD", "A0AACD", "A0ACCD", "A0ADCD")
        var j2 = claim.join(mbr_mcr_partd_history, mbr_mcr_partd_history("A0AACD") === claim("CARRIERID") &&
                mbr_mcr_partd_history("A0ADCD") === claim("GROUPID") &&
                mbr_mcr_partd_history("A0ABCD") === claim("MEMBERID") &&
                mbr_mcr_partd_history("A0ACCD") === claim("ACCOUNTID"), "inner")
        println("MBR COUNT " + j2.count())
        println("CLAIM+MBR COUNT " + j1.count())


        j1.select("ROWID").distinct().write.parquet(baseDir + "/PREVALIDATE/J1ROWID")
        j2.select("ROWID").distinct().write.mode("append").parquet(baseDir + "/PREVALIDATE/J1ROWID")

        println("FILL_DATE - DTEFILLED (ASC) ===>")
        claim.groupBy("DTEFILLED").count.orderBy(claim("DTEFILLED").asc).show(100)

        println("PAID_DATE - ORGPDSBMDT (DESC) ===>")
        claim.groupBy("ORGPDSBMDT").count.orderBy(claim("ORGPDSBMDT").desc).show(100)


        println("CLAIMS PATH = " + baseDir + "/PREVALIDATE/CLAIM")
        println("RESULT PATH = " + baseDir + "/PREVALIDATE/J1ROWID")

    }


    def preValidateORXTMemberColumns(sqlContext: SQLContext, dateStr: String, config: Map[String, String]): Unit = {
        var home_dir = config.get("HOME_DIR").get

        var baseDir = home_dir + "/data/orx/weekly/%s".format(dateStr)

        //  sqlContext.read.parquet(baseDir+"/MEMBER").withColumn("ROWID", monotonically_increasing_id()).write.parquet(baseDir+"/PREVALIDATE/MEMBER")
        var member = sqlContext.read.parquet(baseDir + "/PREVALIDATE/MEMBER")
        var member_eligibility_history = sqlContext.read.parquet(baseDir + "/MEMBER_ELIGIBILITY_HISTORY").select("FLAACD", "FLACCD", "FLADCD", "FLABCD")
        var j1 = member
                .join(member_eligibility_history,
                    member("ABAACD") === member_eligibility_history("FLAACD") &&
                            member("ABADCD") === member_eligibility_history("FLADCD") &&
                            member("ABACCD") === member_eligibility_history("FLACCD") &&
                            member("ABABCD") === member_eligibility_history("FLABCD"), "inner")
        println("MEMBER COUNT " + member.count())
        println("MEH COUNT " + member_eligibility_history.count())
        println("MEMBER+MEH COUNT " + j1.count())

        var mbr_mcr_partd_history = sqlContext.read.parquet(baseDir + "/MBR_MCR_PARTD_HISTORY").select("A0ABCD", "A0AACD", "A0ACCD", "A0ADCD")
        var j2 = member.join(mbr_mcr_partd_history, mbr_mcr_partd_history("A0AACD") === member("ABAACD") &&
                mbr_mcr_partd_history("A0ADCD") === member("GROUPID") &&
                mbr_mcr_partd_history("A0ABCD") === member("MEMBERID") &&
                mbr_mcr_partd_history("A0ACCD") === member("ACCOUNTID"), "inner")
        println("MBR COUNT " + j2.count())
        println("MEMBER+MBR COUNT " + j1.count())


        j1.select("ROWID").distinct().write.parquet(baseDir + "/PREVALIDATE/J1MBRROWID")
        j2.select("ROWID").distinct().write.mode("append").parquet(baseDir + "/PREVALIDATE/J1MBRROWID")

        println("EFF_DATE (ASC) ===>")
        member.groupBy("A0OCHG").count.orderBy(member("A0OCHG").asc).show()

        println("EFF_DATE (DESC) ===>")
        member.groupBy("A0OCHG").count.orderBy(member("A0OCHG").asc).show()

        println("END_DATE (ASC) ===>")
        member.groupBy("A0ODHG").count.orderBy(member("A0ODHG").asc).show()

        println("END_DATE (DESC) ===>")
        member.groupBy("A0ODHG").count.orderBy(member("A0ODHG").asc).show()



        println("YEAR_OF_BIRTH (DESC) ===>")
        member.groupBy("A0OCHG").count.orderBy("A0OCHG").show()
        println("YEAR_OF_BIRTH (DESC) ===>")
        member.groupBy("A0ODHG").count.orderBy("A0ODHG").show()


        println("CLAIMS PATH = " + baseDir + "/PREVALIDATE/CLAIM")
        println("RESULT PATH = " + baseDir + "/PREVALIDATE/J1ROWID")

    }


    def validateDateColumn(orxtable: String, col: String, df: DataFrame) = {
        var column1 = trim(df(col))
        val column2 = df.filter((column1.isNull.or(column1 === "0100-01-01").or(column1.startsWith("20")).or(column1.startsWith("19")).or(column1.startsWith("18"))) === false)
        var c2 = column2.count()
        if (c2 > 0) {
            throw new Exception(orxtable + " ==> " + col + " INVALID DATE VALUE")
        }
        var c1 = df.count()
        var c3 = df.count()
        if (c1 < c3) {
            throw new Exception(orxtable + " ==> " + col + " MISSING SOME VALID DATE VALUEDS (" + c2 + "/" + c3 + ")")
        }
    }

    def postValidateORXTableColumns(sqlContext: SQLContext, dateStr: String, config: Map[String, String]): Unit = {
        var home_dir = config.get("HOME_DIR").get

        println("Validating ... " + home_dir + "/data/orx/weekly/" + dateStr + "/ORX_MEMBER_COVERAGE")
        var ORX_MEMBER_COVERAGE = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/ORX_MEMBER_COVERAGE")
        var result = arrayEquals(Array("BUSINESS_LINE_CODE", "CUSTOMER_SEGMENT_NBR", "D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "INDUSTRY_PRODUCT_CODE",
            "ORX_INDIVIDUAL_ID", "STATE_CODE", "YEAR_OF_BIRTH"), ORX_MEMBER_COVERAGE.columns)
        if (result == false) {
            throw new Exception("ORX_MEMBER_COVERAGE ==> FAILED COLUMN COMPARISON")
        }

        validateDateColumn("ORX_MEMBER_COVERAGE", "EFF_DATE", ORX_MEMBER_COVERAGE)
        validateDateColumn("ORX_MEMBER_COVERAGE", "END_DATE", ORX_MEMBER_COVERAGE)
        println("ORX_MEMBER_COVERAGE EFF_DATE - (ASC) ===>")
        ORX_MEMBER_COVERAGE.groupBy("EFF_DATE").count.orderBy(ORX_MEMBER_COVERAGE("EFF_DATE").asc).show()

        println("ORX_MEMBER_COVERAGE END_DATE - ORGPDSBMDT (DESC) ===>")
        ORX_MEMBER_COVERAGE.groupBy("END_DATE").count.orderBy(ORX_MEMBER_COVERAGE("END_DATE").desc).show()

        println("Validating ... " + home_dir + "/data/orx/weekly/" + dateStr + "/ORX_MEMBER_COVERAGE_PARTD")

        var ORX_MEMBER_COVERAGE_PARTD = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/ORX_MEMBER_COVERAGE_PARTD")
        result = arrayEquals(Array("D_ZIPCODE_3", "EFF_DATE", "END_DATE", "GENDER", "ORX_INDIVIDUAL_ID", "STATE_CODE", "YEAR_OF_BIRTH"), ORX_MEMBER_COVERAGE_PARTD.columns)
        if (result == false) {
            throw new Exception("ORX_MEMBER_COVERAGE_PARTD ==> FAILED COLUMN COMPARISON")
        }
        validateDateColumn("ORX_MEMBER_COVERAGE_PARTD", "EFF_DATE", ORX_MEMBER_COVERAGE_PARTD)
        validateDateColumn("ORX_MEMBER_COVERAGE_PARTD", "END_DATE", ORX_MEMBER_COVERAGE_PARTD)
        validateDateColumn("ORX_MEMBER_COVERAGE_PARTD", "YEAR_OF_BIRTH", ORX_MEMBER_COVERAGE_PARTD)

        println("Validating ... " + home_dir + "/data/orx/weekly/" + dateStr + "/ORX_PHARMACY_CLAIM")

        var ORX_PHARMACY_CLAIM = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/ORX_PHARMACY_CLAIM").drop("ID")
        result = arrayEquals(Array("AHFS_THERAPEUTIC_CLASS", "AMT_AVERAGE_WHOLESALE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_DISPENSING_FEE",
            "BRAND_NAME", "CLAIM_SEQUENCE_NBR", "CLAIM_STATUS", "CODE", "COUNT_DAYS_SUPPLY", "DAW_CODE", "DRUG_PRICE_TYPE_CODE",
            "DRUG_STRENGTH_DESC", "FILL_DATE", "FIRST_FILL_IND", "FORMULARY_IND", "FORMULARY_TYPE_CODE", "GENERIC_IND",
            "LSRD_NABP_NBR", "MAIL_ORDER_IND", "ORX_CLAIM_NBR", "ORX_INDIVIDUAL_ID", "PAID_DATE", "PHRM_NPI_NBR", "PRESCRIPTION_NBR", "PRSC_NPI_NBR",
            "QUANTITY_DRUG_UNITS", "REFILL_NBR", "SPECIALTY_PHMCY", "SPECIFIC_THERAPEUTIC_CLASS"), ORX_PHARMACY_CLAIM.columns)
        if (result == false) {
            throw new Exception("ORX_PHARMACY_CLAIM ==> FAILED COLUMN COMPARISON")
        }
        validateDateColumn("ORX_PHARMACY_CLAIM", "FILL_DATE", ORX_PHARMACY_CLAIM)
        validateDateColumn("ORX_PHARMACY_CLAIM", "PAID_DATE", ORX_PHARMACY_CLAIM)


        println("ORX_PHARMACY_CLAIM FILL_DATE - DTEFILLED (ASC) ===>")
        ORX_PHARMACY_CLAIM.groupBy("FILL_DATE").count.orderBy(ORX_PHARMACY_CLAIM("FILL_DATE").asc).show()
        println("ORX_PHARMACY_CLAIM FILL_DATE - DTEFILLED (DESC) ===>")
        ORX_PHARMACY_CLAIM.groupBy("FILL_DATE").count.orderBy(ORX_PHARMACY_CLAIM("FILL_DATE").desc).show()

        println("ORX_PHARMACY_CLAIM PAID_DATE - ORGPDSBMDT (ASC) ===>")
        ORX_PHARMACY_CLAIM.groupBy("PAID_DATE").count.orderBy(ORX_PHARMACY_CLAIM("PAID_DATE").asc).show()
        println("ORX_PHARMACY_CLAIM PAID_DATE - ORGPDSBMDT (DESC) ===>")
        ORX_PHARMACY_CLAIM.groupBy("PAID_DATE").count.orderBy(ORX_PHARMACY_CLAIM("PAID_DATE").desc).show()


        println("Validating ... " + home_dir + "/data/orx/weekly/" + dateStr + "/ORX_PHARMACY_CLAIM_PARTD")

        var ORX_PHARMACY_CLAIM_PARTD = sqlContext.read.parquet(home_dir + "/data/orx/weekly/" + dateStr + "/ORX_PHARMACY_CLAIM_PARTD")
        result = arrayEquals(Array("AHFS_THERAPEUTIC_CLASS", "AMT_AVERAGE_WHOLESALE", "AMT_COPAY", "AMT_DEDUCTIBLE", "AMT_DISPENSING_FEE",
            "BRAND_NAME", "CLAIM_SEQUENCE_NBR", "CLAIM_STATUS", "CODE", "COUNT_DAYS_SUPPLY", "DAW_CODE", "DRUG_PRICE_TYPE_CODE",
            "DRUG_STRENGTH_DESC", "FILL_DATE", "FIRST_FILL_IND", "FORMULARY_IND", "FORMULARY_TYPE_CODE", "GENERIC_IND",
            "LSRD_NABP_NBR", "MAIL_ORDER_IND", "ORX_CLAIM_NBR", "ORX_INDIVIDUAL_ID", "PAID_DATE", "PHRM_NPI_NBR", "PRESCRIPTION_NBR", "PRSC_NPI_NBR",
            "QUANTITY_DRUG_UNITS", "REFILL_NBR", "SPECIALTY_PHMCY", "SPECIFIC_THERAPEUTIC_CLASS"), ORX_PHARMACY_CLAIM_PARTD.columns)

        if (result == false) {
            throw new Exception("ORX_PHARMACY_CLAIM_PARTD ==> FAILED COLUMN COMPARISON")
        }
        validateDateColumn("ORX_PHARMACY_CLAIM_PARTD", "FILL_DATE", ORX_PHARMACY_CLAIM_PARTD)
        validateDateColumn("ORX_PHARMACY_CLAIM_PARTD", "PAID_DATE", ORX_PHARMACY_CLAIM_PARTD)

        println("ORX_PHARMACY_CLAIM_PARTD FILL_DATE - DTEFILLED (ASC) ===>")
        ORX_PHARMACY_CLAIM.groupBy("FILL_DATE").count.orderBy(ORX_PHARMACY_CLAIM("FILL_DATE").asc).show()
        println("ORX_PHARMACY_CLAIM_PARTD FILL_DATE - DTEFILLED (ASC) ===>")
        ORX_PHARMACY_CLAIM.groupBy("FILL_DATE").count.orderBy(ORX_PHARMACY_CLAIM("FILL_DATE").asc).show()

        println("ORX_PHARMACY_CLAIM_PARTD PAID_DATE - ORGPDSBMDT (DESC) ===>")
        ORX_PHARMACY_CLAIM.groupBy("PAID_DATE").count.orderBy(ORX_PHARMACY_CLAIM("PAID_DATE").desc).show()
        println("ORX_PHARMACY_CLAIM_PARTD PAID_DATE - ORGPDSBMDT (DESC) ===>")
        ORX_PHARMACY_CLAIM.groupBy("PAID_DATE").count.orderBy(ORX_PHARMACY_CLAIM("PAID_DATE").desc).show()

        println(dateStr + " : ORX_MEMBER_COVERAGE ROWCOUNT :" + ORX_MEMBER_COVERAGE.count())
        //CrossixGenericUtil.summary(ORX_MEMBER_COVERAGE)
        println(dateStr + " : ORX_MEMBER_COVERAGE_PARTD :" + ORX_MEMBER_COVERAGE_PARTD.count())
        //CrossixGenericUtil.summary(ORX_MEMBER_COVERAGE_PARTD)
        println(dateStr + " : ORX_PHARMACY_CLAIM ROWCOUNT :" + ORX_PHARMACY_CLAIM.count())
        //CrossixGenericUtil.summary(ORX_PHARMACY_CLAIM)
        println(dateStr + " : ORX_PHARMACY_CLAIM_PARTD ROWCOUNT :" + ORX_PHARMACY_CLAIM_PARTD.count())
        //CrossixGenericUtil.summary(ORX_PHARMACY_CLAIM_PARTD)
        var ORX_MEMBER_PHI = sqlContext.read.parquet(home_dir+"/data/orx/weekly/" + dateStr + "/ORX_MEMBER_PHI")
        println(dateStr + " : ORX_MEMBER_PHI ROWCOUNT :" + ORX_MEMBER_PHI.count())
        //CrossixGenericUtil.summary(ORX_MEMBER_PHI)
    }


    def arrayEquals(src: Array[String], dest: Array[String]): Boolean = {
        var str1 = StringUtils.join(",", src)
        var str2 = StringUtils.join(",", dest)
        var flag = str1.equals(str2)
        if (flag == false)
            println("Comparing FAILED \nEXPETD :" + str1 + "\nACTUAL :" + str2)
        flag
    }

    def runOrxBuild(cfg: Map[String, String]) = {
        println("Creating... ORX_MEMBER_COVERAGE..")
        CrossixGenericUtil.build(new OrxmembercoverageOrxmembercoverage(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_MEMBER_COVERAGE")
        println("Creating... ORX_MEMBER_COVERAGE_PARTD..")
        CrossixGenericUtil.build(new OrxmembercoveragepartdOrxmembercoveragepartd(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_MEMBER_COVERAGE_PARTD")
        println("Creating... ORX_PHARMACY_CLAIM..")
        CrossixGenericUtil.build(new OrxpharmacyclaimOrxpharmacyclaim(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_PHARMACY_CLAIM")
        println("Creating... ORX_PHARMACY_CLAIM_PARTD..")
        CrossixGenericUtil.build(new OrxpharmacyclaimpartdOrxpharmacyclaimpartd(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_PHARMACY_CLAIM_PARTD")
        println("Creating... ORX_MEMBER_PHI..")
        CrossixGenericUtil.build(new OrxmemberphiOrxmemberphi(cfg)).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_MEMBER_PHI")
    }

    def convertParquet2CSV(sqlContext: SQLContext, dateStr: String, config: Map[String, String]): Unit = {
        parquet2csv(sqlContext, dateStr, "ORX_MEMBER_COVERAGE",config)
        parquet2csv(sqlContext, dateStr, "ORX_MEMBER_COVERAGE_PARTD",config)
        parquet2csv(sqlContext, dateStr, "ORX_PHARMACY_CLAIM",config)
        parquet2csv(sqlContext, dateStr, "ORX_PHARMACY_CLAIM_PARTD",config)
        parquet2csv(sqlContext, dateStr, "ORX_MEMBER_PHI",config)
    }


    def parquet2csv(sqlContext: SQLContext, dateStr: String, table: String, config: Map[String, String]): String = {
        var home_dir = config.get("HOME_DIR").get

        var src = home_dir +"/data/orx/weekly/" + dateStr + "/" + table
        var upload = home_dir + "/data/orx/weekly/" + dateStr + "/UPLOAD/" + table + "_" + dateStr + ".gz"
        var dest = home_dir + "/data/orx/weekly/" + dateStr + "/UPLOAD/" + table + "_OUTPUT"
        sqlContext.read.parquet(src).write.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\001").option("codec", "gzip").save(dest)
        var hdp = "hadoop fs -text " + dest + "/*.gz  | gzip | hadoop fs -put - " + upload
        println(hdp)
        hdp
    }


    def convertCSV2GZ(sqlContext: SQLContext, dateStr: String, sc: SparkContext, config: Map[String, String]): Unit = {
        csv2gz(sqlContext, dateStr, "ORX_MEMBER_COVERAGE", sc, config)
        csv2gz(sqlContext, dateStr, "ORX_MEMBER_COVERAGE_PARTD", sc,config)
        csv2gz(sqlContext, dateStr, "ORX_PHARMACY_CLAIM", sc,config)
        csv2gz(sqlContext, dateStr, "ORX_PHARMACY_CLAIM_PARTD", sc,config)
        csv2gz(sqlContext, dateStr, "ORX_MEMBER_PHI", sc,config)
    }

    def csv2gz(sqlContext: SQLContext, dateStr: String, table: String, sc: SparkContext, config: Map[String, String]) = {
        var home_dir = config.get("HOME_DIR").get

        var src = home_dir + "/data/orx/weekly/" + dateStr + "/" + table
        var upload = home_dir + "/data/orx/weekly/" + dateStr + "/UPLOAD/" + table + "_" + dateStr + ".gz"
        var c1 = sqlContext.read.parquet(src).count
        var c2 = sc.textFile(upload).count
        if (c1 == c2) {
            println("hadoop fs -copyToLocal " + upload)
            println(table + "_" + dateStr + ".gz" + " : " + c1)

        }
        else {
            println("ERROR " + dateStr + " : " + table)
        }
    }

/*
    def arrayEquals(src: Array[String], dest: Array[String]): Boolean = {
        var str1 = org.apache.hadoop.util.StringUtils.join(",", src)
        var str2 = org.apache.hadoop.util.StringUtils.join(",", dest)
        var flag = str1.equals(str2)
        if (flag == false)
            println("Comparing FAILED \nEXPETD :" + str1 + "\nACTUAL :" + str2)
        flag
    }
    */



    def parseClaim(sqlContext: SQLContext, sc:SparkContext)={
        val claimAvsc = new Schema.Parser().parse(new File("/users/rbabu/OptumrxClaim.avsc"))
        val claimSchema = SchemaConverters.toSqlType(claimAvsc).dataType.asInstanceOf[StructType]
        val claim = sc.textFile("/user/rbabu/data/orx/weekly/20180426_20180502/DATA/ORX_CLAIM/*").map(_.split("\\\u0001", -1).map(_.replaceAll("\\.$","")))
        val claimRowRDD = claim.map(p => Row(
            p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),
            p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17),p(18),p(19),
            p(20),p(21),p(22),p(23),p(24),p(25),p(26),p(27),p(28),p(29),
            p(30),p(31),p(32),p(33),p(34),p(35),p(36),p(37),p(38),p(39),
            p(40),p(41),p(42),p(43),p(44),p(45),p(46),p(47),p(48),p(49),
            p(50),p(51),p(52),p(53),p(54),p(55),p(56),p(57),p(58),p(59),
            p(60),p(61),p(62),p(63),p(64),p(65),p(66),p(67),p(68),p(69),
            p(70),p(71),p(72),p(73),p(74),p(75),p(76),p(77),p(78),p(79),
            p(80),p(81),p(82)
        ))

        val claimDF = sqlContext.createDataFrame(claimRowRDD, claimSchema)
        claimDF.write.parquet("/user/rbabu/data/orx/weekly/20180426_20180502/ORX_CLAIM")
    }



    def parseMBR_MCR_PARTD(sqlContext: SQLContext, sc:SparkContext)={
        val mbrAvsc = new Schema.Parser().parse(new File("/users/rbabu/OptumrxMbrMcrPartd.avsc"))
        val mbrSchema = SchemaConverters.toSqlType(mbrAvsc).dataType.asInstanceOf[StructType]
        val mbrMcrPartd = sc.textFile("/user/rbabu/data/orx/weekly/20180426_20180502/DATA/ORX_MBR_MCR_PARTD/*").map(_.split("\\\u0001", -1).map(_.replaceAll("\\.$","")))
        val mbrRDD = mbrMcrPartd.map(p => Row(
            p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9),
            p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19),
            p(20), p(21), p(22)
        ))

        val mbrDF = sqlContext.createDataFrame(mbrRDD, mbrSchema)
        mbrDF.write.parquet("/user/rbabu/data/orx/weekly/20180426_20180502/ORX_MBR_MCR_PARTD")
    }



    def parseMEMBER1(sqlContext: SQLContext, sc:SparkContext)={
        val memberAvsc = new Schema.Parser().parse(new File("/users/rbabu/OptumrxMember.avsc"))
        val memberSchema = SchemaConverters.toSqlType(memberAvsc).dataType.asInstanceOf[StructType]
        val member = sc.textFile("/user/rbabu/data/orx/weekly/20180426_20180502/DATA/ORX_MEMBER/*").map(_.split("\\\u0001", -1).map(_.replaceAll("\\.$","")))
        val memberRDD = member.map(p => Row(
            p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),
            p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17),p(18),p(19),
            p(20),p(21),p(22),p(23),p(24),p(25),p(26),p(27),p(28),p(29),
            p(30),p(31),p(32),p(33),p(34),p(35),p(36),p(37),p(38),p(39),
            p(40),p(41),p(42),p(43),p(44),p(45),p(46)
        ))

        val memberDF = sqlContext.createDataFrame(memberRDD, memberSchema)
        memberDF.write.parquet("/user/rbabu/data/orx/weekly/20180426_20180502/ORX_MEMBER")
    }



    def parseMEMBER_ELIGIBILITY1(sqlContext: SQLContext, sc:SparkContext)={
        val memEliAvsc = new Schema.Parser().parse(new File("/users/rbabu/OptumrxMemberEligibility.avsc"))
        val memEliSchema = SchemaConverters.toSqlType(memEliAvsc).dataType.asInstanceOf[StructType]
        val memEli = sc.textFile("/user/rbabu/data/orx/weekly/20180426_20180502/DATA/ORX_MEMBER_ELIGIBILITY/*").map(_.split("\\\u0001", -1).map(_.replaceAll("\\.$","")))
        val memEliRDD = memEli.map(p => Row(
            p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),
            p(10),p(11),p(12),p(13),p(14),p(15),p(16),p(17),p(18),p(19),
            p(20),p(21)
        ))

        val memberDF = sqlContext.createDataFrame(memEliRDD, memEliSchema)
        memberDF.write.parquet("/user/rbabu/data/orx/weekly/20180426_20180502/ORX_MEMBER_ELIGIBILITY")
    }


/*
    /user/rbabu/data/orx/weekly/20180426_20180502/ORX_MEMBER_COVERAGE
    /user/rbabu/data/orx/weekly/20180426_20180502/ORX_MEMBER_COVERAGE_PARTD
    /user/rbabu/data/orx/weekly/20180426_20180502/ORX_PHARMACY_CLAIM
    /user/rbabu/data/orx/weekly/20180426_20180502/ORX_PHARMACY_CLAIM_PARTD
    /user/rbabu/data/orx/weekly/20180426_20180502/ORX_MEMBER_PHI
    */
}