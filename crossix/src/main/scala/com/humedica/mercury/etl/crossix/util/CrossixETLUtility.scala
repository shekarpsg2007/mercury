package com.humedica.mercury.etl.crossix.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by rbabu on 7/26/17.
  */


object CrossixETLUtility {

    def parseORXCrossixDateStr(col: String, df: DataFrame) = {
        var value = trim(df(col))

        when(length(value) === 7, {
            var dt = value
            var firstPart = dt.substr(1, 1)
            var lastPart = dt.substr(2, 7)
            var dt1 = when(firstPart === "1", concat(lit("20"), lastPart)).otherwise(concat(lit("19"), lastPart))
            from_unixtime(unix_timestamp(dt1, "yyyyMMdd"), "yyyy-MM-dd")
        }).otherwise(
            {
                concat(lit("19"), from_unixtime(unix_timestamp(value, "yyMMdd"), "yy-MM-dd"))
            }
        )
    }



    def convertORXCrossixNDC_EFF_END_DateStrToDateObject(col: String, format:String, df: DataFrame) = {
        from_unixtime(unix_timestamp( trim(df(col)), format), "yyyy-MM-dd")
    }


    def parseORXCrossixDateStr(col: String, format:String, df: DataFrame) = {
            from_unixtime(unix_timestamp( trim(df(col)), format), "yyyy-MM-dd")
    }



    def parseORXCrossixDateObj(col: String, format:String, df: DataFrame) = {
        unix_timestamp( trim(df(col)), format)
    }


    def parseORXCrossixDateObj(col: String, df: DataFrame) = {
        var value = trim(df(col))
        when(length(value) === 7, {
            var dt = value
            var firstPart = dt.substr(1, 1)
            var lastPart = dt.substr(2, 7)
            var dt1 = when(firstPart === "1", concat(lit("20"), lastPart)).otherwise(concat(lit("19"), lastPart))
            unix_timestamp(dt1, "yyyyMMdd")
        }).otherwise(
            {
                var d = concat(lit("000000"), value)
                var len = length(d)
                unix_timestamp(d.substr(len, len-8), "yyyyMMdd")
            }
        )
    }


    def parseCrossixTime(col: String, df: DataFrame) = {
        var dt = concat(lit("000000"), trim(df(col)))
        var len = length(dt)
        dt = dt.substr( len - 6, len)
        from_unixtime(unix_timestamp(dt, "HHmmss"), "HH:mm:ss")
    }

    def parseORXCrossixStageCode(col: String, df: DataFrame) = {
        df.withColumn(col, when(upper(trim(df("ABAZTX"))) isin ("AL", "AK", "AZ", "AR", "CA", "CO", "CT",
                "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
                "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"), upper(df("ABAZTX"))).otherwise("UN"))
    }

    def parseORXCrossixYearOfBirth(col: String, df: DataFrame) = {
        var abajdt = trim(df("ABAJDT"))
        val yob = from_unixtime(unix_timestamp(abajdt, "yyyyMMdd"), "yyyy")
        val current_year = year(current_date())
        val yob1 = current_year.-(yob)

        df.withColumn(col, when(abajdt.isNull, "9999").otherwise(when(yob.lt(1860) || yob.gt(current_year), "0").otherwise(
            when(yob.geq(1860) && yob1.gt(89), current_year.-(90)).otherwise(yob)
        )))
    }

    //Concatenate the CARRIERID/ACCOUNTID/GROUPID (only positions 1-8)/MEMBERID.
    def createOrxIndividualId1(carrierid:String, accountid:String, groupid:String, memberid:String, df: DataFrame) = {
        concat(df(carrierid), df(accountid) ,df(groupid), df(memberid))
    }


    def parseORXGender(col: String, df: DataFrame) = {
        var gender = upper(trim(df("ABAAST")))
        df.withColumn(col, when(gender === "M" || gender === "F", gender).otherwise("U"))
    }
    def parseORXRefill(col: String, df: DataFrame) = {
        df.withColumn(col, when(trim(df("REFILL")) isin ("00", "01", "02", "03", "04", "05", "06", "07", "08", "09"), substring(df("REFILL"), 2, 1)).otherwise(df("REFILL")))
    }

    def parseORXCrossixDateStr1(col: String, format:String, df: DataFrame) = {
        var col1 = trim(df(col))
        when(col1 !== "00000000", from_unixtime(unix_timestamp( trim(df(col)), format), "yyyy-MM-dd"))
    }
}
