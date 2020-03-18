import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.humedica.mercury.etl.core.metrics.{Metrics, Toolkit}
import com.humedica.mercury.etl.core.engine.{Engine, EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._
import com.humedica.mercury.etl.core.engine.Types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import Metrics._
import Toolkit._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SQLContext
import java.util.ArrayList
import java.sql.DriverManager

import com.humedica.mercury.etl.crossix.asrxorder.RxordersandprescriptionsErx
import com.humedica.mercury.etl.crossix.epicrxorder.RxordersandprescriptionsMedorders
import com.humedica.mercury.etl.crossix.util._
//import com.humedica.mercury.etl.crossix.orxmembercoverage.OrxmembercoverageOrxmembercoverage
//import com.humedica.mercury.etl.crossix.orxmembercoveragepartd.OrxmembercoveragepartdOrxmembercoveragepartd
//import com.humedica.mercury.etl.crossix.orxmemberphi.OrxmemberphiOrxmemberphi
//import com.humedica.mercury.etl.crossix.orxpharmacyclaim.OrxpharmacyclaimOrxpharmacyclaim
//import com.humedica.mercury.etl.crossix.orxpharmacyclaimpartd.OrxpharmacyclaimpartdOrxpharmacyclaimpartd
//import com.humedica.mercury.etl.crossix.orxpharmacyclaim.{OrxpharmacyclaimOrxpharmacyclaim, OrxpharmacyclaimOrxpharmacyclaimPreAccID}
//import com.humedica.mercury.etl.crossix.orxpharmacyclaimpartd.{OrxpharmacyclaimpartdOrxpharmacyclaimpartd, OrxpharmacyclaimpartdOrxpharmacyclaimpartdPrevAccId}
//import com.humedica.mercury.etl.crossix.RxordersandprescriptionsErx
//import com.humedica.mercury.etl.crossix.epicrxorder.RxordersandprescriptionsMedorders
import java.util.HashMap
import java.util.HashSet
import com.humedica.mercury.etl.core.metrics.{Metrics, Toolkit}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.databricks.spark.avro.SchemaConverters
import java.io.File
import scala.collection.mutable.ListBuffer

var sqlContext = new SQLContext(sc)

spark.conf.set("spark.sql.caseSensitive", "false")
Toolkit.loadConfig()
Toolkit.setSession(spark)


import com.humedica.mercury.etl.crossix.orxmembercoverage.OrxmembercoverageOrxmembercoverage
import com.humedica.mercury.etl.crossix.orxmembercoveragepartd.OrxmembercoveragepartdOrxmembercoveragepartd
import com.humedica.mercury.etl.crossix.orxmemberphi.OrxmemberphiOrxmemberphi
import com.humedica.mercury.etl.crossix.orxpharmacyclaim.OrxpharmacyclaimOrxpharmacyclaim
import com.humedica.mercury.etl.crossix.orxpharmacyclaimpartd.OrxpharmacyclaimpartdOrxpharmacyclaimpartd

import com.humedica.mercury.etl.crossix.util._

import java.sql.{DriverManager, Statement, Connection}


def fetchAllSupportedGroupIds(cfg: Map[String, String]): String = {
    var groupIds = new ArrayList[String]()
    var connectstr = cfg.get("COMMON_CONNECTSTR").get
    var username = cfg.get("COMMON_USER").get
    var pwd = cfg.get("COMMON_PWD").get

    Class.forName("oracle.jdbc.driver.OracleDriver");
    var conn = DriverManager.getConnection(connectstr, username, pwd)
    var stmt = conn.createStatement()

    var qry = "SELECT DISTINCT(GROUPID) FROM COMMON.ALL_GROUPIDS"
    var rs = stmt.executeQuery(qry)
    while (rs.next()) {
        groupIds.add(rs.getString(1))
    }
    stmt.close()
    conn.close()
    "'" + String.join("','", groupIds) + "'"
}



def getAllGroupIdClientDsIdMap(conn:Connection):Map[String, String] = {
    var qry = "SELECT CDSC.CLIENT_ID, CDSC.CLIENT_DATA_SRC_ID FROM METADATA.CLIENT_DATA_SRC CDSC, METADATA.CLIENT_DATA_STREAM CDST WHERE CDSC.DCDR_EXCLUDE='N' AND CDSC.CLIENT_DATA_SRC_ID=CDST.CLIENT_DATA_STREAM_ID"
    var rs = conn.createStatement().executeQuery(qry)
    var map = Map[String, String]()
    while(rs.next()) {
        var groupId = rs.getString(1)
        var clientDataSrcId = rs.getString(2)
        if (groupId == "H340651" && clientDataSrcId == "1741") clientDataSrcId = "46"
        else if (groupId == "H262866" && clientDataSrcId == "1241") clientDataSrcId = "581"
        else if (groupId == "H416989" && clientDataSrcId == "962") clientDataSrcId = "221"
        else if (groupId == "H984442" && clientDataSrcId == "961") clientDataSrcId = "421"
        else if (groupId == "H667594" && clientDataSrcId == "963") clientDataSrcId = "57"
        map += (groupId -> clientDataSrcId)
    }
    println(map)
    map
}


def writeAllFiles(sQLContext: SQLContext,module:String, table:String, cfg: Map[String, String]) = {

    println("Preparing dictionary tables "+table)
    var src = cfg.get("STAGING_BASEDIR").get
    var home_dir = "/user/rbabu/data/rxorder/DICTIONARIES_HISTORY/RAW"
    var fs = FileSystem.get(new Configuration())

    var files = fs.listStatus(new Path(src))
    files.foreach(f=> {
        var path1:Path = f.getPath
        if(path1.toString.contains(module)) {
            var path = new Path(path1, table + "/*")
            var dest = home_dir+"/"+table
            println("Copying " + path.toString +" to "+dest)
            var s = sQLContext.read.parquet(path.toString)
            s.write.mode("append").parquet(dest)
        }
    })
}



def writeAllDataDictionaries(sQLContext: SQLContext,cfg: Map[String, String]) = {
    writeAllFiles(sqlContext, "_EP2_", "ZH_STATE",  cfg)
    writeAllFiles(sqlContext, "_EP2_", "ZH_CLARITYDRG", cfg)
    writeAllFiles(sqlContext, "_EP2_", "ZH_CLARITYDEPT",  cfg)
    writeAllFiles(sqlContext,  "_EP2_", "ZH_BENEPLAN",  cfg)
    writeAllFiles(sqlContext, "_EP2_", "ZH_MED_UNIT",  cfg)
    writeAllFiles(sqlContext, "_CR2_", "ZH_CODE_VALUE", cfg)
    writeAllFiles(sqlContext,  "_CR2_", "ZH_MED_IDENTIFIER",  cfg)
    writeAllFiles(sqlContext, "_CR2_", "ZH_NOMENCLATURE",   cfg)
    writeAllFiles(sqlContext,  "_CR2_", "ZH_ORDER_CATALOG_ITEM_R", cfg)
}



def fetchStagedFiles(conn:Connection, sqlContext: SQLContext, module: String, table: String, dateStr: String, supportedGroupIds: String, cfg: Map[String, String], targetDir:String, groupIdClientDSIDMap:Map[String, String]): Unit = {

    var fs = FileSystem.get(new Configuration());


    var stmt = conn.createStatement()
    var dateStr1 = "'" + dateStr.replaceAll(",", "','") + "'"

    var qry = "SELECT FILE_ID, TABLE_NAME, GROUPID,FILE_STAGED, DATA_STREAM_ID, SCHEMA  FROM FILE_STAGED_MESSAGES WHERE UPPER(SCHEMA) LIKE '%" + module + "%' AND UPPER(TABLE_NAME) = '" + table + "' " +
            "AND SUBSTR(TO_CHAR(TIME_STAGED, 'YYYYMMDD'),1,9) IN (" + dateStr1 + ") AND GROUPID IN (" + supportedGroupIds + ") AND SYSTEM_NAME='PROD_PROD'"
    println("Running query :" + qry)
    var rs = stmt.executeQuery(qry)

    // var targetDir = "/user/rbabu/data/tmp/" + dt

    var path = new Path(targetDir)
    if (!fs.exists(path)) {
        println("Creating dir " + targetDir)
        fs.mkdirs(path)
    }
    var dictTables = "AS_ZC_MEDICATION_DE,MEDADMINREC,ADDRESS,ZH_NOMENCLATURE,ZH_MED_IDENTIFIER,ZH_CODE_VALUE,ZH_MED_UNIT,ZH_BENEPLAN,ZH_CLARITYDRG,ZH_CLARITYDEPT,ZH_STATE,ZH_ORDER_CATALOG_ITEM_R"

    while (rs.next()) {
        var fileid = rs.getString(1)
        var tableName = rs.getString(2)
        var groupId = rs.getString(3)
        var fileStaged = rs.getString(4)

        var clientDsId = if(groupIdClientDSIDMap != null && groupIdClientDSIDMap.contains(groupId)) groupIdClientDSIDMap(groupId) else rs.getString(5)
        var tmpTarget = targetDir
        var schema = rs.getString(6)
        if(dictTables.contains(tableName)) {
            tmpTarget = cfg("DICT_DATA_ROOT")
        }

        var newTargetDir = new Path(tmpTarget + "/" + tableName + "/GROUPID=" + groupId + "/CLIENT_DS_ID=" + clientDsId)
        if (!fs.exists(newTargetDir)) {
            println("Creating dir " + newTargetDir)
            fs.mkdirs(newTargetDir)
        }
        println("Writing  " + fileStaged + " to " + newTargetDir.toString)
        sqlContext.read.parquet(fileStaged).write.mode("append").parquet(newTargetDir.toString)
    }

    stmt.close()
}


//fetchOptumRxStagedFiles(sqlContext, "20180511", cfg)

def fetchOptumRxStagedFiles(sqlContext: SQLContext, dateStr: String, cfg: Map[String, String]): Unit = {

    var connectstr = cfg.get("FASTTRACT_CONNECTSTR").get
    var username = cfg.get("FASTTRACT_USER").get
    var pwd = cfg.get("FASTTRACT_PWD").get

    Class.forName("oracle.jdbc.driver.OracleDriver");
    var conn = DriverManager.getConnection(connectstr, username, pwd)

    var dt1 = dateStr.split(",")
    var dt = dt1(0) + "_" + dt1(dt1.length - 1)

    var targetDir = cfg.get("OPTUMRX_TARGET_BASE_DIR").get + dt

    fetchStagedFiles(conn, sqlContext, "_OPTUMRX_", "ORX_MEMBER", dateStr: String, "'H001001'", cfg, targetDir, null)
    fetchStagedFiles(conn, sqlContext, "_OPTUMRX_", "ORX_CLAIM", dateStr: String, "'H001001'", cfg, targetDir, null)
    fetchStagedFiles(conn, sqlContext, "_OPTUMRX_", "ORX_MEMBER_ELIGIBILITY", dateStr: String, "'H001001'", cfg, targetDir, null)
    fetchStagedFiles(conn, sqlContext, "_OPTUMRX_", "ORX_MBR_MCR_PARTD", dateStr: String, "'H001001'", cfg, targetDir, null)

    var mbrMcrPartdBaseDir = cfg.get("MBR_MCR_PARTD_HISTORY_BASE_DIR").get
    var memEligBaseDir = cfg.get("MEMBER_ELIGIBILITY_HISTORY_BASE_DIR").get
    sqlContext.read.parquet(targetDir + "/ORX_MBR_MCR_PARTD/*").write.mode("append").parquet(mbrMcrPartdBaseDir)
    sqlContext.read.parquet(targetDir + "/ORX_MEMBER_ELIGIBILITY/*").write.mode("append").parquet(memEligBaseDir)

    sqlContext.read.parquet(memEligBaseDir)
            .select("FLCPDA", "FLCQDA", "FLAACD", "FLACCD", "FLADCD", "FLABCD")
            .distinct().write.parquet(targetDir + "/MEMBER_ELIGIBILITY_HISTORY")

    sqlContext.read.parquet(mbrMcrPartdBaseDir)
            .select("A0OCHG", "A0ODHG", "A0AACD", "A0ACCD", "A0ADCD", "A0ABCD")
            .distinct().write.parquet(targetDir + "/MBR_MCR_PARTD_HISTORY")


    println("COPYING " + cfg.get("NDC_WEEKLY_MASTER").get + " To " + targetDir + "/NDC")
    sqlContext.read.parquet(cfg.get("NDC_WEEKLY_MASTER").get)
            .select("AHFS_THERAPEUTIC_CLASS", "BRAND_NAME", "DRUG_PRICE_TYPE_CODE", "DRUG_STRENGTH_DESC", "GENERIC", "SPECIFIC_THERAPEUTIC_CLASS", "CODE", "EFF_DATE", "END_DATE")
            .distinct().write.parquet(targetDir + "/NDC")

    println("COPYING " + cfg.get("PHARMACY_WEEKLY_MASTER").get + " To " + targetDir + "/PHARMACY")
    sqlContext.read.parquet(cfg.get("PHARMACY_WEEKLY_MASTER").get)
            .select("LSRD_NABP_NBR", "DISPENSE_TYPE_CODE", "NC_SPECIALTY_PHMCY", "NABP_NBR")
            .distinct().write.parquet(targetDir + "/PHARMACY")

    println("COPYING " + cfg.get("MEMBERPHI_WEEKLY_MASTER").get + " To " + targetDir + "/MEMBER_PHI")
    sqlContext.read.parquet(cfg.get("MEMBERPHI_WEEKLY_MASTER").get)
            .select("LSRD_INDIVIDUAL_ID", "SOURCE_MEMBER_ID")
            .distinct().write.parquet(targetDir + "/MEMBER_PHI")

}




def transformOptumRxFiles(cfg: Map[String, String]) = {
    println("Creating... ORX_MEMBER_COVERAGE..")
    Toolkit.build(new OrxmembercoverageOrxmembercoverage(cfg), allColumns = true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_MEMBER_COVERAGE")
    println("Creating... ORX_MEMBER_COVERAGE_PARTD..")
    Toolkit.build(new OrxmembercoveragepartdOrxmembercoveragepartd(cfg), allColumns = true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_MEMBER_COVERAGE_PARTD")
    println("Creating... ORX_PHARMACY_CLAIM..")
    Toolkit.build(new OrxpharmacyclaimOrxpharmacyclaim(cfg), allColumns = true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_PHARMACY_CLAIM")
    println("Creating... ORX_PHARMACY_CLAIM_PARTD..")
    Toolkit.build(new OrxpharmacyclaimpartdOrxpharmacyclaimpartd(cfg), allColumns = true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_PHARMACY_CLAIM_PARTD")
    println("Creating... ORX_MEMBER_PHI..")
    Toolkit.build(new OrxmemberphiOrxmemberphi(cfg), allColumns = true).distinct.write.parquet(cfg.get("EMR_DATA_ROOT").get + "/ORX_MEMBER_PHI")
}



def fetchRxOrderStagedFiles(sqlContext: SQLContext, dateStr: String, cfg: Map[String, String]): Unit = {

    var connectstr = cfg.get("FASTTRACT_CONNECTSTR").get
    var username = cfg.get("FASTTRACT_USER").get
    var pwd = cfg.get("FASTTRACT_PWD").get

    Class.forName("oracle.jdbc.driver.OracleDriver");
    var conn = DriverManager.getConnection(connectstr, username, pwd)


    var groupIdClientDSIDMap:Map[String, String] = getAllGroupIdClientDsIdMap(conn)


    var dt1 = dateStr.split(",")
    var dt = dt1(0) + "_" + dt1(dt1.length - 1)

    var targetDir = cfg.get("RXORDER_TARGET_BASE_DIR").get + dt
    var supportedGroupIds = fetchAllSupportedGroupIds(cfg)

    var asTables = "AS_ERX,AS_PATDEM,AS_ZC_MEDICATION_DE"
    asTables.split(",").foreach(tbl=> {
        fetchStagedFiles(conn, sqlContext, "_AS_", tbl, dateStr: String, supportedGroupIds, cfg, targetDir, groupIdClientDSIDMap)
    })

    var epicTables = "PATREG,MEDORDERS,PATIENT_FYI_FLAGS,PAT_RACE,PATIENT_3,IDENTITY_ID,MEDADMINREC"
    epicTables.split(",").foreach(tbl=> {
        fetchStagedFiles(conn, sqlContext, "_EP2_", tbl, dateStr: String, supportedGroupIds, cfg, targetDir, groupIdClientDSIDMap)
    })

    var cr2Tables = "ORDERS,ENCOUNTER,ENC_ALIAS,HUM_DRG,ORDER_ACTION,ORDER_DETAIL,PERSON,PERSON_ALIAS,ADDRESS"
    cr2Tables.split(",").foreach(tbl=> {
        fetchStagedFiles(conn, sqlContext, "_CR2_", tbl, dateStr: String, supportedGroupIds, cfg, targetDir, groupIdClientDSIDMap)
    })


}


def predicate_value_list(p_mpv: DataFrame, dataSrc: String, entity: String, table: String, column: String, colName: String): DataFrame = {
    var mpv1 = p_mpv.filter(p_mpv("DATA_SRC").equalTo(dataSrc).and(p_mpv("ENTITY").equalTo(entity)).and(p_mpv("TABLE_NAME").equalTo(table)).and(p_mpv("COLUMN_NAME").equalTo(column)))
    mpv1=mpv1.withColumn(colName, mpv1("COLUMN_VALUE"))
    mpv1.select("GROUPID", "CLIENT_DS_ID", colName).orderBy(mpv1("DTS_VERSION").desc).distinct()
}





def readTable(sqlContext:SQLContext, dateStr:String, table1:String, cfg: Map[String, String]): DataFrame = {
    var home_dir= cfg.get("EMR_DATA_ROOT").get
    var table = table1
    //var path = cfg.get("EMR_DATA_ROOT").get + "/" + table
    var path = home_dir + "/"+table
    println("Trying to read table " + path)
    sqlContext.read.parquet(path)
}

def readDictionaryTable(sqlContext:SQLContext, table1:String, cfg: Map[String, String]): DataFrame = {
    var table = table1+"/*"
    var path = cfg.get("DICT_DATA_ROOT").get +"/"+ table
    println("Trying to read table "+path)
    sqlContext.read.parquet(path)
}


def readData(sqlContext:SQLContext, table:String, cfg: Map[String, String]): DataFrame = {
    var path = cfg.get("CDR_DATA_ROOT").get +"/"+table
    println("Trying to read data "+path)
    var c = sqlContext.read.parquet(path)
    println(c.count())
    c
}


def readCommonData(sqlContext:SQLContext, table:String, cfg: Map[String, String]): DataFrame = {
    var path = cfg.get("COMMON_DATA_ROOT").get +"/"+table+"*"
    println("Trying to read common data "+path)
    var c = sqlContext.read.parquet(path)
    println(c.count())
    c
}


def readGenerated(sqlContext:SQLContext, dateStr:String, table:String, cfg: Map[String, String]): DataFrame = {
    var home_dir= cfg.get("HOME_DIR").get

    var path = cfg.get("EMR_DATA_ROOT").get+"/GENERATED/"+table
    //var path = home_dir + "/data/rxorder/weekly/"+dateStr+"/GENERATED/"+table

    println("Trying to read generated files "+path)
    sqlContext.read.parquet(path)
}

def writeGenerated(df:DataFrame, dateStr:String, table:String, cfg: Map[String, String]): String = {
    var home_dir= cfg.get("HOME_DIR").get

    //var path = home_dir + "/data/rxorder/weekly/"+dateStr+"/GENERATED/"+table
    var path = cfg.get("EMR_DATA_ROOT").get+"/GENERATED/"+table
    println("Trying to write generated files "+ path)
    df.distinct.write.parquet(path)
    path
}


def appendGenerated(df:DataFrame, dateStr:String, table:String, cfg: Map[String, String]): Unit = {
    var home_dir= cfg.get("HOME_DIR").get

    var path = cfg.get("EMR_DATA_ROOT").get+"/GENERATED/"+table
    //var path = home_dir + "/data/rxorder/weekly/"+dateStr+"/GENERATED/"+table

    println("Trying to write append files "+ table)
    df.write.mode("append").parquet(path)
}




def validateSSN(df:DataFrame, col:String) :Column = {
    {
        try {
            var SSN = trim(df(col))
            when (SSN.isNull, null)
            when (length(SSN) < 11, "N")

            val first = substring(SSN, 0, 3)
            val second = substring(SSN, 4, 6)
            val third = substring(SSN, 7, 11)
            when ((substring(SSN, 0, 1) === "9") || first === "000" || first === "666" || second === "00" || third === "0000", "DIGT").otherwise("Y")
        }
        catch {
            case e: Exception => {
                return lit("N")
            }
        }
    }
}

def STANDARDIZE_POSTAL_CODE(colm : Column) = {
    var count: Int = 5
    regexp_replace(when (length(colm) < 5, colm).otherwise(trim(colm).substr(0, count)), "O", "0")
}




def tempgrp(FILTER_PATDEM1:DataFrame, LOCALVALUE:Column, PATIENTDETAILTYPE:String) = {
    var PATIENT_DETAIL_ZIP = FILTER_PATDEM1.withColumn("FACILITYID", lit("NULL")).withColumn("ENCOUNTERID", lit("NULL"))
            .withColumn("PATIENTID", FILTER_PATDEM1("PATIENT_MRN")).withColumn("DATEOFBIRTH", FILTER_PATDEM1("PATIENT_DATE_OF_BIRTH"))
            .withColumn("LOCALVALUE",LOCALVALUE).withColumn("PATIENTDETAILTYPE", lit(PATIENTDETAILTYPE))
            .withColumn("DATASRC", lit("patdem")).withColumnRenamed("LAST_UPDATED_DATE", "PATDETAIL_TIMESTAMP")
    PATIENT_DETAIL_ZIP = PATIENT_DETAIL_ZIP.select("GROUPID_pat", "CLIENT_DS_ID_pat", "FACILITYID", "ENCOUNTERID", "PATIENTID", "DATEOFBIRTH","LOCALVALUE",
        "PATIENTDETAILTYPE", "PATDETAIL_TIMESTAMP", "DATASRC")
    val group1 = Window.partitionBy(PATIENT_DETAIL_ZIP("PATIENTID"), PATIENT_DETAIL_ZIP("LOCALVALUE"), PATIENT_DETAIL_ZIP("GROUPID_pat"), PATIENT_DETAIL_ZIP("CLIENT_DS_ID_pat")).orderBy(PATIENT_DETAIL_ZIP("PATDETAIL_TIMESTAMP").desc)
    var TEMP_PATIENT_3 = PATIENT_DETAIL_ZIP.withColumn("ROW_NUMBER", row_number().over(group1))
    TEMP_PATIENT_3 = TEMP_PATIENT_3.filter((TEMP_PATIENT_3("ROW_NUMBER")===1).and(TEMP_PATIENT_3("LOCALVALUE").isNotNull))
    TEMP_PATIENT_3 = TEMP_PATIENT_3.withColumn("PATIENTDETAILQUAL", lit("NULL")).withColumn("HGPID", lit("NULL")).withColumn("GRP_MPI", lit("NULL"))
    TEMP_PATIENT_3.select("GROUPID_pat", "CLIENT_DS_ID_pat", "DATASRC", "FACILITYID", "LOCALVALUE", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATDETAIL_TIMESTAMP","DATEOFBIRTH")
}





def processASTempPatientDetails(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {
    var PATDEM = readTable(sqlContext, dateStr,"AS_PATDEM", cfg)
    PATDEM = PATDEM.withColumn("FILE_ID", PATDEM("FILEID").cast("String"))
    PATDEM = PATDEM.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_pat").withColumnRenamed("GROUPID", "GROUPID_pat")
    println("PROCESSING PATDEM "+PATDEM.count() +" rows...")
    var PATIENT_MPI1 = readData(sqlContext, "PATIENT_MPI", cfg).select("GROUPID","CLIENT_DS_ID","PATIENTID","HGPID","GRP_MPI").distinct()

    var FILTER_PATDEM = PATDEM.filter((PATDEM("PATIENT_LAST_NAME").rlike(".*%.*") and PATDEM("PATIENT_FIRST_NAME").isNull) or
            (PATDEM("PATIENT_LAST_NAME").rlike("^TEST |TEST$|ZZTEST") or PATDEM("PATIENT_FIRST_NAME").rlike("TEST")
                    or upper(PATDEM("PATIENT_FIRST_NAME")) === "TEST")===false)
    var TEMP_PATIENT1 = FILTER_PATDEM.withColumn("DATASRC", lit("patdem")).withColumn("PATIENTID", FILTER_PATDEM("PATIENT_MRN")).withColumn("MEDICALRECORDNUMBER", FILTER_PATDEM("PATIENT_MRN") )
            .withColumnRenamed("LAST_UPDATED_DATE","UPDATE_TS")
    TEMP_PATIENT1 = TEMP_PATIENT1.select("GROUPID_pat","CLIENT_DS_ID_pat","DATASRC", "PATIENTID", "MEDICALRECORDNUMBER", "PATIENT_DATE_OF_BIRTH", "UPDATE_TS", "INACTIVE_FLAG").distinct()

    val group1 = Window.partitionBy(TEMP_PATIENT1("PATIENTID"),TEMP_PATIENT1("GROUPID_pat"),TEMP_PATIENT1("CLIENT_DS_ID_pat")).orderBy(TEMP_PATIENT1("UPDATE_TS").desc)
    var TEMP_PATIENT_3 = TEMP_PATIENT1.withColumn("ROWNMBR", row_number().over(group1)).withColumn("FACILITYID", lit("NULL"))
    var TEMP_PATIENT = TEMP_PATIENT_3.select("GROUPID_pat","CLIENT_DS_ID_pat","DATASRC", "PATIENTID", "FACILITYID", "PATIENT_DATE_OF_BIRTH", "MEDICALRECORDNUMBER", "INACTIVE_FLAG", "ROWNMBR").distinct()
    var TEMP_PAT_FINAL = TEMP_PATIENT.filter(TEMP_PATIENT("ROWNMBR")===1).withColumnRenamed("PATIENTID", "PATIENTID_pat")
    var TEMP_PAT_JOIN = TEMP_PAT_FINAL.join(PATIENT_MPI1,
        TEMP_PAT_FINAL("GROUPID_pat")===PATIENT_MPI1("GROUPID") && TEMP_PAT_FINAL("CLIENT_DS_ID_pat")===PATIENT_MPI1("CLIENT_DS_ID") && TEMP_PAT_FINAL("PATIENTID_pat")===PATIENT_MPI1("PATIENTID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID").drop("PATIENTID")
    TEMP_PAT_JOIN=TEMP_PAT_JOIN.withColumn("DATEOFDEATH", lit("NULL"))
    var TEMP_PAT_JOIN_OUT = TEMP_PAT_JOIN.select("GROUPID_pat", "DATASRC", "PATIENTID_pat", "FACILITYID","PATIENT_DATE_OF_BIRTH","MEDICALRECORDNUMBER","HGPID", "DATEOFDEATH", "CLIENT_DS_ID_pat", "GRP_MPI" , "INACTIVE_FLAG")
    var PATIENT = TEMP_PAT_JOIN_OUT.filter(TEMP_PAT_JOIN_OUT("GRP_MPI").isNotNull).withColumnRenamed("PATIENTID_pat", "PATIENTID").withColumnRenamed("GROUPID_pat", "GROUPID").withColumnRenamed("CLIENT_DS_ID_pat", "CLIENT_DS_ID")
            .withColumnRenamed("PATIENT_DATE_OF_BIRTH", "DATEOFBIRTH")
    PATIENT = PATIENT.withColumn("FACILITYID", when(PATIENT("FACILITYID") !== "NULL",PATIENT("FACILITYID")))
    PATIENT = PATIENT.select("GROUPID", "DATASRC", "PATIENTID", "FACILITYID","DATEOFBIRTH","MEDICALRECORDNUMBER","HGPID","DATEOFDEATH","CLIENT_DS_ID", "GRP_MPI","INACTIVE_FLAG")
    writeGenerated(PATIENT, dateStr,"AS_TEMP_PATIENT",cfg)


    var FILTER_PATDEM1 = PATDEM.filter(( (PATDEM("PATIENT_LAST_NAME").rlike(".*%.*") and PATDEM("PATIENT_FIRST_NAME").isNull) or
            (PATDEM("PATIENT_LAST_NAME").rlike("^TEST |TEST$|ZZTEST") or PATDEM("PATIENT_FIRST_NAME").rlike("TEST")
                    or upper(PATDEM("PATIENT_FIRST_NAME")) === "TEST")===false) and length(PATDEM("PATIENT_MRN")) > 2)
    FILTER_PATDEM1 = FILTER_PATDEM1.withColumn("COLUMN_VALUE", concat_ws("",FILTER_PATDEM1("PATIENT_SSN"), FILTER_PATDEM1("PATIENT_NOTE2")))

    var PATMPI = PATIENT_MPI1.select("PATIENTID","CLIENT_DS_ID", "GROUPID").withColumnRenamed("CLIENT_DS_ID", "MPI_CLIENT_DS_ID").withColumnRenamed("GROUPID", "MPI_GROUPID")
    FILTER_PATDEM1 = FILTER_PATDEM1.join(PATMPI, FILTER_PATDEM1("PATIENT_MRN")===PATMPI("PATIENTID") and FILTER_PATDEM1("CLIENT_DS_ID_pat")===PATMPI("MPI_CLIENT_DS_ID") and FILTER_PATDEM1("GROUPID_pat")===PATMPI("MPI_GROUPID"), "left_outer")

    var TEMP_PAT_DET_FST_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_FIRST_NAME"), "FIRST_NAME")
    var TEMP_PAT_DET_LST_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_LAST_NAME"), "LAST_NAME")
    var TEMP_PAT_DET_CITY = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_CITY"), "CITY")
    var TEMP_PAT_DET_STATE = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_STATE"), "STATE")
    var TEMP_PAT_DET_ZIP = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_ZIP"), "ZIPCODE")
    var TEMP_PAT_DET_GENDER = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("GENDER_ID"), "GENDER")

    var TEMP_PAT_DET_RACE = tempgrp(FILTER_PATDEM1, concat_ws("", FILTER_PATDEM1("CLIENT_DS_ID_pat"), FILTER_PATDEM1("RACE_ID")), "RACE")

    var ETH_COL = when(FILTER_PATDEM1("ETHNICITY").isNotNull.and(FILTER_PATDEM1("ETHNICITYDE") !== 0), concat_ws("",FILTER_PATDEM1("CLIENT_DS_ID_pat"), FILTER_PATDEM1("ETHNICITYDE")))
            .when(FILTER_PATDEM1("RACE_ID").isNotNull.and(FILTER_PATDEM1("RACE_ID") !== "0"), concat_ws("",FILTER_PATDEM1("CLIENT_DS_ID_pat"), FILTER_PATDEM1("RACE_ID"))).otherwise("NULL")
    var TEMP_PAT_DET_ETHNICITY = tempgrp(FILTER_PATDEM1, ETH_COL, "ETHNICITY")
    var TEMP_PAT_DET_DECEASED = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("DEATH_FLAG_INDICATOR"), "DECEASED")
    var TEMP_PAT_DET_LANGUAGE = tempgrp(FILTER_PATDEM1, concat_ws("", FILTER_PATDEM1("CLIENT_DS_ID_pat"),FILTER_PATDEM1("PRIMARY_LANGUAGE_ID")), "LANGUAGE")
    var TEMP_PAT_DET_MID_NAME = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("PATIENT_MIDDLE_NAME"), "MIDDLE_NAME")
    var TEMP_PAT_DET_MARITAL = tempgrp(FILTER_PATDEM1, FILTER_PATDEM1("MARITAL_STATUS_ID"), "MARITAL")
    //FILTER_PATDEM1 = FILTER_PATDEM1.join(PATIENT_MPI1, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")
    FILTER_PATDEM1 = FILTER_PATDEM1.withColumnRenamed("PATIENTID", "PATIENTID_pat")
    FILTER_PATDEM1 = FILTER_PATDEM1.join(PATIENT_MPI1,FILTER_PATDEM1("GROUPID_pat")===PATIENT_MPI1("GROUPID") && FILTER_PATDEM1("CLIENT_DS_ID_pat")===PATIENT_MPI1("CLIENT_DS_ID") && FILTER_PATDEM1("PATIENTID_pat")===PATIENT_MPI1("PATIENTID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID").drop("PATIENTID")

    var TEMP_PATIENT_ID1 = FILTER_PATDEM1.withColumn("DATASRC", lit("patdem")).withColumn("PATIENTID", FILTER_PATDEM1("PATIENT_MRN")).withColumn("IDTYPE", lit("PATIENT_SSN")).withColumnRenamed("COLUMN_VALUE", "SSN").withColumn("ID_SUBTYPE", lit("NULL"))

    var group2 = Window.partitionBy(TEMP_PATIENT_ID1("PATIENTID"),TEMP_PATIENT_ID1("GROUPID_pat"),TEMP_PATIENT_ID1("CLIENT_DS_ID_pat")).orderBy(TEMP_PATIENT_ID1("LAST_UPDATED_DATE").desc)
    var FIL_PATIENT_ID_SSN = TEMP_PATIENT_ID1.withColumn("ROW_NUMBER", row_number().over(group2))
    FIL_PATIENT_ID_SSN = FIL_PATIENT_ID_SSN.filter((FIL_PATIENT_ID_SSN("ROW_NUMBER")===1).and(FIL_PATIENT_ID_SSN("SSN").isNotNull))


    var TEMP_PATIENT_ID =FIL_PATIENT_ID_SSN.withColumn("IDVALUE", lit("NULL")).withColumn("FACILITYID", lit("NULL"))
    TEMP_PATIENT_ID = TEMP_PATIENT_ID.select("GROUPID_pat", "PATIENTID_pat", "DATASRC", "IDTYPE", "IDVALUE", "CLIENT_DS_ID_pat", "HGPID", "GRP_MPI", "ID_SUBTYPE", "FACILITYID").distinct()
    PATIENT_MPI1 = PATIENT_MPI1.withColumnRenamed("HGPID","HGPID_pmi").withColumnRenamed("GRP_MPI","GRP_MPI_pmi")
    var TEMP_PATIENT_ID_JOIN = TEMP_PATIENT_ID.join(PATIENT_MPI1,  TEMP_PATIENT_ID("GROUPID_pat")===PATIENT_MPI1("GROUPID") && TEMP_PATIENT_ID("CLIENT_DS_ID_pat")===PATIENT_MPI1("CLIENT_DS_ID") && TEMP_PATIENT_ID("PATIENTID_pat")===PATIENT_MPI1("PATIENTID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID").drop("PATIENTID")
    var TEMP_PATIENT_ID_2 = TEMP_PATIENT_ID_JOIN.select("GROUPID_pat","PATIENTID_pat","DATASRC", "IDTYPE", "IDVALUE", "CLIENT_DS_ID_pat", "HGPID", "GRP_MPI", "ID_SUBTYPE").distinct()
    TEMP_PATIENT_ID_2 = TEMP_PATIENT_ID_2.withColumnRenamed("GROUPID_pat", "GROUPID").withColumnRenamed("PATIENTID_pat", "PATIENTID").withColumnRenamed("CLIENT_DS_ID_pat", "CLIENT_DS_ID")
    TEMP_PATIENT_ID_2 = TEMP_PATIENT_ID_2.select("GROUPID","PATIENTID", "DATASRC", "IDTYPE", "IDVALUE","CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE");

    writeGenerated(TEMP_PATIENT_ID_2,dateStr, "AS_TEMP_PATIENT_ID",cfg)

    var TEMP_PATIENTDETAIL = TEMP_PAT_DET_MARITAL.unionAll(TEMP_PAT_DET_MID_NAME).distinct().unionAll(TEMP_PAT_DET_LANGUAGE).distinct().unionAll(TEMP_PAT_DET_DECEASED).distinct()
            .unionAll(TEMP_PAT_DET_ETHNICITY).distinct().unionAll(TEMP_PAT_DET_RACE).distinct().unionAll(TEMP_PAT_DET_GENDER).distinct()
            .unionAll(TEMP_PAT_DET_ZIP).distinct().unionAll(TEMP_PAT_DET_STATE).distinct().unionAll(TEMP_PAT_DET_CITY).distinct()
            .unionAll(TEMP_PAT_DET_LST_NAME).distinct().unionAll(TEMP_PAT_DET_FST_NAME).distinct()
    TEMP_PATIENTDETAIL = TEMP_PATIENTDETAIL.withColumnRenamed("PATIENTID", "PATIENTID_pat")
    var TEMP_PATDETAIL_JOIN = TEMP_PATIENTDETAIL.join(PATIENT_MPI1,  TEMP_PATIENTDETAIL("GROUPID_pat")===PATIENT_MPI1("GROUPID") && TEMP_PATIENTDETAIL("CLIENT_DS_ID_pat")===PATIENT_MPI1("CLIENT_DS_ID") && TEMP_PATIENTDETAIL("PATIENTID_pat")===PATIENT_MPI1("PATIENTID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID").drop("PATIENTID")

    var TEMP_PATDETAIL = TEMP_PATDETAIL_JOIN.withColumn("PATIENTDETAILQUAL", lit("NULL"))
    TEMP_PATDETAIL = TEMP_PATDETAIL.select("GROUPID_pat", "DATASRC", "FACILITYID", "PATIENTID_pat", "ENCOUNTERID", "PATIENTDETAILTYPE",
        "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID_pat", "HGPID_pmi", "GRP_MPI_pmi", "DATEOFBIRTH").withColumn("FACILITYID", when(TEMP_PATDETAIL("FACILITYID") !== "NULL",TEMP_PATDETAIL("FACILITYID"))).withColumn("DATEOFBIRTH", when(TEMP_PATDETAIL("DATEOFBIRTH") !== "NULL",TEMP_PATDETAIL("DATEOFBIRTH")))
    TEMP_PATDETAIL = TEMP_PATDETAIL.withColumnRenamed("HGPID_pmi","HGPID").withColumnRenamed("GRP_MPI_pmi","GRP_MPI").withColumnRenamed("GROUPID_pat","GROUPID").withColumnRenamed("PATIENTID_pat","PATIENTID").withColumnRenamed("CLIENT_DS_ID_pat","CLIENT_DS_ID")
    TEMP_PATDETAIL = TEMP_PATDETAIL.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")

    writeGenerated(TEMP_PATDETAIL,dateStr, "AS_TEMP_PATIENT_DETAILS", cfg)
}


def processEPTempPatientDetails(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]): Unit = {
    var PATREG = readTable(sqlContext, dateStr, "PATREG",cfg)
    PATREG = PATREG.withColumnRenamed("GROUPID", "GROUPID_pat").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_pat")

    var PA = PATREG.withColumnRenamed("UPDATE_DATE", "PA_UPDATE_DATE").select( "GROUPID_pat","CLIENT_DS_ID_pat", "PAT_ID", "PAT_MRN_ID", "BIRTH_DATE", "DEATH_DATE", "PAT_STATUS_C",
        "MARITAL_STATUS_C", "PAT_MIDDLE_NAME", "ETHNIC_GROUP_C", "PAT_FIRST_NAME", "PAT_LAST_NAME",
        "SEX_C", "CITY", "STATE_C", "ZIP", "SSN", "LANGUAGE_C", "PA_UPDATE_DATE").distinct()

    var MPV = readData(sqlContext, "MAP_PREDICATE_VALUES", cfg)
    var VAL1 = predicate_value_list(MPV, "IDENTITY_ID", "PATIENT", "IDENTITY_ID", "IDENTITY_TYPE_ID", "COLUMN_VALUE")
    var VAL2 = predicate_value_list(MPV, "IDENTITY_ID", "PATIENT_ID", "IDENTITY_ID", "IDENTITY_TYPE_ID", "COLUMN_VALUE")
    var VAL = VAL1.unionAll(VAL2)
    var ID_SUBTYPE = VAL.withColumn("IDENTITY_TYPE_ID", VAL("COLUMN_VALUE").substr(1,1)).withColumn("IDENTITY_SUBTYPE_ID", VAL("COLUMN_VALUE").substr(1,1))
    var IDENTITY_ID = readTable(sqlContext, dateStr, "IDENTITY_ID", cfg)
    IDENTITY_ID = IDENTITY_ID.withColumnRenamed("GROUPID", "GROUPID_id1").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_id1")
    ID_SUBTYPE = ID_SUBTYPE.withColumnRenamed("GROUPID_id", "GROUPID_idsub").withColumnRenamed("CLIENT_DS_ID_id", "CLIENT_DS_ID_idsub")
    IDENTITY_ID = IDENTITY_ID.withColumnRenamed("IDENTITY_TYPE_ID", "IDENTITY_TYPE_ID1")
    var JOIN_ID = IDENTITY_ID.join(ID_SUBTYPE, IDENTITY_ID("IDENTITY_TYPE_ID1")===ID_SUBTYPE("IDENTITY_TYPE_ID") && IDENTITY_ID("GROUPID_id1")===ID_SUBTYPE("GROUPID") && IDENTITY_ID("CLIENT_DS_ID_id1")===ID_SUBTYPE("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var ID = JOIN_ID.select("GROUPID_id1", "CLIENT_DS_ID_id1",   "IDENTITY_TYPE_ID","IDENTITY_ID", "PAT_ID", "IDENTITY_SUBTYPE_ID").distinct()

    var PAT_RACE = readTable(sqlContext, dateStr, "PAT_RACE", cfg).withColumnRenamed("UPDATE_DATE","RA_UPDATE_DATE").select("GROUPID","CLIENT_DS_ID", "PATIENT_RACE_C", "PAT_ID", "RA_UPDATE_DATE")
    PAT_RACE = PAT_RACE.withColumnRenamed("GROUPID", "GROUPID_pc").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_pc")

    val group1 = Window.partitionBy(PAT_RACE("PAT_ID"),PAT_RACE("GROUPID_pc"),PAT_RACE("CLIENT_DS_ID_pc")).orderBy(PAT_RACE("GROUPID_pc"),PAT_RACE("CLIENT_DS_ID_pc"),PAT_RACE("RA_UPDATE_DATE").desc)
    var PAT_RACE1 = PAT_RACE.withColumn("RA_RN", row_number().over(group1))


    var PATIENT_3 = readTable(sqlContext, dateStr, "PATIENT_3", cfg).select("GROUPID","CLIENT_DS_ID", "IS_TEST_PAT_YN", "PAT_ID", "UPDATE_DATE")
    PATIENT_3 = PATIENT_3.withColumnRenamed("GROUPID", "GROUPID_patient").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_patient")

    val group2 = Window.partitionBy(PATIENT_3("PAT_ID"),PATIENT_3("GROUPID_patient"),PATIENT_3("CLIENT_DS_ID_patient")).orderBy(PATIENT_3("GROUPID_patient"),PATIENT_3("CLIENT_DS_ID_patient"),PATIENT_3("UPDATE_DATE").desc)
    var PAT3_D = PATIENT_3.withColumn("P3_RNBR", row_number().over(group2)).filter("P3_RNBR == 1")



    var PATIENT_FYI_FLAGS = readTable(sqlContext, dateStr, "PATIENT_FYI_FLAGS", cfg).select("GROUPID","CLIENT_DS_ID","PAT_FLAG_TYPE_C","PATIENT_ID")
    PATIENT_FYI_FLAGS = PATIENT_FYI_FLAGS.withColumnRenamed("GROUPID", "GROUPID_pyf").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_pyf")

    var ZH_STATE = readDictionaryTable(sqlContext, "ZH_STATE", cfg).withColumnRenamed("NAME", "ZH_STATE_NAME")
    ZH_STATE = ZH_STATE.withColumnRenamed("GROUPID", "GROUPID_zs").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_zs")
    PA = PA.withColumnRenamed("PAT_ID", "PAT_ID_PA")
    ID = ID.withColumnRenamed("PAT_ID", "PAT_ID_ID")
    PAT_RACE1 = PAT_RACE1.withColumnRenamed("PAT_ID", "PAT_ID_PR")
    PAT3_D = PAT3_D.withColumnRenamed("PAT_ID", "PAT_ID_PR")

    var PA_ID = PA.join(ID, PA("PAT_ID_PA")===ID("PAT_ID_ID") && PA("GROUPID_pat")===ID("GROUPID_id1")  && PA("CLIENT_DS_ID_pat")===ID("CLIENT_DS_ID_id1") , "left_outer").drop("PAT_ID_ID").drop("GROUPID_id1").drop("CLIENT_DS_ID_id1")
    var PA_ID_RA = PA_ID.join(PAT_RACE1, PA_ID("PAT_ID_PA")===PAT_RACE1("PAT_ID_PR") && PA_ID("GROUPID_pat")===PAT_RACE1("GROUPID_pc")  && PA_ID("CLIENT_DS_ID_pat")===PAT_RACE1("CLIENT_DS_ID_pc"), "left_outer").drop("GROUPID_pc").drop("CLIENT_DS_ID_pc")
    var PA_ID_RA_PA3 = PA_ID_RA.join(PAT3_D,  PA_ID("PAT_ID_PA")===PAT3_D("PAT_ID_PR") && PA_ID("GROUPID_pat")===PAT3_D("GROUPID_patient")  && PA_ID("CLIENT_DS_ID_pat")===PAT3_D("CLIENT_DS_ID_patient"), "left_outer").drop("GROUPID_patient").drop("CLIENT_DS_ID_patient")
    var PA_PATFLAG = PA_ID_RA_PA3.join(PATIENT_FYI_FLAGS, PA_ID_RA_PA3("PAT_ID_PA")===PATIENT_FYI_FLAGS("PATIENT_ID") && PA_ID_RA_PA3("GROUPID_pat")===PATIENT_FYI_FLAGS("GROUPID_pyf") && PA_ID_RA_PA3("CLIENT_DS_ID_pat")===PATIENT_FYI_FLAGS("CLIENT_DS_ID_pyf"), "left_outer").drop("GROUPID_pyf").drop("CLIENT_DS_ID_pyf")
    var PA_PATFLAG_ZH = PA_PATFLAG.join(ZH_STATE, PA_PATFLAG("STATE_C")===ZH_STATE("STATE_C") && PA_PATFLAG("GROUPID_pat")===ZH_STATE("GROUPID_zs") &&  PA_PATFLAG("CLIENT_DS_ID_pat")===ZH_STATE("CLIENT_DS_ID_zs"), "left_outer").drop("GROUPID_zs").drop("CLIENT_DS_ID_zs")
    PA_PATFLAG_ZH = PA_PATFLAG_ZH.withColumnRenamed("PAT_ID_PA", "PAT_ID")
    var TEMP_EPIC_PATS_GRPID = PA_PATFLAG_ZH.withColumn("PATIENTID", PA_PATFLAG_ZH("PAT_ID"))
            .withColumn("MEDICALRECORDNUMBER", PA_PATFLAG_ZH("IDENTITY_ID"))
            .withColumn("MRN_ID", PA_PATFLAG_ZH("IDENTITY_ID"))
            .withColumn("DATEOFBIRTH", PA_PATFLAG_ZH("BIRTH_DATE"))
            .withColumn("DATEOFDEATH", PA_PATFLAG_ZH("DEATH_DATE"))
            .withColumn("PAT_STATUS", PA_PATFLAG_ZH("PAT_STATUS_C"))
            .withColumn("MARITAL_STATUS", PA_PATFLAG_ZH("MARITAL_STATUS_C"))
            .withColumn("MIDDLE_NAME", PA_PATFLAG_ZH("PAT_MIDDLE_NAME"))
            .withColumn("ETHNICITY", when(PA_PATFLAG_ZH("ETHNIC_GROUP_C")!== "-1", concat_ws("", lit("e."), PA_PATFLAG_ZH("ETHNIC_GROUP_C")))
                    .when(PA_PATFLAG_ZH("PATIENT_RACE_C").isNull, concat_ws("", lit("r."), PA_PATFLAG_ZH("PATIENT_RACE_C"))))
            .withColumn("ETH_UPDATE_DATE", when(PA_PATFLAG_ZH("ETHNIC_GROUP_C")!== "-1", PA_PATFLAG_ZH("PA_UPDATE_DATE")).otherwise(PA_PATFLAG_ZH("RA_UPDATE_DATE")))
            .withColumn("FIRSTNAME", PA_PATFLAG_ZH("PAT_FIRST_NAME"))
            .withColumn("LASTNAME", PA_PATFLAG_ZH("PAT_LAST_NAME"))
            .withColumn("GENDER", PA_PATFLAG_ZH("SEX_C"))
            .withColumn("RACE", PA_PATFLAG_ZH("PATIENT_RACE_C"))
            .withColumn("RA_UPDATE_DATE", PA_PATFLAG_ZH("UPDATE_DATE"))
            .withColumn("UPDATE_DATE", PA_PATFLAG_ZH("PA_UPDATE_DATE"))
            .withColumn("STATE", PA_PATFLAG_ZH("ZH_STATE_NAME"))
            .withColumn("ZIP", STANDARDIZE_POSTAL_CODE(PA_PATFLAG_ZH("ZIP")))
            .withColumn("LANGUAGE", PA_PATFLAG_ZH("LANGUAGE_C"))
            .withColumn("SSN", when(PA_PATFLAG_ZH("SSN")=== "123-45-6789", null).otherwise(PA_PATFLAG_ZH("SSN")))
            .withColumn("DATASRC", lit("PATREG"))
            .withColumn("DOB_IND", when(PA_PATFLAG_ZH("BIRTH_DATE").isNull, "1").otherwise("0"))

    val group3 = Window.partitionBy(TEMP_EPIC_PATS_GRPID("PATIENTID"),TEMP_EPIC_PATS_GRPID("GROUPID_pat"),TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID_pat")).orderBy(TEMP_EPIC_PATS_GRPID("PATIENTID"),TEMP_EPIC_PATS_GRPID("GROUPID_pat"),TEMP_EPIC_PATS_GRPID("DOB_IND").desc,TEMP_EPIC_PATS_GRPID("UPDATE_DATE").desc)

    var TEMP_PATIENT_2 = TEMP_EPIC_PATS_GRPID.withColumn("DATEOFBIRTH", first(TEMP_EPIC_PATS_GRPID("DATEOFBIRTH")).over(group3))

    val group4 = Window.partitionBy(TEMP_PATIENT_2("PATIENTID"),TEMP_PATIENT_2("GROUPID_pat"),TEMP_PATIENT_2("CLIENT_DS_ID_pat")).orderBy(TEMP_PATIENT_2("GROUPID_pat"),TEMP_PATIENT_2("CLIENT_DS_ID_pat"),TEMP_PATIENT_2("UPDATE_DATE").desc)
    var TEMP_PAT_FINAL = TEMP_PATIENT_2.withColumn("DATEOFDEATH", first(TEMP_PATIENT_2("DATEOFDEATH")).over(group4))
            .withColumn("MEDICALRECORDNUMBER", first(TEMP_PATIENT_2("MEDICALRECORDNUMBER")).over(group4))
            .withColumn("ROWNBR", row_number().over(group4)).filter("ROWNBR==1")
            .withColumn("FACILITYID", lit("NULL")).withColumn("INACTIVE_FLAG", lit("NULL"))


    TEMP_PAT_FINAL = TEMP_PAT_FINAL.withColumn("FACILITYID", when(TEMP_PAT_FINAL("FACILITYID")!=="NULL", TEMP_PAT_FINAL("FACILITYID") ))
            .withColumn("INACTIVE_FLAG", when(TEMP_PAT_FINAL("INACTIVE_FLAG")!=="NULL", TEMP_PAT_FINAL("INACTIVE_FLAG") ))

    var PATIENT_MPI = readData(sqlContext, "PATIENT_MPI", cfg).select("GROUPID","CLIENT_DS_ID","PATIENTID","HGPID","GRP_MPI").withColumnRenamed("GROUPID", "GROUPID_MPI").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_MPI").withColumnRenamed("PATIENTID", "PATIENTID_MPI").distinct()
    TEMP_PAT_FINAL = TEMP_PAT_FINAL.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_tpf").withColumnRenamed("GROUPID", "GROUPID_tpf").withColumnRenamed("HGPID", "HGPID_tpf").withColumnRenamed("GRP_MPI", "GRP_MPI_tpf")
    TEMP_PAT_FINAL = TEMP_PAT_FINAL.join(PATIENT_MPI, TEMP_PAT_FINAL("GROUPID_pat")===PATIENT_MPI("GROUPID_MPI") and TEMP_PAT_FINAL("CLIENT_DS_ID_pat")===PATIENT_MPI("CLIENT_DS_ID_MPI") and TEMP_PAT_FINAL("PATIENTID")===PATIENT_MPI("PATIENTID_MPI"), "left_outer")
    TEMP_PAT_FINAL=TEMP_PAT_FINAL.withColumnRenamed("GROUPID_pat","GROUPID").withColumnRenamed("CLIENT_DS_ID_pat","CLIENT_DS_ID").select( "MRN_ID", "IDENTITY_TYPE_ID", "SSN", "MIDDLE_NAME",  "LANGUAGE", "MARITAL_STATUS", "PAT_STATUS", "ETH_UPDATE_DATE", "ETHNICITY", "RACE", "GENDER", "LASTNAME", "FIRSTNAME", "UPDATE_DATE", "GROUPID", "DATASRC", "PATIENTID","FACILITYID", "DATEOFBIRTH", "MEDICALRECORDNUMBER", "HGPID", "DATEOFDEATH", "CLIENT_DS_ID", "GRP_MPI", "INACTIVE_FLAG", "ZIP")
    //TEMP_PAT_FINAL = TEMP_PAT_FINAL.filter("GRP_MPI IS NULL")
    var TEMP_PAT_JOIN = TEMP_PAT_FINAL.select("GROUPID", "DATASRC", "PATIENTID", "FACILITYID","DATEOFBIRTH","MEDICALRECORDNUMBER","HGPID", "DATEOFDEATH", "CLIENT_DS_ID", "GRP_MPI" , "INACTIVE_FLAG")
    writeGenerated(TEMP_PAT_JOIN, dateStr, "EP2_TEMP_PATIENT", cfg)

    TEMP_PAT_FINAL = TEMP_PAT_FINAL.withColumn("ENCOUNTERID", lit("NULL"))
    TEMP_EPIC_PATS_GRPID = TEMP_PAT_FINAL.select("GROUPID","CLIENT_DS_ID", "MRN_ID", "IDENTITY_TYPE_ID","SSN", "MIDDLE_NAME", "LANGUAGE", "MARITAL_STATUS", "PAT_STATUS", "ETH_UPDATE_DATE", "ETHNICITY","RACE", "GENDER", "ENCOUNTERID", "LASTNAME", "FIRSTNAME", "GROUPID", "DATASRC", "PATIENTID", "FACILITYID","DATEOFBIRTH","MEDICALRECORDNUMBER","HGPID", "DATEOFDEATH", "CLIENT_DS_ID", "GRP_MPI" , "INACTIVE_FLAG", "UPDATE_DATE","ZIP")

    //      ---TEMP_PATIENT_DETAIL_FIRST_NAME
    var TEMP_PATIENT_DETAIL_FIRST_NAME = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("FIRST_NAME")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("FIRSTNAME")).select("HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
    var group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_FIRST_NAME("GROUPID"),TEMP_PATIENT_DETAIL_FIRST_NAME("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_FIRST_NAME("PATIENTID"), TEMP_PATIENT_DETAIL_FIRST_NAME("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_FIRST_NAME("GROUPID"),TEMP_PATIENT_DETAIL_FIRST_NAME("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_FIRST_NAME("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_FNAME1 = TEMP_PATIENT_DETAIL_FIRST_NAME.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null").drop("ROW_NUM")
    var TEMP_PAT_DET_FST_NAME = FIL_TEMP_PATDET_FNAME1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    //        ---TEMP_PATIENT_DETAIL_FIRST_NAME
    var TEMP_PATIENT_DETAIL_LAST_NAME = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("LAST_NAME")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("LASTNAME")).select("HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_LAST_NAME("GROUPID"),TEMP_PATIENT_DETAIL_LAST_NAME("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_LAST_NAME("PATIENTID"), TEMP_PATIENT_DETAIL_LAST_NAME("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_LAST_NAME("GROUPID"),TEMP_PATIENT_DETAIL_LAST_NAME("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_LAST_NAME("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_LNAME1 = TEMP_PATIENT_DETAIL_LAST_NAME.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_LST_NAME = FIL_TEMP_PATDET_LNAME1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    //        ---TEMP_PATIENT_DETAIL_CITY
    var TEMP_PATIENT_DETAIL_CITY = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("CITY")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("LASTNAME")).select("HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_CITY("GROUPID"),TEMP_PATIENT_DETAIL_CITY("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_CITY("PATIENTID"), TEMP_PATIENT_DETAIL_CITY("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_CITY("GROUPID"),TEMP_PATIENT_DETAIL_CITY("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_CITY("PATDETAIL_TIMESTAMP").desc)
    var TEMP_PATDET_CITY1 = TEMP_PATIENT_DETAIL_CITY.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_CITY = TEMP_PATDET_CITY1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")
    //        ---TEMP_PATIENT_DETAIL_CITY
    var TEMP_PATIENT_DETAIL_STATE = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("STATE")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("LASTNAME")).select("HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_STATE("GROUPID"),TEMP_PATIENT_DETAIL_STATE("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_STATE("PATIENTID"), TEMP_PATIENT_DETAIL_STATE("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_STATE("GROUPID"),TEMP_PATIENT_DETAIL_STATE("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_STATE("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_STATE1 = TEMP_PATIENT_DETAIL_STATE.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_STATE = FIL_TEMP_PATDET_STATE1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    //        ---TEMP_PATIENT_DETAIL_CITY
    var TEMP_PATIENT_DETAIL_ZIP = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("ZIPCODE")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("ZIP")).select("HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_ZIP("GROUPID"),TEMP_PATIENT_DETAIL_ZIP("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_ZIP("PATIENTID"), TEMP_PATIENT_DETAIL_ZIP("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_ZIP("GROUPID"),TEMP_PATIENT_DETAIL_ZIP("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_ZIP("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_ZIP1 = TEMP_PATIENT_DETAIL_ZIP.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_ZIP = FIL_TEMP_PATDET_ZIP1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    var TEMP_PATIENT_DETAIL_GENDER = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("GENDER")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("GENDER"))).select("GENDER", "HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "GENDER")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_GENDER("GROUPID"),TEMP_PATIENT_DETAIL_GENDER("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_GENDER("PATIENTID"), TEMP_PATIENT_DETAIL_GENDER("GENDER")).orderBy(TEMP_PATIENT_DETAIL_GENDER("GROUPID"),TEMP_PATIENT_DETAIL_GENDER("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_GENDER("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_GENDER1 = TEMP_PATIENT_DETAIL_GENDER.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_GENDER = FIL_TEMP_PATDET_GENDER1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    var TEMP_PATIENT_DETAIL_RACE = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("RACE")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("RACE"))).select("RACE", "HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "RACE")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_RACE("GROUPID"),TEMP_PATIENT_DETAIL_RACE("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_RACE("PATIENTID"), TEMP_PATIENT_DETAIL_RACE("RACE")).orderBy(TEMP_PATIENT_DETAIL_RACE("GROUPID"),TEMP_PATIENT_DETAIL_RACE("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_RACE("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_RACE1 = TEMP_PATIENT_DETAIL_RACE.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_RACE = FIL_TEMP_PATDET_RACE1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    var TEMP_PATIENT_DETAIL_ETHNICITY = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("ETHNICITY")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("ETHNICITY"))).select("ETH_UPDATE_DATE", "ETHNICITY","HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "ETHNICITY", "ETH_UPDATE_DATE")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_ETHNICITY("GROUPID"),TEMP_PATIENT_DETAIL_ETHNICITY("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_ETHNICITY("PATIENTID"), TEMP_PATIENT_DETAIL_ETHNICITY("ETHNICITY")).orderBy(TEMP_PATIENT_DETAIL_ETHNICITY("GROUPID"),TEMP_PATIENT_DETAIL_ETHNICITY("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_ETHNICITY("ETH_UPDATE_DATE").desc)
    var FIL_TEMP_PATDET_ETHNICITY1 = TEMP_PATIENT_DETAIL_ETHNICITY.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_ETHNICITY = FIL_TEMP_PATDET_ETHNICITY1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    var TEMP_PATIENT_DETAIL_STATUS = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("DECEASED")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("PAT_STATUS"))).select("PAT_STATUS","HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "PAT_STATUS")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_STATUS("GROUPID"),TEMP_PATIENT_DETAIL_STATUS("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_STATUS("PATIENTID"), TEMP_PATIENT_DETAIL_STATUS("PAT_STATUS")).orderBy(TEMP_PATIENT_DETAIL_STATUS("GROUPID"),TEMP_PATIENT_DETAIL_STATUS("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_STATUS("PATDETAIL_TIMESTAMP").desc)
    var TEMP_PATDET_STATUS1 = TEMP_PATIENT_DETAIL_STATUS.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_STATUS = TEMP_PATDET_STATUS1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    var TEMP_PATIENT_DETAIL_MARITAL = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("MARITAL")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("PAT_STATUS"))).select("MARITAL_STATUS", "HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "MARITAL_STATUS")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_MARITAL("GROUPID"),TEMP_PATIENT_DETAIL_MARITAL("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_MARITAL("PATIENTID"), TEMP_PATIENT_DETAIL_MARITAL("MARITAL_STATUS")).orderBy(TEMP_PATIENT_DETAIL_MARITAL("GROUPID"),TEMP_PATIENT_DETAIL_MARITAL("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_MARITAL("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_MARITAL1 = TEMP_PATIENT_DETAIL_MARITAL.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_MARITAL = FIL_TEMP_PATDET_MARITAL1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")


    var TEMP_PATIENT_DETAIL_MIDDLE_NAME = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("MIDDLE_NAME")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE", TEMP_EPIC_PATS_GRPID("MIDDLE_NAME")).select("MIDDLE_NAME", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE","HGPID", "GRP_MPI", "ENCOUNTERID", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_MIDDLE_NAME("GROUPID"),TEMP_PATIENT_DETAIL_MIDDLE_NAME("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_MIDDLE_NAME("PATIENTID"), TEMP_PATIENT_DETAIL_MIDDLE_NAME("LOCALVALUE")).orderBy(TEMP_PATIENT_DETAIL_MIDDLE_NAME("GROUPID"),TEMP_PATIENT_DETAIL_MIDDLE_NAME("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_MIDDLE_NAME("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_MNAME1 = TEMP_PATIENT_DETAIL_MIDDLE_NAME.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_MID_NAME = FIL_TEMP_PATDET_MNAME1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")


    var TEMP_PATIENT_DETAIL_LANG = TEMP_EPIC_PATS_GRPID.withColumn("DATASRC", lit("PATREG")).withColumn("PATIENTDETAILTYPE", lit("LANGUAGE")).withColumn("PATDETAIL_TIMESTAMP", TEMP_EPIC_PATS_GRPID("UPDATE_DATE")).withColumn("LOCALVALUE",  concat(TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID"), lit("."), TEMP_EPIC_PATS_GRPID("LANGUAGE"))).select( "GROUPID", "CLIENT_DS_ID","LANGUAGE", "HGPID", "GRP_MPI", "ENCOUNTERID", "GROUPID", "CLIENT_DS_ID", "DATASRC", "PATIENTDETAILTYPE", "PATIENTID", "PATDETAIL_TIMESTAMP", "LOCALVALUE", "LANGUAGE")
    group5 = Window.partitionBy(TEMP_PATIENT_DETAIL_LANG("GROUPID"),TEMP_PATIENT_DETAIL_LANG("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_LANG("PATIENTID"), TEMP_PATIENT_DETAIL_LANG("LANGUAGE")).orderBy(TEMP_PATIENT_DETAIL_LANG("GROUPID"),TEMP_PATIENT_DETAIL_LANG("CLIENT_DS_ID"),TEMP_PATIENT_DETAIL_LANG("PATDETAIL_TIMESTAMP").desc)
    var FIL_TEMP_PATDET_LANG1 = TEMP_PATIENT_DETAIL_LANG.withColumn("ROW_NUM", row_number().over(group5)).filter("ROW_NUM == 1 and LOCALVALUE is not null")
    var TEMP_PAT_DET_LANGUAGE = FIL_TEMP_PATDET_LANG1.withColumn("FACILITYID", lit("NULL")).withColumn("PATIENTDETAILQUAL", lit("NULL")).select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP","CLIENT_DS_ID", "HGPID", "GRP_MPI")

    var TEMP_PATIENT_ID_SSN1 = TEMP_EPIC_PATS_GRPID.filter("SSN is not null").withColumn("DATASRC", lit("PATREG")).withColumn("IDVALUE", TEMP_EPIC_PATS_GRPID("SSN")).withColumn("IDTYPE", lit("SSN")).withColumn("ID_SUBTYPE", lit("NULL")).select("SSN", "GROUPID", "PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE")
    VAL2 = VAL2.withColumnRenamed("GROUPID", "GROUPID_val").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_val")
    var TEMP_PATIENT_ID_MRN = TEMP_EPIC_PATS_GRPID.join(VAL2, TEMP_EPIC_PATS_GRPID("IDENTITY_TYPE_ID")===VAL2("COLUMN_VALUE") && TEMP_EPIC_PATS_GRPID("GROUPID")===VAL2("GROUPID_val") && TEMP_EPIC_PATS_GRPID("CLIENT_DS_ID")===VAL2("CLIENT_DS_ID_val"), "left_outer").drop("GROUPID_val").drop("CLIENT_DS_ID_val")
    TEMP_PATIENT_ID_MRN=TEMP_PATIENT_ID_MRN.withColumn("DATASRC", lit("PATREG")).withColumn("IDVALUE", concat_ws("", TEMP_EPIC_PATS_GRPID("MRN_ID"), lit("A"))).withColumn("IDTYPE", lit("MRN")).withColumn("ID_SUBTYPE", lit("NULL"))
    TEMP_PATIENT_ID_MRN=TEMP_PATIENT_ID_MRN.select("GROUPID", "PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE",  "IDENTITY_TYPE_ID", "MRN_ID")
    TEMP_PATIENT_ID_MRN=TEMP_PATIENT_ID_MRN.withColumn("IDENTITY_SUBTYPE_ID", when(TEMP_PATIENT_ID_MRN("IDENTITY_TYPE_ID").isNotNull, concat_ws("", TEMP_PATIENT_ID_MRN("IDENTITY_TYPE_ID"), lit("."))))
    TEMP_PATIENT_ID_MRN = TEMP_PATIENT_ID_MRN.withColumn("MRN_SUBTYPE_JOIN", concat_ws("", TEMP_PATIENT_ID_MRN("IDENTITY_SUBTYPE_ID"), TEMP_PATIENT_ID_MRN("IDENTITY_TYPE_ID")))

    var JOIN_EPIC_PATS_MPV1 = TEMP_PATIENT_ID_MRN.join(VAL2, TEMP_PATIENT_ID_MRN("GROUPID")===VAL2("GROUPID_val") and  TEMP_PATIENT_ID_MRN("CLIENT_DS_ID")===VAL2("CLIENT_DS_ID_val") and TEMP_PATIENT_ID_MRN("MRN_SUBTYPE_JOIN")===VAL2("COLUMN_VALUE"), "inner").drop("CLIENT_DS_ID_val").drop("GROUP_val")
    var FILTER_EPIC_PATS_MPV1 = JOIN_EPIC_PATS_MPV1.filter("MRN_ID is not null and IDENTITY_SUBTYPE_ID is not null")
    var TEMP_PATIENT_ID_MRN1 = FILTER_EPIC_PATS_MPV1.withColumn("DATASRC", lit("PATREG")).withColumn("IDTYPE", lit("MRN")).withColumn("IDVALUE", lit("MRN_ID")).withColumn("ID_SUBTYPE", lit("NULL"))
            .select("GROUPID", "PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE")


    var TEMP_PATIENTDETAIL = TEMP_PAT_DET_LANGUAGE.unionAll(TEMP_PAT_DET_STATE).unionAll(TEMP_PAT_DET_MID_NAME).unionAll(TEMP_PAT_DET_MARITAL)
            .unionAll(TEMP_PAT_DET_STATUS).unionAll(TEMP_PAT_DET_ETHNICITY).unionAll(TEMP_PAT_DET_RACE).unionAll(TEMP_PAT_DET_GENDER).unionAll(TEMP_PAT_DET_ZIP)
            .unionAll(TEMP_PAT_DET_CITY).unionAll(TEMP_PAT_DET_LST_NAME).unionAll(TEMP_PAT_DET_FST_NAME)


    TEMP_PATIENT_ID_SSN1 = TEMP_PATIENT_ID_SSN1.withColumnRenamed("HGPID","HGPID_SSN").withColumnRenamed("GRP_MPI","GRP_MPI_SSN").drop("IDENTITY_SUBTYPE_ID").drop("IDENTITY_TYPE_ID").drop("MRN_ID").drop("MRN_SUBTYPE_JOIN").drop("SSN")
    TEMP_PATIENT_ID_MRN = TEMP_PATIENT_ID_MRN.withColumnRenamed("GRP_MPI","GRP_MPI_MRN").withColumnRenamed("GRP_MPI","GRP_MPI_MRN").drop("IDENTITY_SUBTYPE_ID").drop("IDENTITY_TYPE_ID").drop("MRN_ID").drop("MRN_SUBTYPE_JOIN")

    var TEMP_PATIENT_ID_1 = TEMP_PATIENT_ID_SSN1.unionAll(TEMP_PATIENT_ID_MRN).unionAll(TEMP_PATIENT_ID_MRN1)
    var TEMP_PATIENT_ID_JOIN = TEMP_PATIENT_ID_1.join(PATIENT_MPI, TEMP_PATIENT_ID_1("GROUPID")===PATIENT_MPI("GROUPID_MPI") and  TEMP_PATIENT_ID_1("CLIENT_DS_ID")===PATIENT_MPI("CLIENT_DS_ID_MPI") and TEMP_PATIENT_ID_1("PATIENTID")===PATIENT_MPI("PATIENTID_MPI"), "left_outer")
            .select("GROUPID", "PATIENTID", "DATASRC", "IDTYPE", "IDVALUE", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE")

    var TEMP_PATDETAIL_JOIN = TEMP_PATIENTDETAIL.withColumnRenamed("HGPID","HGPID_tpj1").withColumnRenamed("GRP_MPI","GRP_MPI_tpj1").join(PATIENT_MPI, TEMP_PATIENT_ID_1("GROUPID")===PATIENT_MPI("GROUPID_MPI") and  TEMP_PATIENT_ID_1("CLIENT_DS_ID")===PATIENT_MPI("CLIENT_DS_ID_MPI") and TEMP_PATIENT_ID_1("PATIENTID")===PATIENT_MPI("PATIENTID_MPI"), "left_outer")

    var TEMP_PATDETAIL = TEMP_PATDETAIL_JOIN.withColumn("DATASRC", lit("PATREG")).withColumn("IDTYPE", lit("MRN")).withColumn("IDVALUE", lit("MRN_ID")).withColumn("ID_SUBTYPE", lit("NULL"))
            .select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID", "HGPID", "GRP_MPI")

    TEMP_PATDETAIL = TEMP_PATDETAIL.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")


    writeGenerated(TEMP_PATDETAIL,dateStr,  "EP2_TEMP_PATIENT_DETAILS", cfg)

    TEMP_PATIENT_ID_JOIN = TEMP_PATIENT_ID_JOIN.select("GROUPID","PATIENTID", "DATASRC", "IDTYPE", "IDVALUE","CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE");

    writeGenerated(TEMP_PATIENT_ID_JOIN,dateStr,  "EP2_TEMP_PATIENT_ID", cfg)

}




def processCRTempPatientDetails(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {

    // var tmpPath = "/user/rbabu/tmp/rxorder/cernerv2_output"
    var mpv = readData(sqlContext, "MAP_PREDICATE_VALUES", cfg)

    var list_person_alias_type_cd = predicate_value_list(mpv, "PATIENT", "PATIENT", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "LIST_PERSON_ALIAS_TYPE_CD_COL_VAL")
    var list_ssn_person_alias_type_cd = predicate_value_list(mpv, "SSN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "list_ssn_person_alias_type_cd_val")
    var list_cmrn_person_alias_type_cd = predicate_value_list(mpv, "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "list_cmrn_person_alias_type_cd_val")
    var list_citizenship_cd = predicate_value_list(mpv, "PATIENT", "PATIENT", "PERSON", "CITIZENSHIP_CD", "list_citizenship_cd_val").select("list_citizenship_cd_val")
    var list_rmrn_person_alias_type_cd = predicate_value_list(mpv, "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "list_rmrn_person_alias_type_cd_val")
    var list_mcmrn_prsn_alias_type_cd = predicate_value_list(mpv, "MRN_CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "PERSON_ALIAS_TYPE_CD", "list_mcmrn_prsn_alias_type_cd_val")
    var list_cmrn_alias_pool_cd = predicate_value_list(mpv, "CMRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD", "list_cmrn_alias_pool_cd_val")
    var list_alias_pool_cd = predicate_value_list(mpv, "MRN", "PATIENT_IDENTIFIER", "PERSON_ALIAS", "ALIAS_POOL_CD", "LIST_ALIAS_POOL_CD_COL_VAL")
    var LIST_RMRN_ALIAS_POOL_CD = predicate_value_list(mpv, "MRN","PATIENT_IDENTIFIER","PERSON_ALIAS","ALIAS_POOL_CD", "LIST_RMRN_ALIAS_POOL_CD_VAL");
    var LIST_CMRN_ALIAS_POOL_CD = predicate_value_list(mpv, "CMRN","PATIENT_IDENTIFIER","PERSON_ALIAS","ALIAS_POOL_CD", "LIST_CMRN_ALIAS_POOL_CD_VAL");


    var person = readTable(sqlContext,dateStr, "PERSON",cfg).select("GROUPID", "CLIENT_DS_ID", "NAME_LAST", "NAME_FULL_FORMATTED","PERSON_ID", "BIRTH_DT_TM", "DECEASED_DT_TM", "NAME_FIRST", "UPDT_DT_TM", "religion_cd", "BEG_EFFECTIVE_DT_TM",
        "ethnic_grp_cd", "sex_cd", "race_cd", "deceased_cd","nationality_cd", "language_cd", "marital_type_cd", "name_middle", "active_ind", "citizenship_cd").withColumnRenamed("active_ind", "person_active_ind")
    person = person.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_person").withColumnRenamed("GROUPID", "GROUPID_person")

    var name_last = person("name_last")
    var name_full_formatted = person("name_full_formatted")
    var person1 = person.filter((name_last.contains("zz").or(name_last.contains("ZZ")).and((name_full_formatted.contains("patient").and(name_full_formatted.contains("test"))))) !==true)

    var personAlias = readTable(sqlContext,dateStr, "PERSON_ALIAS",cfg).select( "GROUPID", "CLIENT_DS_ID", "person_alias_type_cd", "alias_pool_cd", "person_id", "end_effective_dt_tm","alias", "updt_dt_tm", "active_ind").withColumnRenamed("person_id", "person_alias_id")
    personAlias = personAlias.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_pa").withColumnRenamed("GROUPID", "GROUPID_pa")

    var person_alias_type_cd = personAlias("person_alias_type_cd")
    var alias_pool_cd = personAlias("alias_pool_cd")
    var alias = personAlias("alias")
    personAlias = personAlias.withColumn("end_effective_dt_tm", from_unixtime(personAlias("end_effective_dt_tm").divide(1000)))
    var end_effective_dt_tm =personAlias("end_effective_dt_tm")

    var pa0 = personAlias.filter(end_effective_dt_tm < current_timestamp())

    var pa = personAlias.join(list_cmrn_person_alias_type_cd, personAlias("person_alias_type_cd")===list_cmrn_person_alias_type_cd("list_cmrn_person_alias_type_cd_val") && personAlias("GROUPID_pa")===list_cmrn_person_alias_type_cd("GROUPID") && personAlias("CLIENT_DS_ID_pa")===list_cmrn_person_alias_type_cd("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    pa = pa.join(list_ssn_person_alias_type_cd, pa("person_alias_type_cd")===list_ssn_person_alias_type_cd("list_ssn_person_alias_type_cd_val")&& personAlias("GROUPID_pa")===list_ssn_person_alias_type_cd("GROUPID") && personAlias("CLIENT_DS_ID_pa")===list_ssn_person_alias_type_cd("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    pa = pa.join(list_person_alias_type_cd, pa("person_alias_type_cd")===list_person_alias_type_cd("LIST_PERSON_ALIAS_TYPE_CD_COL_VAL")&& personAlias("GROUPID_pa")===list_person_alias_type_cd("GROUPID") && personAlias("CLIENT_DS_ID_pa")===list_person_alias_type_cd("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    pa = pa.join(list_alias_pool_cd, pa("alias_pool_cd")===list_alias_pool_cd("LIST_ALIAS_POOL_CD_COL_VAL")&& personAlias("GROUPID_pa")===list_alias_pool_cd("GROUPID") && personAlias("CLIENT_DS_ID_pa")===list_alias_pool_cd("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    pa = pa.join(list_rmrn_person_alias_type_cd, pa("ALIAS_POOL_CD")===list_rmrn_person_alias_type_cd("list_rmrn_person_alias_type_cd_val")&& personAlias("GROUPID_pa")===list_rmrn_person_alias_type_cd("GROUPID") && personAlias("CLIENT_DS_ID_pa")===list_rmrn_person_alias_type_cd("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    pa = pa.join(LIST_RMRN_ALIAS_POOL_CD, pa("ALIAS_POOL_CD")===LIST_RMRN_ALIAS_POOL_CD("LIST_RMRN_ALIAS_POOL_CD_VAL")&& personAlias("GROUPID_pa")===LIST_RMRN_ALIAS_POOL_CD("GROUPID") && personAlias("CLIENT_DS_ID_pa")===LIST_RMRN_ALIAS_POOL_CD("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    pa = pa.join(LIST_CMRN_ALIAS_POOL_CD, pa("ALIAS_POOL_CD")===LIST_CMRN_ALIAS_POOL_CD("LIST_CMRN_ALIAS_POOL_CD_VAL")&& personAlias("GROUPID_pa")===LIST_CMRN_ALIAS_POOL_CD("GROUPID") && personAlias("CLIENT_DS_ID_pa")===LIST_CMRN_ALIAS_POOL_CD("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")

    pa = pa.filter((pa("person_alias_type_cd").isNotNull.or(pa("person_alias_type_cd").isNotNull).or(pa("list_ssn_person_alias_type_cd_val").isNotNull).or(pa("LIST_PERSON_ALIAS_TYPE_CD_COL_VAL").isNotNull)
            .or(pa("list_cmrn_person_alias_type_cd_val").isNotNull)).or(pa("list_rmrn_person_alias_type_cd_val").isNotNull))

    val alias_row = Window.partitionBy(pa("person_alias_id"), pa("LIST_PERSON_ALIAS_TYPE_CD_COL_VAL"), pa("GROUPID_pa"), pa("CLIENT_DS_ID_pa")).orderBy(pa("updt_dt_tm").desc, pa("active_ind").desc)
    var pa1 = pa.withColumn("alias_row", row_number.over(alias_row))
    var personAlias1 = pa1.withColumnRenamed("updt_dt_tm", "pa_updt_dt_tm").filter(pa1("alias_row")===1)

    var ppa = person1.join(personAlias1, person1("person_id") === personAlias1("person_alias_id") &&  person1("GROUPID_person") === personAlias1("GROUPID_pa") &&  person1("CLIENT_DS_ID_person") === personAlias1("CLIENT_DS_ID_pa"), "left_outer").drop("GROUPID_pa").drop("CLIENT_DS_ID_pa")

    var address = readDictionaryTable(sqlContext, "ADDRESS", cfg).select("GROUPID", "CLIENT_DS_ID", "ZIPCODE", "PARENT_ENTITY_ID")
    address=address.withColumn("ZIPCODE", when(address("ZIPCODE") !== "0", address("ZIPCODE").substr(1,5))).select("GROUPID", "CLIENT_DS_ID", "ZIPCODE", "PARENT_ENTITY_ID")
    address = address.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_address").withColumnRenamed("GROUPID", "GROUPID_address")
    ppa = ppa.join(address, ppa("PERSON_ID")===address("PARENT_ENTITY_ID") and ppa("CLIENT_DS_ID_person")===address("CLIENT_DS_ID_address") and ppa("GROUPID_person")===address("GROUPID_address"), "left_outer")


    var ppa2 = ppa.withColumnRenamed("GROUPID", "GROUPID_person").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_person").select("person_id", "birth_dt_tm", "deceased_dt_tm", "name_first", "name_last", "updt_dt_tm", "pa_updt_dt_tm",
        "end_effective_dt_tm", "beg_effective_dt_tm", "ethnic_grp_cd", "sex_cd", "race_cd", "deceased_cd",
        "nationality_cd", "language_cd", "marital_type_cd", "name_middle", "person_active_ind", "person_alias_type_cd", "alias_pool_cd",
        "alias",  "citizenship_cd", "list_cmrn_person_alias_type_cd_val", "list_ssn_person_alias_type_cd_val", "LIST_PERSON_ALIAS_TYPE_CD_COL_VAL",
        "LIST_ALIAS_POOL_CD_COL_VAL", "list_rmrn_person_alias_type_cd_val", "LIST_RMRN_ALIAS_POOL_CD_VAL", "LIST_CMRN_ALIAS_POOL_CD_VAL", "religion_cd","GROUPID_person", "CLIENT_DS_ID_person", "ZIPCODE", "PARENT_ENTITY_ID")

    var PATIENT_MPI1 = readData(sqlContext, "PATIENT_MPI", cfg).select("GROUPID","CLIENT_DS_ID","PATIENTID","HGPID","GRP_MPI")

    ppa2 = ppa2.join(PATIENT_MPI1, ppa2("person_id")===PATIENT_MPI1("patientid") && ppa2("GROUPID_person")===PATIENT_MPI1("GROUPID") && ppa2("CLIENT_DS_ID_person")===PATIENT_MPI1("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    var client_ds_id=ppa2("CLIENT_DS_ID_person")

    var ppa3 = ppa2.withColumnRenamed("person_id", "personid")
    ppa3=ppa3.withColumnRenamed("birth_dt_tm", "dateofbirth")
    ppa3=ppa3.withColumnRenamed("deceased_dt_tm", "dateofdeath")
    ppa3=ppa3.withColumnRenamed("name_first", "first_name")
    ppa3=ppa3.withColumnRenamed("name_last", "last_name")
    ppa3=ppa3.withColumnRenamed("updt_dt_tm", "update_date")
    ppa3=ppa3.withColumnRenamed("pa_updt_dt_tm", "pa_update_date")
    ppa3=ppa3.withColumn("ethnicity_value", when(ppa2("ethnic_grp_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("ethnic_grp_cd"))))
    ppa3=ppa3.withColumn("gender", when(ppa2("sex_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("sex_cd"))))
    ppa3=ppa3.withColumn("race", when(ppa2("race_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("race_cd"))))
    ppa3=ppa3.withColumn("death_ind", when(ppa2("deceased_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("deceased_cd"))))
    ppa3=ppa3.withColumn("ethnic_grp", when(ppa2("nationality_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("nationality_cd"))))
    ppa3=ppa3.withColumn("language", when(ppa2("language_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("language_cd"))))
    ppa3=ppa3.withColumn("marital", when(ppa2("marital_type_cd").isNull, "0").otherwise(concat_ws(".", client_ds_id, ppa2("marital_type_cd"))))
    ppa3=ppa3.withColumnRenamed("name_middle", "middle_name")
    ppa3=ppa3.withColumn("mrn", when(ppa3("person_active_ind") === "1" and (ppa3("LIST_PERSON_ALIAS_TYPE_CD_COL_VAL").isNotNull.or(ppa3("LIST_ALIAS_POOL_CD_COL_VAL").isNotNull)), ppa3("alias")))
    ppa3=ppa3.withColumn("rmrn", when(ppa3("person_active_ind") === "1" and (ppa3("list_rmrn_person_alias_type_cd_val").isNotNull.or(ppa3("LIST_RMRN_ALIAS_POOL_CD_VAL").isNotNull)), ppa3("alias")))
    ppa3=ppa3.withColumn("empi", when(ppa3("person_alias_type_cd") === "2", ppa3("alias")))
    ppa3=ppa3.withColumn("ssn", when(ppa3("person_alias_type_cd") === ppa3("list_ssn_person_alias_type_cd_val"), ppa3("alias")))
    ppa3=ppa3.withColumn("ID_SUBTYPE", when(ppa3("LIST_CMRN_ALIAS_POOL_CD_VAL").isNotNull, "CMRN").otherwise(when(ppa3("LIST_RMRN_ALIAS_POOL_CD_VAL").isNotNull, "MRN")))
    ppa3=ppa3.withColumn("active_id_flag", when(ppa3("person_active_ind") === "1", "1"))
    ppa3=ppa3.withColumnRenamed("display", "religion_cd")
    ppa3=ppa3.withColumnRenamed("citizenship_cd", "citizenship_code")
    ppa3 = ppa3.withColumn("ethnicity", when(ppa3("ethnicity_value").isNotNull, ppa3("ethnicity_value")).otherwise(ppa3("race")))

    val dob = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person")).orderBy((when(ppa3("dateofbirth").isNull, lit("1")).otherwise(lit("0"))).desc)
    val dod = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person")).orderBy(ppa3("update_date").desc)
    val medicalrecordnumber = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person")).orderBy(ppa3("active_id_flag").desc, (when(ppa3("mrn").isNull, lit("1")).otherwise(lit("0"))).desc, ppa3("pa_update_date"))
    val medicalrecordnumber_excp = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person")).orderBy(ppa3("active_id_flag").desc, (when(ppa3("rmrn").isNull, lit("1")).otherwise(lit("0"))).desc, ppa3("pa_update_date"))
    val rownumber = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person")).orderBy(ppa3("update_date").desc)
    val status_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), ppa3("death_ind")).orderBy(ppa3("update_date").desc)
    val ethnicity_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), when(ppa3("ethnicity_value").isNull, ppa3("race")).otherwise(ppa3("ethnicity_value"))).orderBy(ppa3("update_date").desc)
    val first_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("first_name"))).orderBy(ppa3("update_date").desc)
    val last_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("last_name"))).orderBy(ppa3("update_date").desc)
    val gender_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("gender"))).orderBy(ppa3("update_date").desc)
    val race_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("race"))).orderBy(ppa3("update_date").desc)
    val ssn_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("ssn"))).orderBy(ppa3("update_date").desc)
    val empi_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), when(ppa3("empi").isNull, "0").otherwise("1")).orderBy(ppa3("end_effective_dt_tm").desc, ppa3("beg_effective_dt_tm").desc, ppa3("pa_update_date").desc)
    val mrn_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("mrn"))).orderBy(ppa3("update_date").desc)
    val rmrn_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("rmrn"))).orderBy(ppa3("update_date").desc)
    val ethnic_grp_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("ethnic_grp"))).orderBy(ppa3("update_date").desc)
    val language_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("language"))).orderBy(ppa3("update_date").desc)
    val marital_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("marital"))).orderBy(ppa3("update_date").desc)
    val middle_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("middle_name"))).orderBy(ppa3("update_date").desc)
    val religion_row = Window.partitionBy(ppa3("patientid"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("religion_cd"))).orderBy(ppa3("update_date").desc)
    val zipcode_row = Window.partitionBy(ppa3("PARENT_ENTITY_ID"), ppa3("GROUPID_person"), ppa3("CLIENT_DS_ID_person"), upper(ppa3("ZIPCODE"))).orderBy(ppa3("update_date").desc)

    var ppa5 = ppa3.withColumn("GROUPID", ppa3("GROUPID_person"))
    ppa5=ppa5.withColumn("datasrc", lit("patient"))
    ppa5=ppa5.withColumn("dob", first("dateofbirth").over(dob))
    ppa5=ppa5.withColumn("dod", first("dateofdeath").over(dod))
    ppa5=ppa5.withColumn("MEDICALRECORDNUMBER", first("mrn").over(medicalrecordnumber))
    ppa5=ppa5.withColumn("medicalrecordnumber_excp", first("rmrn").over(medicalrecordnumber_excp))
    ppa5=ppa5.withColumn("rownumber", row_number().over(rownumber))
    ppa5=ppa5.withColumn("status_row", row_number().over(status_row))
    ppa5=ppa5.withColumn("ethnicity_row", row_number().over(ethnicity_row))
    ppa5=ppa5.withColumn("last_row", row_number().over(last_row))
    ppa5=ppa5.withColumn("race_row", row_number().over(race_row))
    ppa5=ppa5.withColumn("first_row", row_number().over(first_row))
    ppa5=ppa5.withColumn("gender_row", row_number().over(gender_row))
    ppa5=ppa5.withColumn("ssn_row", row_number().over(ssn_row))
    ppa5=ppa5.withColumn("empi_row", row_number().over(empi_row))
    ppa5=ppa5.withColumn("mrn_row", row_number().over(mrn_row))
    ppa5=ppa5.withColumn("rmrn_row", row_number().over(rmrn_row))
    ppa5=ppa5.withColumn("ethnic_grp_row", row_number().over(ethnic_grp_row))
    ppa5=ppa5.withColumn("language_row", row_number().over(language_row))
    ppa5=ppa5.withColumn("marital_row", row_number().over(marital_row))
    ppa5=ppa5.withColumn("middle_row", row_number().over(middle_row))
    ppa5=ppa5.withColumn("religion_row", row_number().over(religion_row))
    ppa5=ppa5.withColumn("zip_row", row_number().over(zipcode_row))

    ppa5=ppa5.withColumn("middle_row", row_number().over(middle_row))
    ppa5=ppa5.withColumn("FACILITYID", lit("NULL"))
    ppa5=ppa5.withColumn("ENCOUNTERID", lit("NULL"))
    ppa5=ppa5.withColumn("PATIENTDETAILQUAL", lit("NULL"))


    var temp_patient = ppa5.withColumn("INACTIVE_FLAG", lit("NULL")).select("GROUPID", "DATASRC", "PATIENTID", "FACILITYID","DATEOFBIRTH","MEDICALRECORDNUMBER","HGPID",
        "DATEOFDEATH", "CLIENT_DS_ID_person", "GRP_MPI" , "INACTIVE_FLAG")
    temp_patient = temp_patient.withColumn("FACILITYID", when(temp_patient("FACILITYID")!== "NULL", temp_patient("FACILITYID"))).withColumn("INACTIVE_FLAG", when(temp_patient("INACTIVE_FLAG")!== "NULL", temp_patient("INACTIVE_FLAG")))
    temp_patient = temp_patient.withColumnRenamed("CLIENT_DS_ID_person", "CLIENT_DS_ID")
    temp_patient = temp_patient.select("GROUPID", "DATASRC", "PATIENTID", "FACILITYID","DATEOFBIRTH","MEDICALRECORDNUMBER","HGPID","DATEOFDEATH","CLIENT_DS_ID", "GRP_MPI","INACTIVE_FLAG")

    writeGenerated(temp_patient,dateStr, "CR2_TEMP_PATIENT",cfg)

    ppa5 = ppa5.withColumnRenamed("CLIENT_DS_ID_person", "client_ds_id")
    var temp_patientdetail1 = ppa5.filter(ppa5("first_row") === 1 && ppa5("first_name").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "first_name", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("FIRST_NAME"))
            .withColumn("localvalue", ppa5("first_name"))
    temp_patientdetail1 = temp_patientdetail1.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")

    var temp_patientdetail2 = ppa5.filter(ppa5("last_row") === 1 && ppa5("last_name").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "last_name", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("LAST_NAME"))
            .withColumn("localvalue", ppa5("last_name"))
    temp_patientdetail2 = temp_patientdetail2.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")

    var temp_patientdetail3 = ppa5.filter(ppa5("gender_row") === 1 && ppa5("gender").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "gender", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("GENDER"))
            .withColumn("localvalue", ppa5("gender"))
    temp_patientdetail3 = temp_patientdetail3.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")

    var temp_patientdetail4 = ppa5.filter(ppa5("race_row") === 1 && ppa5("race").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "race", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("RACE"))
            .withColumn("localvalue", ppa5("race"))
    temp_patientdetail4 = temp_patientdetail4.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")

    var temp_patientdetail5 = ppa5.filter(ppa5("ethnicity_row") === 1 && ppa5("ethnicity").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "ethnicity", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("ETHNICITY"))
            .withColumn("localvalue", ppa5("ethnicity"))
    temp_patientdetail5 = temp_patientdetail5.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")

    var temp_patientdetail6 = ppa5.filter(ppa5("status_row") === 1 && ppa5("death_ind").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "death_ind", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("DECEASED"))
            .withColumn("localvalue", ppa5("death_ind"))
    temp_patientdetail6 = temp_patientdetail6.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")


    var temp_patientdetail7 = ppa5.filter(ppa5("ethnic_grp_row") === 1 && ppa5("ethnic_grp").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "ethnic_grp", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("ETHNIC_GROUP"))
            .withColumn("localvalue", ppa5("ethnic_grp"))
    temp_patientdetail7 = temp_patientdetail7.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")


    var temp_patientdetail8 = ppa5.filter(ppa5("language_row") === 1 && ppa5("language").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "language", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("LANGUAGE"))
            .withColumn("localvalue", ppa5("language"))
    temp_patientdetail8 = temp_patientdetail8.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")


    var temp_patientdetail9 = ppa5.filter(ppa5("marital_row") === 1 && ppa5("marital").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "marital", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("MARITAL"))
            .withColumn("localvalue", ppa5("marital"))
    temp_patientdetail9 = temp_patientdetail9.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")



    var temp_patientdetail10 = ppa5.filter(ppa5("middle_row") === 1 && ppa5("middle_name").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "middle_name", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("MIDDLE_NAME"))
            .withColumn("localvalue", ppa5("middle_name"))
    temp_patientdetail10 = temp_patientdetail10.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")



    var temp_patientdetail11 = ppa5.filter(ppa5("religion_row") === 1 && ppa5("religion_cd").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "religion_cd", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("RELIGION"))
            .withColumn("localvalue", ppa5("religion_cd"))
    temp_patientdetail11 = temp_patientdetail11.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")


    var temp_patientdetail12 = ppa5.filter(ppa5("zip_row") === 1 && ppa5("zipcode").isNotNull).select("groupid", "datasrc", "patientid", "client_ds_id", "update_date", "ZIPCODE", "HGPID", "GRP_MPI", "FACILITYID","ENCOUNTERID", "PATIENTDETAILQUAL")
            .withColumn("PATDETAIL_TIMESTAMP", ppa5("update_date"))
            .withColumn("patientdetailtype", lit("ZIPCODE"))
            .withColumn("localvalue", ppa5("ZIPCODE"))
    temp_patientdetail12 = temp_patientdetail12.select("GROUPID", "DATASRC", "FACILITYID", "PATIENTID", "ENCOUNTERID", "PATIENTDETAILTYPE", "PATIENTDETAILQUAL", "LOCALVALUE", "PATDETAIL_TIMESTAMP", "CLIENT_DS_ID","HGPID","GRP_MPI")

    appendGenerated(temp_patientdetail1,dateStr, "CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail2,dateStr, "CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail3,dateStr, "CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail4,dateStr, "CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail5, dateStr,"CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail6, dateStr,"CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail7,dateStr, "CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail8,dateStr,"CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail9,dateStr, "CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail10,dateStr, "CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail11, dateStr,"CR2_TEMP_PATIENT_DETAILS",cfg)
    appendGenerated(temp_patientdetail12,dateStr, "CR2_TEMP_PATIENT_DETAILS",cfg)

    var temp_patient_id1 = ppa5.filter((ppa5("ssn_row") === 1).and(validateSSN(ppa5, "ssn") === "Y").and(ppa5("active_id_flag") === "1"))
            .withColumn("IDTYPE", lit("SSN"))
            .withColumn("IDVALUE", ppa5("ssn"))
    temp_patient_id1 = temp_patient_id1.select("GROUPID","PATIENTID", "DATASRC", "IDTYPE", "IDVALUE","CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE");

    var temp_patient_id2 = ppa5.filter((ppa5("empi_row") === 1).and(ppa5("empi").isNotNull).and(ppa5("active_id_flag") === "1"))
            .withColumn("IDTYPE", lit("EMPI"))
            .withColumn("IDVALUE", ppa5("empi"))
    temp_patient_id2 = temp_patient_id2.select("GROUPID","PATIENTID", "DATASRC", "IDTYPE", "IDVALUE","CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE");

    var temp_patient_id3 = ppa5.filter((ppa5("mrn_row") === 1).and(ppa5("mrn").isNotNull).and(ppa5("active_id_flag") === "1"))
            .withColumn("IDTYPE", ppa5("MRN"))
            .withColumn("IDVALUE", lit("CMRN"))
    temp_patient_id3 = temp_patient_id3.select("GROUPID","PATIENTID", "DATASRC", "IDTYPE", "IDVALUE","CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE");

    var temp_patient_id4 = ppa5.filter((ppa5("rmrn_row") === 1).and(ppa5("RMRN").isNotNull).and(ppa5("active_id_flag") === "1"))
            .withColumn("IDTYPE", lit("RMRN"))
            .withColumn("IDVALUE", ppa5("RMRN"))
    temp_patient_id4 = temp_patient_id4.select("GROUPID","PATIENTID", "DATASRC", "IDTYPE", "IDVALUE","CLIENT_DS_ID", "HGPID", "GRP_MPI", "ID_SUBTYPE");

    appendGenerated(temp_patient_id1,dateStr, "CR2_TEMP_PATIENT_ID",cfg)
    appendGenerated(temp_patient_id2,dateStr, "CR2_TEMP_PATIENT_ID",cfg)
    appendGenerated(temp_patient_id3,dateStr,"CR2_TEMP_PATIENT_ID",cfg)
    appendGenerated(temp_patient_id4, dateStr,"CR2_TEMP_PATIENT_ID",cfg)

}





def mergeAllTempDetails(sqlContext:SQLContext, dateStr:String, cfg: Map[String,String]) = {
    var s = readGenerated(sqlContext, dateStr,"AS_TEMP_PATIENT", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENT", cfg)
    s = readGenerated(sqlContext, dateStr,"CR2_TEMP_PATIENT", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENT", cfg)
    s = readGenerated(sqlContext, dateStr,"EP2_TEMP_PATIENT", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENT", cfg)

    s = readGenerated(sqlContext, dateStr,"AS_TEMP_PATIENT_ID", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENTID", cfg)
    s = readGenerated(sqlContext, dateStr,"CR2_TEMP_PATIENT_ID", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENTID", cfg)
    s = readGenerated(sqlContext, dateStr,"EP2_TEMP_PATIENT_ID", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENTID", cfg)

    s = readGenerated(sqlContext, dateStr,"AS_TEMP_PATIENT_DETAILS", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENT_DETAILS", cfg)
    s = readGenerated(sqlContext, dateStr,"CR2_TEMP_PATIENT_DETAILS", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENT_DETAILS", cfg)
    s = readGenerated(sqlContext,dateStr, "EP2_TEMP_PATIENT_DETAILS", cfg)
    appendGenerated(s, dateStr,"CONSOLIDATED_TEMP_PATIENT_DETAILS", cfg)
}



def rxorder_patient_summary(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {


    var pat = readGenerated(sqlContext, dateStr,"CONSOLIDATED_TEMP_PATIENT", cfg)
    var dsp = readData(sqlContext, "ZCM_DATASRC_PRIORITY", cfg)
    var j1 = pat.join(dsp, Seq("GROUPID", "CLIENT_DS_ID", "DATASRC"), "left_outer")
    var d = j1.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID").agg(min("DATEOFBIRTH").as("DATEOFBIRTH"),max("DATEOFDEATH").as("DATEOFDEATH"),max("MEDICALRECORDNUMBER").as("MEDICALRECORDNUMBER"),max("INACTIVE_FLAG").as("INACTIVE_FLAG"))

    var patd = readGenerated(sqlContext,dateStr, "CONSOLIDATED_TEMP_PATIENT_DETAILS", cfg)


    var gender = patd.filter("PATIENTDETAILTYPE == 'GENDER'")
    var map_gender = readData(sqlContext, "MAP_GENDER", cfg).withColumnRenamed("CUI", "MG_CUI").withColumnRenamed("GROUPID", "MG_GROUPID")
    gender=gender.join(map_gender, gender("LOCALVALUE")===map_gender("MNEMONIC") and gender("GROUPID")===map_gender("MG_GROUPID"), "left_outer" ).drop("MG_GROUPID")
    gender=gender.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(gender("LOCALVALUE")).as("GENDER"),max(gender("MG_CUI")).as("GENDER_CUI"))
    d = d.join(gender, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")


    var race = patd.filter("PATIENTDETAILTYPE == 'RACE'")
    var map_race = readData(sqlContext, "MAP_RACE", cfg).withColumnRenamed("CUI", "MR_CUI").withColumnRenamed("GROUPID", "MR_GROUPID")
    race=race.join(map_race, race("LOCALVALUE")===map_race("MNEMONIC") and race("GROUPID")===map_race("MR_GROUPID"), "left_outer" ).drop("MR_GROUPID")
    race=race.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(race("LOCALVALUE")).as("RACE"),max(race("MR_CUI")).as("RACE_CUI"))

    d = d.join(race, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")


    var firstname = patd.filter("PATIENTDETAILTYPE == 'FIRST_NAME'")
    firstname=firstname.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(firstname("LOCALVALUE")).as("FIRST_NAME"))

    d = d.join(firstname, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")

    var religion = patd.filter("PATIENTDETAILTYPE == 'RELIGION'")
    religion=religion.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(religion("LOCALVALUE")).as("RELIGION"))

    d = d.join(religion, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")

    var lastname = patd.filter("PATIENTDETAILTYPE == 'LAST_NAME'")
    lastname=lastname.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(lastname("LOCALVALUE")).as("LAST_NAME"))
    d = d.join(lastname, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")


    var ethnicity = patd.filter("PATIENTDETAILTYPE == 'ETHNICITY'")
    var map_ethnicity = readData(sqlContext, "MAP_ETHNICITY", cfg).withColumnRenamed("CUI", "ME_CUI").withColumnRenamed("GROUPID", "ME_GROUPID")
    ethnicity=ethnicity.join(map_ethnicity, ethnicity("LOCALVALUE")===map_ethnicity("MNEMONIC") and ethnicity("GROUPID")===map_ethnicity("ME_GROUPID"), "left_outer" ).drop("ME_GROUPID")
    ethnicity=ethnicity.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(ethnicity("LOCALVALUE")).as("ETHNICITY"),max(ethnicity("ME_CUI")).as("ETHNIC_CUI"))

    d = d.join(ethnicity, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")


    var zipcode = patd.filter("PATIENTDETAILTYPE == 'ZIPCODE'")
    zipcode=zipcode.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(zipcode("LOCALVALUE")).as("ZIPCODE"))

    d = d.join(zipcode, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")


    var deceased = patd.filter("PATIENTDETAILTYPE == 'DECEASED'")
    var map_deceased = readData(sqlContext, "MAP_DECEASED_INDICATOR", cfg).withColumnRenamed("CUI", "MD_CUI").withColumnRenamed("GROUPID", "MD_GROUPID")
    deceased=deceased.join(map_deceased, when(deceased("LOCALVALUE").isNull, "DEFAULT")===map_deceased("MNEMONIC") and deceased("GROUPID")===map_deceased("MD_GROUPID"), "left_outer" ).drop("MD_GROUPID")
    deceased=deceased.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")

            .agg(max(deceased("LOCALVALUE")).as("DECEASED"),max(deceased("MD_CUI")).as("DECEASED_CUI"))

    d = d.join(deceased, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")

    var language = patd.filter("PATIENTDETAILTYPE == 'LANGUAGE'")
    var map_language = readData(sqlContext, "MAP_LANGUAGE", cfg).withColumnRenamed("CUI", "ML_CUI").withColumnRenamed("GROUPID", "ML_GROUPID")
    language=language.join(map_language, language("LOCALVALUE")===map_language("MNEMONIC") and language("GROUPID")===map_language("ML_GROUPID"), "left_outer" ).drop("ML_GROUPID")
    language=language.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(language("LOCALVALUE")).as("LANGUAGE"),max(language("ML_CUI")).as("LANGUAGE_CUI"))

    d = d.join(language, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")

    var marital = patd.filter("PATIENTDETAILTYPE == 'MARITAL'")
    var map_marital = readData(sqlContext, "MAP_MARITAL_STATUS", cfg).withColumnRenamed("CUI", "M_CUI").withColumnRenamed("GROUPID", "M_GROUPID")
    marital=marital.join(map_marital, marital("LOCALVALUE")===map_marital("LOCAL_CODE") and marital("GROUPID")===map_marital("M_GROUPID"), "left_outer" ).drop("M_GROUPID")
    marital=marital.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID")
            .agg(max(marital("LOCAL_CODE")).as("MARITAL_STATUS"),max(marital("M_CUI")).as("MARITAL_STATUS_CUI"))

    d = d.join(marital, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")


    var patid = readGenerated(sqlContext, dateStr,"CONSOLIDATED_TEMP_PATIENTID", cfg).filter("IDTYPE == 'SSN'")
    patid=patid.groupBy("GROUPID", "CLIENT_DS_ID", "PATIENTID").agg(count(regexp_replace(patid("IDVALUE"), "-", "")).as("COUNT1"), min(regexp_replace(patid("IDVALUE"), "-", "")).as("SSN")).filter("COUNT1 == 1").drop("COUNT1")

    d = d.join(patid, Seq("GROUPID", "CLIENT_DS_ID", "PATIENTID"), "left_outer")


    var ref_death_index = readData(sqlContext, "REF_DEATH_INDEX", cfg).withColumnRenamed("FIRST_NAME", "RDI_FIRST_NAME").withColumnRenamed("LAST_NAME", "RDI_LAST_NAME").withColumnRenamed("DATE_OF_DEATH", "RDI_DATE_OF_DEATH").withColumnRenamed("SSN", "RDI_SSN")
    ref_death_index=ref_death_index.withColumn("DTB", from_unixtime(unix_timestamp(ref_death_index("DATE_OF_BIRTH"), "MMddyyyy"), "yyyy-MM-dd"))

    var d1 = d.join(ref_death_index, d("DATEOFBIRTH").contains(ref_death_index("DTB")) and d("SSN")===(ref_death_index("RDI_SSN")), "left_outer")

    var ps = d1.withColumn("YOB", from_unixtime(unix_timestamp(d1("DATEOFBIRTH").substr(1,10), "yyyy-MM-dd"), "yyyy"))
    ps=ps   .withColumn("MOB", from_unixtime(unix_timestamp(d1("DATEOFBIRTH").substr(1,10), "yyyy-MM-dd"), "yyyyMM"))
    ps=ps   .withColumn("DOB", from_unixtime(unix_timestamp(d1("DATEOFBIRTH").substr(1,10), "yyyy-MM-dd"), "yyyyMMdd"))

    //       .withColumn("DATE_OF_DEATH", when(ref_death_index("DATE_OF_DEATH").substr(3,2) === "00", last_day(unix_timestamp(concat(from_unixtime(unix_timestamp(ref_death_index("DATE_OF_BIRTH"), "MMddyyyy"), "yyyyMM"), lit("01")))))
    //                                  .when(ref_death_index("DATE_OF_DEATH").isNotNull,  ref_death_index("DATE_OF_DEATH")).otherwise(d("DATE_OF_DEATH")))
    ps=ps  .withColumn("DATE_OF_DEATH", when(ps("RDI_DATE_OF_DEATH").isNotNull,  ps("RDI_DATE_OF_DEATH")).otherwise(d("DATEOFBIRTH")))
    ps=ps  .withColumn("LOCAL_DOD", ps("DATEOFBIRTH"))
    ps=ps  .withColumn("SSDI_DOD", when(ps("DATE_OF_DEATH").isNotNull, "Y").otherwise("N"))
    ps=ps  .withColumn("SSDI_NAME_MATCH", when((upper(trim(ps("RDI_FIRST_NAME"))) === upper(trim(ps("FIRST_NAME")))) and (upper(trim(ps("RDI_LAST_NAME"))) === upper(trim(ps("LAST_NAME")))), "Y").otherwise("N"))
    ps=ps  .withColumn("LOCAL_GENDER", ps("GENDER"))
    ps=ps  .withColumn("LOCAL_RACE", ps("RACE"))
    ps=ps  .withColumn("LOCAL_ETHNICITY", ps("ETHNICITY"))
    ps=ps  .withColumn("LOCAL_ZIPCODE", ps("ZIPCODE"))
    ps=ps  .withColumn("LOCAL_LANGUAGE", ps("LANGUAGE"))
    ps=ps  .withColumn("LOCAL_MARITAL_STATUS", ps("MARITAL_STATUS"))
    ps=ps.withColumn("MAPPED_ZIPCODE",
        when((trim(ps("ZIPCODE")).isNull) or (trim(ps("ZIPCODE"))==="NULL"), "CH999999")
                .when(trim(ps("ZIPCODE"))==="00000", "CH999990")
                .when(trim(ps("ZIPCODE")).contains("-"), trim(ps("ZIPCODE")).substr(0,5))
                .when((trim(ps("ZIPCODE")).between("00601", "99950")===false), "CH999990")
                .when(trim(ps("ZIPCODE")).rlike("[:space:]*(([A-Z][0-9]|[a-z][0-9]){3}[:space:]*)"), trim(upper(ps("ZIPCODE"))))
                .when((regexp_replace(trim(ps("ZIPCODE")), "O", "0").rlike("[:space:]*([0-9]{5}([- ]?[0-9]{4})?)[:space:]*") === false), "CH999990")
                .otherwise(regexp_replace(regexp_replace(trim(ps("ZIPCODE")), "O", "0"), "[:space:]*([0-9]{5})[- ]?[0-9]{4}[:space:]*", ""))
    )

            .withColumn("LOCAL_DEATH_IND", d("DECEASED"))

            .withColumn("MAPPED_DEATH_IND", when(d("DECEASED_CUI").isNull, "CH999999").otherwise(d("DECEASED_CUI")))
            .withColumn("MAPPED_GENDER", when(d("GENDER").isNull and d("GENDER_CUI").isNull, "CH999999")
                    .when(d("GENDER").isNotNull and d("GENDER_CUI").isNull, "CH999990").otherwise(d("GENDER_CUI")))

            .withColumn("MAPPED_ETHNICITY", when(d("ETHNICITY").isNull and d("ETHNIC_CUI").isNull, "CH999999")
                    .when(d("ETHNICITY").isNotNull and d("ETHNIC_CUI").isNull, "CH999990").otherwise(d("ETHNIC_CUI")))

            .withColumn("MAPPED_RACE", when(d("RACE").isNull and d("RACE_CUI").isNull, "CH999999")
                    .when(d("RACE").isNotNull and d("RACE_CUI").isNull, "CH999990").otherwise(d("RACE_CUI")))

            .withColumn("MAPPED_LANGUAGE", when(d("LANGUAGE").isNull and d("LANGUAGE_CUI").isNull, "CH999999")
                    .when(d("LANGUAGE").isNotNull and d("LANGUAGE_CUI").isNull, "CH999990").otherwise(d("LANGUAGE_CUI")))

            .withColumn("MAPPED_MARITAL_STATUS", when(d("MARITAL_STATUS").isNull and d("MARITAL_STATUS_CUI").isNull, "CH999999")
                    .when(d("MARITAL_STATUS").isNotNull and d("MARITAL_STATUS_CUI").isNull, "CH999990").otherwise(d("MARITAL_STATUS_CUI")))
    ps = ps.withColumn("INACTIVE_FLAG", when(ps("INACTIVE_FLAG") !== "NULL", ps("INACTIVE_FLAG")))
    var ps1 = ps.select("GROUPID", "CLIENT_DS_ID", "PATIENTID", "MEDICALRECORDNUMBER", "YOB", "MOB", "DOB", "LOCAL_GENDER",
        "LOCAL_ETHNICITY", "LOCAL_RACE", "LOCAL_ZIPCODE", "FIRST_NAME", "LAST_NAME", "MAPPED_GENDER",
        "MAPPED_ETHNICITY", "MAPPED_RACE", "MAPPED_ZIPCODE", "LOCAL_DEATH_IND", "MAPPED_DEATH_IND",
        "DATE_OF_DEATH", "LOCAL_DOD", "SSDI_DOD", "SSDI_NAME_MATCH","INACTIVE_FLAG", "LOCAL_LANGUAGE",
        "MAPPED_LANGUAGE", "LOCAL_MARITAL_STATUS", "MAPPED_MARITAL_STATUS", "RELIGION")
    writeGenerated(ps1,dateStr, "CONSOLIDATED_PATIENT_SUMMARY_WITHOUT_GRP_MPI", cfg)


}



def rxorder_patient_phi(sqlContext: SQLContext,dateStr:String, cfg: Map[String,String]) = {

    var patient_summary = readGenerated(sqlContext, dateStr,"CONSOLIDATED_PATIENT_SUMMARY_WITHOUT_GRP_MPI",cfg)
    var PATIENT_MPI = readData(sqlContext, "PATIENT_MPI", cfg).select("GROUPID","CLIENT_DS_ID","PATIENTID","GRP_MPI").withColumnRenamed("GRP_MPI", "CDR_GRP_MPI").distinct()
    var JOIN_PATIENT_SUMMARY = patient_summary.join(PATIENT_MPI, Seq("GROUPID","CLIENT_DS_ID","PATIENTID"), "left_outer")

    var ALLOBJECTIDS = readCommonData(sqlContext, "ALL_GROUPIDS", cfg).select("GROUPID")
    JOIN_PATIENT_SUMMARY = ALLOBJECTIDS.join(JOIN_PATIENT_SUMMARY, Seq("GROUPID"), "inner")

    var GRP_MPI_XREF_IOT = readCommonData(sqlContext, "GRP_MPI_XREF_IOT", cfg).select("GROUPID", "GRP_MPI", "NEW_ID").withColumnRenamed("GROUPID", "G_GROUPID").withColumnRenamed("GRP_MPI", "G_GRP_MPI").withColumnRenamed("NEW_ID", "G_NEW_ID")

    JOIN_PATIENT_SUMMARY = JOIN_PATIENT_SUMMARY.join(GRP_MPI_XREF_IOT, JOIN_PATIENT_SUMMARY("GROUPID")===GRP_MPI_XREF_IOT("G_GROUPID") and
            JOIN_PATIENT_SUMMARY("CDR_GRP_MPI")===GRP_MPI_XREF_IOT("G_GRP_MPI"), "left_outer")
    JOIN_PATIENT_SUMMARY = JOIN_PATIENT_SUMMARY.withColumn("DCDR_GRP_MPI", trim(JOIN_PATIENT_SUMMARY("G_NEW_ID")))

    var patient_summary_grp_mpi = readData(sqlContext, "PATIENT_SUMMARY_GRP_MPI", cfg) .withColumnRenamed("DOB", "DOB_PSGM").withColumnRenamed("FIRST_NAME", "FIRST_NAME_PSGM")
            .withColumnRenamed("LAST_NAME", "LAST_NAME_PSGM").withColumnRenamed("MAPPED_GENDER", "MAPPED_GENDER_PSGM").withColumnRenamed("GRP_MPI","CDR_GRP_MPI")
            .withColumnRenamed("MAPPED_ETHNICITY", "MAPPED_ETHNICITY_PSGM").withColumnRenamed("MAPPED_RACE", "MAPPED_RACE_PSGM").withColumnRenamed("MAPPED_ZIPCODE", "MAPPED_ZIPCODE_PSGM")

    var pat_sum_join = JOIN_PATIENT_SUMMARY.join(patient_summary_grp_mpi, Seq("GROUPID", "CDR_GRP_MPI"), "left_outer")

    pat_sum_join = pat_sum_join.withColumn("DOB", when(pat_sum_join("DOB_PSGM").isNotNull, pat_sum_join("DOB_PSGM")).otherwise(pat_sum_join("DOB")))
            .withColumn("FIRST_NAME", when(pat_sum_join("FIRST_NAME_PSGM").isNotNull, pat_sum_join("FIRST_NAME_PSGM")).otherwise(pat_sum_join("FIRST_NAME")))
            .withColumn("LAST_NAME", when(pat_sum_join("LAST_NAME_PSGM").isNotNull, pat_sum_join("LAST_NAME_PSGM")).otherwise(pat_sum_join("LAST_NAME")))
            .withColumn("MAPPED_GENDER", when(pat_sum_join("MAPPED_GENDER_PSGM").isNotNull, pat_sum_join("MAPPED_GENDER_PSGM")).otherwise(pat_sum_join("MAPPED_GENDER")))
            .withColumn("MAPPED_ETHNICITY", when(pat_sum_join("MAPPED_ETHNICITY_PSGM").isNotNull, pat_sum_join("MAPPED_ETHNICITY_PSGM")).otherwise(pat_sum_join("MAPPED_ETHNICITY")))
            .withColumn("MAPPED_RACE", when(pat_sum_join("MAPPED_RACE_PSGM").isNotNull, pat_sum_join("MAPPED_RACE_PSGM")).otherwise(pat_sum_join("MAPPED_RACE")))
            .withColumn("MAPPED_ZIPCODE", when(pat_sum_join("MAPPED_ZIPCODE_PSGM").isNotNull, pat_sum_join("MAPPED_ZIPCODE_PSGM")).otherwise(pat_sum_join("MAPPED_ZIPCODE")))

    var PATIENT_ID = readData(sqlContext, "PATIENT_ID", cfg).filter("IDTYPE='SSN'").select("GROUPID", "GRP_MPI", "IDVALUE")

    PATIENT_ID = PATIENT_ID.withColumn("SSN", regexp_replace( PATIENT_ID("IDVALUE"), "-", ""))
    val group = Window.partitionBy(PATIENT_ID("GROUPID"),PATIENT_ID("GRP_MPI")).orderBy(PATIENT_ID("SSN").desc)
    PATIENT_ID = PATIENT_ID.withColumn("ROWNMBR", row_number().over(group)).filter("ROWNMBR = 1").withColumnRenamed("GRP_MPI", "CDR_GRP_MPI")

    pat_sum_join = pat_sum_join.join(PATIENT_ID, Seq("GROUPID", "CDR_GRP_MPI"), "left_outer")

    pat_sum_join = pat_sum_join.withColumn("SSN", when(pat_sum_join("SSN").isNull, "NULL")
            .when(length(regexp_extract(pat_sum_join("SSN"), "[0-9]{9}", 0)) !== 9, "LEN")
            .when(pat_sum_join("SSN").substr(1,1) === "9", "DIGT")
            .when(pat_sum_join("SSN").substr(1,3).isin("000", "666"), "DIGT")
            .when(pat_sum_join("SSN").substr(4,2) === "00", "DIGT")
            .when(pat_sum_join("SSN").substr(6,4)=== "0000", "DIGT")
            .when(pat_sum_join("SSN").isin(
                "111111111","123456789","222222222","555555555","111111112","333333333","444444444","111223333","111111113","012345678",
                "444444441","123121234","123123123","111111114","123456788","234567890","099999999","001010001","222334444","111111115",
                "234567891","010101010","121212121","200202000","111111110","011111111","111111119","333445555","100101000","111112222",
                "444556666","001111111","111222333","444444442","111221111","101010101","123456780","123456799","111111234","199999999",
                "111111116","777777777","123456787","123457890","111111118","456789123","123451234","345678901","555555503","111223344",
                "001234567","123456798","333221111","654098765","555555556","122222222","222222223","222222221","111111101","555555551",
                "121121212","321654987","111102000","555555501","555555502","555667777","111111117","111101000","345678910","111103000",
                "112233445","009999999","123999999","123111111","123456781","111223334","098765432","111111102","111111122","111224444",
                "200202001","211111111","123345678","345678912","123455678","111111103","111111000","888888888","555555504"
            ), "SAME")
            .otherwise(pat_sum_join("SSN"))
    )
    pat_sum_join = pat_sum_join.withColumn("SSN", regexp_replace(pat_sum_join("SSN"), "LEN|DIGT|SAME", "NULL"))
    pat_sum_join = pat_sum_join.withColumn("SSN", when(pat_sum_join("SSN") !== "NULL", pat_sum_join("SSN").substr(1, 9)))

    var FINAL_PATIENT_SUMMARY = pat_sum_join.select("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI", "GROUPID", "DOB", "FIRST_NAME",
        "LAST_NAME", "MAPPED_GENDER", "MAPPED_ETHNICITY", "MAPPED_RACE", "MAPPED_ZIPCODE", "SSN").distinct()

    writeGenerated(FINAL_PATIENT_SUMMARY,dateStr, "RXORDER_PHI", cfg)
}


def runRxOrderPhi(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {
    //        println("Running processASTempPatientDetails...")
    //        processASTempPatientDetails(sqlContext,dateStr, cfg)
    //println ("Running EpicV2 processEPTempPatientDetails...")
    //processEPTempPatientDetails(sqlContext,dateStr, cfg)
    //        println ("Merging all TempDetails..")
    //        processCRTempPatientDetails(sqlContext,dateStr, cfg)
    //        mergeAllTempDetails(sqlContext,dateStr, cfg)
    //        println ("Running rxorder_patient_summary..")
    //        rxorder_patient_summary(sqlContext,dateStr,cfg)
    //        println ("Running rxorder_patient_phi..")
    //        rxorder_patient_phi(sqlContext, dateStr, cfg)
}



def processASRxOrder(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String], entirySrc:EntitySource) = {
    writeGenerated(Toolkit.build(new RxordersandprescriptionsErx(cfg)).distinct().withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID", "CLIENT_DS_ID"), dateStr, "ASRXORDER", cfg)
}

def processEP2RxOrder(sqlContext: SQLContext, dateStr:String, entitySrc3:EntitySource, cfg: Map[String,String])= {
    writeGenerated(Toolkit.build(new RxordersandprescriptionsMedorders(cfg)).distinct().withColumnRenamed("GRPID1", "GROUPID").withColumnRenamed("CLSID1", "CLIENT_DS_ID"), dateStr, "EPRXORDER", cfg)
}





def find_minimum(column1:Column, column2:Column, column3:Column) = {
    when(column1 < column2 && column1 < column3, column1)
            .when(column2 < column1 && column2 < column3, column2)
            .when(column3 < column1 && column3 < column2, column3)
}

def clinicalEncounterCernverV2(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {

    var ENCOUNTER = readTable(sqlContext, dateStr, "ENCOUNTER",cfg).select( "GROUPID", "CLIENT_DS_ID", "ACTIVE_IND", "CONTRIBUTOR_SYSTEM_CD", "ENCNTR_ID", "DISCH_DT_TM", "INPATIENT_ADMIT_DT_TM"
        , "ARRIVE_DT_TM", "REG_DT_TM", "UPDT_DT_TM", "PERSON_ID", "ENCNTR_TYPE_CD", "LOC_FACILITY_CD", "LOC_BUILDING_CD", "MED_SERVICE_CD", "ADMIT_SRC_CD",
        "DISCH_DISPOSITION_CD", "REASON_FOR_VISIT")
    ENCOUNTER = ENCOUNTER.withColumn("UPDT_DT_TM_enc", ENCOUNTER("UPDT_DT_TM"))
    ENCOUNTER = ENCOUNTER.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_enc").withColumnRenamed("GROUPID", "GROUPID_enc")

    var TEMP_INC = ENCOUNTER.withColumn("INP", when(date_format(from_unixtime(ENCOUNTER("INPATIENT_ADMIT_DT_TM").divide(1000)), "yyyy-MM-dd") === "2100-12-31", null)
            .when(date_format(from_unixtime(ENCOUNTER("INPATIENT_ADMIT_DT_TM").divide(1000)), "yyyy-MM-dd") === "1900-01-01", null).otherwise(ENCOUNTER("INPATIENT_ADMIT_DT_TM")))
            .withColumn("ARR", when(date_format(from_unixtime(ENCOUNTER("ARRIVE_DT_TM").divide(1000)), "yyyy-MM-dd") === "2100-12-31", null)
                    .when(date_format(from_unixtime(ENCOUNTER("ARRIVE_DT_TM").divide(1000)), "yyyy-MM-dd") === "1900-01-01", null).otherwise(ENCOUNTER("ARRIVE_DT_TM")))
            .withColumn("REG", when(date_format(from_unixtime(ENCOUNTER("REG_DT_TM").divide(1000)), "yyyy-MM-dd") === "2100-12-31", null)
                    .when(date_format(from_unixtime(ENCOUNTER("REG_DT_TM").divide(1000)), "yyyy-MM-dd") === "1900-01-01", null).otherwise(ENCOUNTER("REG_DT_TM")))

    var TEMP_ENC1 = TEMP_INC.select("REASON_FOR_VISIT", "ACTIVE_IND", "CONTRIBUTOR_SYSTEM_CD", "ENCNTR_ID", "DISCH_DT_TM", "INPATIENT_ADMIT_DT_TM", "ARRIVE_DT_TM",
        "REG_DT_TM", "UPDT_DT_TM", "PERSON_ID", "ENCNTR_TYPE_CD", "LOC_FACILITY_CD", "LOC_BUILDING_CD", "MED_SERVICE_CD",
        "ADMIT_SRC_CD", "DISCH_DISPOSITION_CD", "ARRIVE_DT_TM", "REG_DT_TM", "GROUPID_enc", "CLIENT_DS_ID_enc", "UPDT_DT_TM_enc")
            .withColumn("ADMITTIME", coalesce(TEMP_INC("INPATIENT_ADMIT_DT_TM"), TEMP_INC("ARRIVE_DT_TM"), TEMP_INC("REG_DT_TM")))

            .withColumn("ARRIVALTIME", find_minimum(coalesce(TEMP_INC("INPATIENT_ADMIT_DT_TM"), TEMP_INC("ARRIVE_DT_TM"), TEMP_INC("REG_DT_TM")),
                coalesce(TEMP_INC("REG_DT_TM"), TEMP_INC("INPATIENT_ADMIT_DT_TM"), TEMP_INC("ARRIVE_DT_TM")),
                coalesce(TEMP_INC("ARRIVE_DT_TM"), TEMP_INC("REG_DT_TM"), TEMP_INC("INPATIENT_ADMIT_DT_TM"))))
            .withColumn("INS_TIMESTAMP", when(TEMP_INC("ARRIVE_DT_TM") === TEMP_INC("REG_DT_TM"),null).otherwise(TEMP_INC("ARRIVE_DT_TM")))


    val group1 = Window.partitionBy(TEMP_ENC1("GROUPID_enc"),TEMP_ENC1("CLIENT_DS_ID_enc"),TEMP_ENC1("ENCNTR_ID")).orderBy(TEMP_ENC1("UPDT_DT_TM").desc)
    TEMP_ENC1 = TEMP_ENC1.withColumn("RESULT", row_number().over(group1))
    TEMP_ENC1 = TEMP_ENC1.filter(TEMP_ENC1("ENCNTR_ID").isNotNull.and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "472")
            .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "1276570")
            .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "1278011")
            .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "1278025")
            .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "19271182")
            .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "22257494")
            .and(TEMP_ENC1("CONTRIBUTOR_SYSTEM_CD") !== "32427669"))

    var TEMP_PATIENT = readGenerated(sqlContext, dateStr, "CR2_TEMP_PATIENT",cfg).withColumnRenamed("CLIENT_DS_ID","CLIENT_DS_ID_person").withColumnRenamed("GROUPID","GROUPID_person")

    var TEMP_ENC_PATIENT_JN = TEMP_PATIENT.join(TEMP_ENC1, TEMP_PATIENT("PATIENTID") === TEMP_ENC1("PERSON_ID") && TEMP_PATIENT("GROUPID_person") === TEMP_ENC1("GROUPID_enc") && TEMP_PATIENT("CLIENT_DS_ID_person") === TEMP_ENC1("CLIENT_DS_ID_enc"), "left_outer").drop("GROUPID_enc1")

    val group0 = Window.partitionBy(TEMP_ENC_PATIENT_JN("GROUPID_enc"),TEMP_ENC_PATIENT_JN("CLIENT_DS_ID_enc"),TEMP_ENC_PATIENT_JN("ENCNTR_ID")).orderBy(TEMP_ENC_PATIENT_JN("UPDT_DT_TM").desc)
    TEMP_ENC_PATIENT_JN = TEMP_ENC_PATIENT_JN.withColumn("RN", row_number().over(group0)).filter("RN == 1").drop("RN")


    var HUM_DRG = readTable(sqlContext, dateStr, "HUM_DRG",cfg).select("CLIENT_DS_ID", "GROUPID", "NOMENCLATURE_ID", "RISK_OF_MORTALITY_CD", "SEVERITY_OF_ILLNESS_CD", "DRG_PRIORITY", "UPDT_DT_TM", "ACTIVE_IND", "ENCNTR_ID", "PERSON_ID")
    var HUM_DRG_1 = HUM_DRG.filter(HUM_DRG("ACTIVE_IND") === "1").withColumnRenamed("NOMENCLATURE_ID", "HD_NOMENCLATURE_ID")
    HUM_DRG_1 = HUM_DRG_1.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_hd").withColumnRenamed("GROUPID", "GROUPID_hd")

    var ZH_NOMENCLATURE = readDictionaryTable(sqlContext, "ZH_NOMENCLATURE",cfg).select("CLIENT_DS_ID", "GROUPID", "SOURCE_VOCABULARY_CD", "NOMENCLATURE_ID", "SOURCE_IDENTIFIER")
    var ZH_NOMENCLATURE_FILTER = ZH_NOMENCLATURE.filter(ZH_NOMENCLATURE("SOURCE_IDENTIFIER").isNotNull)
    ZH_NOMENCLATURE_FILTER = ZH_NOMENCLATURE_FILTER.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_zmc").withColumnRenamed("GROUPID", "GROUPID_zmc")


    var mpv = readData(sqlContext,  "MAP_PREDICATE_VALUES", cfg)

    var LIST_ZAC_SOURCE_VACABULARY_CD = predicate_value_list(mpv, "ENCOUNTERS", "CLINICAL_ENCOUNTER", "ZH_APRDRG_CD", "SOURCE_VOCABULARY_CD", "ZACSOURCE_VOCABULARY_CD")

    var APRDRG = ZH_NOMENCLATURE_FILTER.join(LIST_ZAC_SOURCE_VACABULARY_CD, ZH_NOMENCLATURE_FILTER("SOURCE_VOCABULARY_CD") === LIST_ZAC_SOURCE_VACABULARY_CD("ZACSOURCE_VOCABULARY_CD") && ZH_NOMENCLATURE_FILTER("GROUPID_zmc") === LIST_ZAC_SOURCE_VACABULARY_CD("GROUPID") && ZH_NOMENCLATURE_FILTER("CLIENT_DS_ID_zmc") === LIST_ZAC_SOURCE_VACABULARY_CD("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")

    var APRDRG1 = APRDRG.filter(APRDRG("ZACSOURCE_VOCABULARY_CD").isNull)

    var APR_HUM_DRG = HUM_DRG_1.join(APRDRG1, HUM_DRG_1("HD_NOMENCLATURE_ID") === APRDRG1("NOMENCLATURE_ID") && HUM_DRG_1("GROUPID_hd") === APRDRG1("GROUPID_zmc") && HUM_DRG_1("CLIENT_DS_ID_hd") === APRDRG1("CLIENT_DS_ID_zmc"), "left_outer").drop("GROUPID_zmc").drop("CLIENT_DS_ID_zmc")



    var APR_HUM_DRG1 = APR_HUM_DRG.select("GROUPID_hd", "CLIENT_DS_ID_hd", "ENCNTR_ID", "PERSON_ID", "NOMENCLATURE_ID", "RISK_OF_MORTALITY_CD", "SEVERITY_OF_ILLNESS_CD", "DRG_PRIORITY", "SOURCE_IDENTIFIER", "UPDT_DT_TM")
            .withColumn("ENCNTR_ID", APR_HUM_DRG("ENCNTR_ID").substr(1, 5)).withColumnRenamed("NOMENCLATURE_ID", "APR_NOMENCLATURE_ID")
            .withColumnRenamed("RISK_OF_MORTALITY_CD", "APR_RISK_OF_MORTALITY_CD").withColumnRenamed("SEVERITY_OF_ILLNESS_CD", "APR_SEVERITY_OF_ILLNESS_CD")
            .withColumnRenamed("SOURCE_IDENTIFIER", "APRDRG").withColumnRenamed("UPDT_DT_TM", "APR_UPDT_DT_TM")

    val group3 = Window.partitionBy(APR_HUM_DRG1("GROUPID_hd"),APR_HUM_DRG1("CLIENT_DS_ID_hd"),APR_HUM_DRG1("ENCNTR_ID")).orderBy(APR_HUM_DRG1("DRG_PRIORITY").desc, APR_HUM_DRG1("APR_UPDT_DT_TM").desc)
    APR_HUM_DRG1 = APR_HUM_DRG1.withColumn("RESULT1", row_number().over(group3)).filter("RESULT1 == 1").drop("RESULT1")

    var MS_HUM_DRG1 = APR_HUM_DRG1.withColumnRenamed("ENCNTR_ID", "ENCNTR_ID1").withColumnRenamed("APR_NOMENCLATURE_ID", "MS_NOMENCLATURE_ID").withColumnRenamed("APR_RISK_OF_MORTALITY_CD","MS_RISK_OF_MORTALITY_CD")
            .withColumnRenamed("APR_SEVERITY_OF_ILLNESS_CD", "MS_SEVERITY_OF_ILLNESS_CD").withColumnRenamed("APRDRG", "MSDRG").withColumnRenamed("APR_UPDT_DT_TM", "MS_UPDT_DT_TM").withColumnRenamed("PERSON_ID","PERSON_ID2")
    APR_HUM_DRG1 = APR_HUM_DRG1.withColumnRenamed("PERSON_ID","PERSON_ID1").withColumnRenamed("GROUPID_hd","GROUPID_hd1").withColumnRenamed("CLIENT_DS_ID_hd","CLIENT_DS_ID_hd1")

    APR_HUM_DRG1 = APR_HUM_DRG1.withColumnRenamed("ENCNTR_ID", "ENCNTR_ID3")
    var HUM_DRG2 = APR_HUM_DRG1.join(MS_HUM_DRG1, APR_HUM_DRG1("PERSON_ID1")===  MS_HUM_DRG1("PERSON_ID2") &&  APR_HUM_DRG1("ENCNTR_ID3")===MS_HUM_DRG1("ENCNTR_ID1") && APR_HUM_DRG1("ENCNTR_ID3")===MS_HUM_DRG1("ENCNTR_ID1") &&  APR_HUM_DRG1("GROUPID_hd1")===MS_HUM_DRG1("GROUPID_hd")  &&  APR_HUM_DRG1("CLIENT_DS_ID_hd1")===MS_HUM_DRG1("CLIENT_DS_ID_hd") , "left_outer")
    var J4 = TEMP_ENC_PATIENT_JN.join(HUM_DRG2, TEMP_ENC_PATIENT_JN("PERSON_ID")===  HUM_DRG2("PERSON_ID1") && TEMP_ENC_PATIENT_JN("GROUPID_enc")===  HUM_DRG2("GROUPID_hd1") && TEMP_ENC_PATIENT_JN("CLIENT_DS_ID_enc")===  HUM_DRG2("CLIENT_DS_ID_hd1"), "left_outer")


    var RCV = readDictionaryTable(sqlContext, "ZH_CODE_VALUE",cfg)
    RCV=RCV.withColumn("FILE_ID", RCV("FILEID").cast("String")).select("GROUPID", "CLIENT_DS_ID",  "CODE_VALUE", "CDF_MEANING")
    RCV = RCV.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_rcv").withColumnRenamed("GROUPID", "GROUPID_rcv")

    var SCV = readDictionaryTable(sqlContext, "ZH_CODE_VALUE",cfg)
    SCV=SCV.withColumn("FILE_ID", SCV("FILEID").cast("String")).select("GROUPID", "CLIENT_DS_ID", "CODE_VALUE", "CDF_MEANING")
    SCV = SCV.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_scv").withColumnRenamed("GROUPID", "GROUPID_scv")

    var J5 = J4.join(RCV, J4("APR_RISK_OF_MORTALITY_CD") === RCV("CODE_VALUE") && J4("GROUPID_enc") === RCV("GROUPID_rcv") && J4("CLIENT_DS_ID_enc") === RCV("CLIENT_DS_ID_rcv"), "left_outer").drop("GROUPID_rcv").drop("CLIENT_DS_ID_rcv")
    var J6 = J5.join(SCV, J5("APR_SEVERITY_OF_ILLNESS_CD") === SCV("CODE_VALUE") && J5("GROUPID_enc") === SCV("GROUPID_scv") && J5("CLIENT_DS_ID_enc") === SCV("CLIENT_DS_ID_scv"), "left_outer").drop("GROUPID_scv").drop("CLIENT_DS_ID_scv")
    var J7 = J6.join(ZH_NOMENCLATURE_FILTER, J6("MS_NOMENCLATURE_ID") === ZH_NOMENCLATURE_FILTER("NOMENCLATURE_ID") && J6("GROUPID_enc") === ZH_NOMENCLATURE_FILTER("GROUPID_zmc") && J6("CLIENT_DS_ID_enc") === ZH_NOMENCLATURE_FILTER("CLIENT_DS_ID_zmc"), "left_outer").drop("GROUPID_zmc").drop("CLIENT_DS_ID_zmc")

    var ENC_ALIAS = readTable(sqlContext, dateStr,  "ENC_ALIAS",cfg).select("GROUPID", "CLIENT_DS_ID","ENCNTR_ALIAS_TYPE_CD", "ENCNTR_ID", "ALIAS", "UPDT_DT_TM")
    ENC_ALIAS = ENC_ALIAS.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_ea").withColumnRenamed("GROUPID", "GROUPID_ea")

    val group2 = Window.partitionBy(ENC_ALIAS("GROUPID_ea"),ENC_ALIAS("CLIENT_DS_ID_ea"),ENC_ALIAS("ENCNTR_ID")).orderBy(ENC_ALIAS("UPDT_DT_TM").desc)
    var ENC_ALIAS1 = ENC_ALIAS.withColumn("ROW_NUMBER", row_number().over(group2))
    ENC_ALIAS1 = ENC_ALIAS1.filter(ENC_ALIAS1("ROW_NUMBER") === 1).withColumnRenamed("ENCNTR_ID", "ENCNTR_ID1")

    var J8 = J7.join(ENC_ALIAS1, J7("ENCNTR_ID")===ENC_ALIAS1("ENCNTR_ID1") && J7("GROUPID_enc")===ENC_ALIAS1("GROUPID_ea") && J7("CLIENT_DS_ID_enc")===ENC_ALIAS1("CLIENT_DS_ID_ea"), "left_outer").drop("GROUPID_ea").drop("CLIENT_DS_ID_ea")

    var LIST_ZLDT_SOURCE_VOCABULARY_CD = predicate_value_list(mpv, "ENCOUNTERS", "CLINICAL_ENCOUNTER", "ZH_LCL_DRG_TYPE", "SOURCE_VOCABULARY_CD", "ZLDT_SOURCE_VOCABULARY")

    var RESULT_J1 = J8.join(LIST_ZLDT_SOURCE_VOCABULARY_CD, J8("SOURCE_VOCABULARY_CD") === LIST_ZLDT_SOURCE_VOCABULARY_CD("ZLDT_SOURCE_VOCABULARY") && J8("GROUPID_enc") === LIST_ZLDT_SOURCE_VOCABULARY_CD("GROUPID") && J8("CLIENT_DS_ID_enc") === LIST_ZLDT_SOURCE_VOCABULARY_CD("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    var LIST_ENCNTR_ALIAS_TYPE_CD = predicate_value_list(mpv, "ALT_ENC_ID", "CLINICALENCOUNTER", "ENC_ALIAS", "ENCNTR_ALIAS_TYPE_CD", "LST_ENCNTR_ALIAS_TYPE_CD")
    var RESULT_J2 = RESULT_J1.join(LIST_ENCNTR_ALIAS_TYPE_CD, RESULT_J1("ENCNTR_ALIAS_TYPE_CD") === LIST_ENCNTR_ALIAS_TYPE_CD("LST_ENCNTR_ALIAS_TYPE_CD") && RESULT_J1("GROUPID_enc") === LIST_ENCNTR_ALIAS_TYPE_CD("GROUPID") && RESULT_J1("CLIENT_DS_ID_enc") === LIST_ENCNTR_ALIAS_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    var LIST_MED_SVC_CD = predicate_value_list(mpv, "ENCOUNTER", "CLINICALENCOUNTER", "ENCOUNTER", "MED_SERVICE_CD", "LST_MED_SERVICE_CD")
    var RESULT_J3 = RESULT_J2.join(LIST_MED_SVC_CD, concat_ws("_", RESULT_J2("ENCNTR_TYPE_CD"), RESULT_J2("MED_SERVICE_CD")) === LIST_MED_SVC_CD("LST_MED_SERVICE_CD") && RESULT_J2("CLIENT_DS_ID_enc") ===  LIST_MED_SVC_CD("CLIENT_DS_ID")&& RESULT_J2("GROUPID_enc") ===  LIST_MED_SVC_CD("GROUPID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    // var RESULT_FE = RESULT_J3.filter((RESULT_J3("RESULT") === 1).and(RESULT_J3("ARRIVALTIME").isNotNull).and(RESULT_J3("ACTIVE_IND").notEqual("0")).and(RESULT_J3("REASON_FOR_VISIT").isNull.or(RESULT_J3("REASON_FOR_VISIT").notEqual("NO_SHOW") and RESULT_J3("REASON_FOR_VISIT").notEqual("CANCELLED"))).and((RESULT_J3("ENCNTR_ALIAS_TYPE_CD").isNull.or(RESULT_J3("LST_ENCNTR_ALIAS_TYPE_CD").isNull))))
    var RESULT_FE = RESULT_J3.filter((RESULT_J3("RESULT") === 1).and(RESULT_J3("ACTIVE_IND").notEqual("0")).and(RESULT_J3("REASON_FOR_VISIT").isNull.or(RESULT_J3("REASON_FOR_VISIT").notEqual("NO_SHOW") and RESULT_J3("REASON_FOR_VISIT").notEqual("CANCELLED"))).and((RESULT_J3("ENCNTR_ALIAS_TYPE_CD").isNull.or(RESULT_J3("LST_ENCNTR_ALIAS_TYPE_CD").isNull))))
    var RESULT = RESULT_FE.withColumn("DATASRC", lit("encounter"))
    RESULT_FE=RESULT.withColumnRenamed("PERSON_ID", "PATIENTID1").withColumnRenamed("ENCNTR_ID", "ENCOUNTERID").withColumnRenamed("DISCH_DT_TM", "DISCHARGETIME")
    RESULT_FE=RESULT_FE.withColumn("VISITID", RESULT_FE("ENCOUNTERID")).withColumnRenamed("APRDRG", "APRDRG_CD").withColumnRenamed("MSDRG", "LOCALDRG")
    RESULT_FE=RESULT_FE.withColumn("APRDRG_SOI", when(SCV("CDF_MEANING").isNull, "0").otherwise(SCV("CDF_MEANING")))
    RESULT_FE=RESULT_FE.withColumn("APRDRG_ROM", when(RCV("CDF_MEANING").isNull, "0").otherwise(RCV("CDF_MEANING")))
    RESULT_FE=RESULT_FE.withColumnRenamed("FACILITYID", "LOC_FACILITY_CD")
    RESULT_FE=RESULT_FE.withColumn("LOCALPATIENTTYPE", when(RESULT_FE("ENCNTR_TYPE_CD").isNull.or(RESULT_FE("ENCNTR_TYPE_CD") === "0"), null)
            .when(RESULT_FE("LST_MED_SERVICE_CD").isNotNull, concat_ws("_", concat_ws(".", RESULT_FE("CLIENT_DS_ID_person"),  RESULT_FE("ENCNTR_TYPE_CD")), RESULT_FE("MED_SERVICE_CD")))
            .otherwise(concat_ws(".", RESULT_FE("CLIENT_DS_ID_person"),  RESULT_FE("ENCNTR_TYPE_CD"))))

    RESULT_FE=RESULT_FE.withColumn("LOCALADMITSOURCE", when(RESULT_FE("ADMIT_SRC_CD").isNull.or(RESULT_FE("ADMIT_SRC_CD") === "0"), null).otherwise(concat_ws(".", RESULT_FE("CLIENT_DS_ID_person"), RESULT_FE("ADMIT_SRC_CD"))))
    RESULT_FE=RESULT_FE.withColumn("LOCALDISCHARGEDISPOSITION", when(RESULT_FE("DISCH_DISPOSITION_CD").isNull, null).otherwise(concat_ws(".", RESULT_FE("CLIENT_DS_ID_person"),
        RESULT_FE("DISCH_DISPOSITION_CD"))))
    RESULT_FE=RESULT_FE.withColumn("LOCALDRGTYPE", when(RESULT_FE("ZLDT_SOURCE_VOCABULARY").isNull, "MS-DRG")
            .when(RESULT_FE("ZLDT_SOURCE_VOCABULARY") === "1221", "DRG").otherwise(null))
    RESULT_FE=RESULT_FE.withColumn("ALT_ENCOUNTERID", when(RESULT_FE("ENCNTR_ALIAS_TYPE_CD").isNull, RESULT_FE("ALIAS")).otherwise(null))

    val group7 = Window.partitionBy(RESULT_FE("GROUPID_enc"),RESULT_FE("CLIENT_DS_ID_enc"),RESULT_FE("ENCOUNTERID")).orderBy(RESULT_FE("UPDT_DT_TM_enc").desc)
    RESULT_FE = RESULT_FE.withColumn("RN", row_number().over(group7)).filter("RN==1").drop("RN")

    RESULT_FE=RESULT_FE.withColumnRenamed("GROUPID_enc", "GROUPID").withColumnRenamed("CLIENT_DS_ID_enc", "CLIENT_DS_ID").select("GROUPID", "DATASRC", "CLIENT_DS_ID", "PATIENTID", "ENCOUNTERID", "ARRIVALTIME", "ADMITTIME",
        "DISCHARGETIME", "VISITID", "APRDRG_CD", "LOCALDRG", "APRDRG_SOI", "APRDRG_ROM", "LOCALPATIENTTYPE", "LOCALDISCHARGEDISPOSITION", "ALT_ENCOUNTERID")

    writeGenerated(RESULT_FE, dateStr, "CR2CLINICALENCOUNTER",cfg)
}





def safe_to_number(value:Column)={
    try {
        value.cast("Integer")
        value
    }
    catch {
        case ex:Exception => null
    }
}



def predicate_value_listlike(p_mpv: DataFrame, dataSrc: String, entity: String, table: String, column: String, colName: String): DataFrame = {
    var mpv1 = p_mpv.filter(p_mpv("DATA_SRC").equalTo(dataSrc).and(p_mpv("ENTITY").rlike(entity)).and(p_mpv("TABLE_NAME").equalTo(table)).and(p_mpv("COLUMN_NAME").equalTo(column)))
    mpv1=mpv1.withColumn(colName, mpv1("COLUMN_VALUE"))
    mpv1.select("GROUPID", "CLIENT_DS_ID", colName).distinct()
}



def safe_to_number_with_default(value:Column, default:Object)={
    when(safe_to_number(value).isNull, lit(default)).otherwise(value)
}


def rxorder_cernerv2(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {

    var O_MS = readTable(sqlContext, dateStr, "ORDERS",cfg).select("GROUPID", "CLIENT_DS_ID", "ORDER_DETAIL_DISPLAY_LINE", "ENCNTR_ID", "PERSON_ID", "ACTIVE_IND", "CATALOG_CD", "ACTIVITY_TYPE_CD", "ORDER_ID", "UPDT_DT_TM", "ORIG_ORD_AS_FLAG", "ORIG_ORDER_DT_TM")
    O_MS = O_MS.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_oms").withColumnRenamed("GROUPID", "GROUPID_oms")

    var ZH_CATALOG_CD_MS =readDictionaryTable(sqlContext, "ZH_CODE_VALUE",cfg).withColumnRenamed("DISPLAY", "DISPLAY_zh")
    ZH_CATALOG_CD_MS=ZH_CATALOG_CD_MS.select("GROUPID", "CLIENT_DS_ID", "CODE_VALUE", "CODE_SET", "DISPLAY_zh")
    ZH_CATALOG_CD_MS = ZH_CATALOG_CD_MS.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_zms").withColumnRenamed("GROUPID", "GROUPID_zms")

    var OJ1 = O_MS.join(ZH_CATALOG_CD_MS, O_MS("GROUPID_oms") === ZH_CATALOG_CD_MS("GROUPID_zms") && O_MS("CLIENT_DS_ID_oms") === ZH_CATALOG_CD_MS("CLIENT_DS_ID_zms")  && O_MS("CATALOG_CD") === ZH_CATALOG_CD_MS("CODE_VALUE")  && lit("200") === ZH_CATALOG_CD_MS("CODE_SET"), "left_outer").drop("CLIENT_DS_ID_zms").drop("GROUPID_zms")
    var mpv = readData(sqlContext, "MAP_PREDICATE_VALUES", cfg)
    var LIST_ACTIVITY_TYPE_CD = predicate_value_list(mpv, "ORDERS", "RX", "ORDERS", "ACTIVITY_TYPE_CD", "ACTIVITY_TYPE_CD_VAL")

    var OJ2 = OJ1.join(LIST_ACTIVITY_TYPE_CD, OJ1("ACTIVITY_TYPE_CD") === LIST_ACTIVITY_TYPE_CD("ACTIVITY_TYPE_CD_VAL") && OJ1("GROUPID_oms") === LIST_ACTIVITY_TYPE_CD("GROUPID") && OJ1("CLIENT_DS_ID_oms") === LIST_ACTIVITY_TYPE_CD("CLIENT_DS_ID"), "inner").drop("CLIENT_DS_ID").drop("GROUPID")
    val group1 = Window.partitionBy(OJ2("ORDER_ID"), OJ2("GROUPID_oms"), OJ2("CLIENT_DS_ID_oms")).orderBy(OJ2("UPDT_DT_TM").desc)
    var OJ3 = OJ2.withColumn("ROW_NUMBER", row_number().over(group1)) //dedupe orders
    OJ3 = OJ3.filter("ROW_NUMBER == 1")
    var OJ4 = OJ3.filter(OJ3("ACTIVITY_TYPE_CD_VAL").isNotNull.and((OJ3("ORIG_ORD_AS_FLAG") === "0" || OJ3("ORIG_ORD_AS_FLAG") === "1" || OJ3("ORIG_ORD_AS_FLAG") === "4" || OJ3("ORIG_ORD_AS_FLAG") === "5")))
    var OA = readTable(sqlContext,dateStr,  "ORDER_ACTION",cfg)
    OA=OA.withColumnRenamed("ORDER_ID", "OA_ORDER_ID").select("GROUPID",  "CLIENT_DS_ID", "ACTION_TYPE_CD", "OA_ORDER_ID", "ACTION_SEQUENCE", "UPDT_DT_TM", "ORDER_DT_TM", "ORDER_PROVIDER_ID", "ORDER_STATUS_CD", "STOP_TYPE_CD")
    OA = OA.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_oa").withColumnRenamed("GROUPID", "GROUPID_oa")

    var LIST_JOIN_ACTION_TYPE_CD = predicate_value_list(mpv, "JOINCRITERIA", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD", "LIST_JOIN_ACTION_TYPE_CD_VAL")
    var LIST_DELETE_ORDER_ID = predicate_value_list(mpv, "DELETE_ORDER_CD", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD", "DELETE_ORDER_CD_VAL")

    var OA1 = OA.join(LIST_JOIN_ACTION_TYPE_CD, OA("CLIENT_DS_ID_oa") === LIST_JOIN_ACTION_TYPE_CD("CLIENT_DS_ID") && OA("GROUPID_oa") === LIST_JOIN_ACTION_TYPE_CD("GROUPID") && OA("ACTION_TYPE_CD") === LIST_JOIN_ACTION_TYPE_CD("LIST_JOIN_ACTION_TYPE_CD_VAL") && OA("ACTION_TYPE_CD") === LIST_JOIN_ACTION_TYPE_CD("LIST_JOIN_ACTION_TYPE_CD_VAL") && OA("ACTION_TYPE_CD") === LIST_JOIN_ACTION_TYPE_CD("LIST_JOIN_ACTION_TYPE_CD_VAL"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OA2 = OA1.join(LIST_DELETE_ORDER_ID, OA("CLIENT_DS_ID_oa") === LIST_JOIN_ACTION_TYPE_CD("CLIENT_DS_ID") && OA("GROUPID_oa") === LIST_JOIN_ACTION_TYPE_CD("GROUPID") && OA("ACTION_TYPE_CD") === LIST_DELETE_ORDER_ID("DELETE_ORDER_CD_VAL"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    val group2 = Window.partitionBy(OA2("OA_ORDER_ID"), OA2("ACTION_SEQUENCE"),OA2("GROUPID_oa"), OA2("CLIENT_DS_ID_oa")).orderBy(OA2("UPDT_DT_TM").desc)
    var OA3 = OA2.withColumn("ROW_NUMBER1", row_number().over(group2))
    OA3 = OA3.filter(OA3("ROW_NUMBER1")===1)
    var OA4 = OA3.filter(OA3("LIST_JOIN_ACTION_TYPE_CD_VAL").isNotNull)

    val group3 = Window.partitionBy(OA4("OA_ORDER_ID"), OA4("GROUPID_oa"), OA4("CLIENT_DS_ID_oa")).orderBy(OA4("ACTION_SEQUENCE").desc)
    var OA5 = OA4.withColumn("LATEST_ACTION_SEQUENCE_FLG", row_number().over(group3))
    // var OA52 = OA5.filter(OA5("LATEST_ACTION_SEQUENCE_FLG") === 1)
    var OA7 = OA5.withColumn("DELETE_FLG", when(OA5("DELETE_ORDER_CD_VAL").isNotNull, "Y").otherwise("N"))
    var OOAJ1 = OJ4.join(OA7, OJ4("ORDER_ID")===OA7("OA_ORDER_ID") &&  OJ4("GROUPID_oms")===OA7("GROUPID_oa") &&  OJ4("CLIENT_DS_ID_oms")===OA7("CLIENT_DS_ID_oa"), "left_outer").drop("CLIENT_DS_ID_oa").drop("GROUPID_oa")

    var OD = readTable(sqlContext, dateStr, "ORDER_DETAIL",cfg)
    OD=OD.withColumnRenamed("ORDER_ID","OD_ORDER_ID").select("GROUPID", "CLIENT_DS_ID",  "OE_FIELD_VALUE", "OD_ORDER_ID", "ACTION_SEQUENCE", "OE_FIELD_ID", "UPDT_DT_TM", "OE_FIELD_DISPLAY_VALUE", "OE_FIELD_DT_TM_VALUE")
    OD = OD.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_od").withColumnRenamed("GROUPID", "GROUPID_od")

    var ZH_FIELD_VALUE = readDictionaryTable(sqlContext, "ZH_CODE_VALUE",cfg)
    ZH_FIELD_VALUE=ZH_FIELD_VALUE.select("GROUPID", "CLIENT_DS_ID", "CODE_VALUE", "DESCRIPTION")
    ZH_FIELD_VALUE = ZH_FIELD_VALUE.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_zfv").withColumnRenamed("GROUPID", "GROUPID_zfv")

    var ODZ = OD.join(ZH_FIELD_VALUE, OD("OE_FIELD_VALUE") === ZH_FIELD_VALUE("CODE_VALUE") &&  OD("CLIENT_DS_ID_od") === ZH_FIELD_VALUE("CLIENT_DS_ID_zfv") &&  OD("GROUPID_od") === ZH_FIELD_VALUE("GROUPID_zfv"), "left_outer").drop("CLIENT_DS_ID_zfv").drop("GROUPID_zfv")
    val group4 = Window.partitionBy(ODZ("OD_ORDER_ID"), ODZ("ACTION_SEQUENCE"), ODZ("OE_FIELD_ID"), ODZ("GROUPID_od"), ODZ("CLIENT_DS_ID_od")).orderBy(ODZ("UPDT_DT_TM").desc)
    var ODZ1 = ODZ.withColumn("RESULT", row_number().over(group4))
    ODZ1 = ODZ1.filter(ODZ1("RESULT") === 1)

    var OOAJ21 = OOAJ1.join(ODZ1, OOAJ1("ORDER_ID") === ODZ1("OD_ORDER_ID") && OOAJ1("ACTION_SEQUENCE") === ODZ1("ACTION_SEQUENCE") && lit(1) === ODZ1("RESULT") && OOAJ1("GROUPID_oms") === ODZ1("GROUPID_od")  && OOAJ1("CLIENT_DS_ID_oms") === ODZ1("CLIENT_DS_ID_od") , "left_outer").drop("CLIENT_DS_ID_od").drop("GROUPID_od")
    var OOAJ2 =OOAJ21.filter("ACTIVE_IND != '0'")


    var LIST_NO_ACTION_TYPE_CD = predicate_value_list(mpv, "NEW_ORDER_CD", "RXORDER", "ORDER_ACTION", "ACTION_TYPE_CD", "LIST_NO_ACTION_TYPE_CD_VAL");
    var LIST_IO_OE_FIELD_ID = predicate_value_list(mpv, "INFUSEOVER", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_IO_OE_FIELD_ID_VAL");
    var LIST_IOUNIT_OE_FIELD_ID = predicate_value_list(mpv, "INFUSEOVERUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_IOUNIT_OE_FIELD_ID_VAL");
    var LIST_RATE_OE_FIELD_ID = predicate_value_list(mpv, "RATE", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_RATE_OE_FIELD_ID_VAL");
    var LIST_RUNIT_OE_FIELD_ID = predicate_value_list(mpv, "RATEUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_RUNIT_OE_FIELD_ID_VAL");
    var LIST_TV_OE_FIELD_ID = predicate_value_list(mpv, "TOTALVOLUME", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_TV_OE_FIELD_ID_VAL");
    var LIST_DAW_OE_FIELD_ID = predicate_value_list(mpv, "DAW", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DAW_OE_FIELD_ID_VAL");
    var LIST_FREQ_OE_FIELD_ID = predicate_value_list(mpv, "FREQ", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_FREQ_OE_FIELD_ID_VAL");
    var LIST_VDUNIT_OE_FIELD_ID = predicate_value_list(mpv, "VOLUMEDOSEUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_VDUNIT_OE_FIELD_ID_VAL");
    var LIST_DUR_OE_FIELD_ID = predicate_value_list(mpv, "DURATION", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DUR_OE_FIELD_ID_VAL");
    var LIST_DURUNIT_OE_FIELD_ID = predicate_value_list(mpv, "DURATIONUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DURUNIT_OE_FIELD_ID_VAL");
    var LIST_DF_OE_FIELD_ID = predicate_value_list(mpv, "DRUGFORM", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DF_OE_FIELD_ID_VAL");
    var LIST_VD_OE_FIELD_ID = predicate_value_list(mpv, "VOLUMEDOSE", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_VD_OE_FIELD_ID_VAL");
    var LIST_RXROUTE_OE_FIELD_ID = predicate_value_list(mpv, "RXROUTE", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_RXROUTE_OE_FIELD_ID_VAL");
    var LIST_SD_OE_FIELD_ID = predicate_value_list(mpv, "STRENGTHDOSE", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_SD_OE_FIELD_ID_VAL");
    var LIST_SDUNIT_OE_FIELD_ID = predicate_value_list(mpv, "STRENGTHDOSEUNIT", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_SDUNIT_OE_FIELD_ID_VAL");
    var LIST_LDS_OE_FIELD_ID = predicate_value_list(mpv, "ORDERS", "RXORDER_LOCALDAYSUPPLIED", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_LDS_OE_FIELD_ID_VAL");
    var LIST_QTY_OE_FIELD_ID = predicate_value_list(mpv, "QTYPERFILL", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_QTY_OE_FIELD_ID_VAL");
    var LIST_NBR_OE_FIELD_ID = predicate_value_list(mpv, "NBRREFILLS", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_NBR_OE_FIELD_ID_VAL");
    var LIST_RQSTARTDT_ACT_TYPE_CD = predicate_value_list(mpv, "REQSTART_ACTION_TYPE_CD", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_RQSTARTDT_ACT_TYPE_CD_VAL");
    var LIST_RQSTARTDT_OE_FIELD_ID = predicate_value_list(mpv, "REQSTARTDTTM", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_RQSTARTDT_OE_FIELD_ID_VAL");
    var LIST_STOP_ACT_TYPE_CD = predicate_value_list(mpv, "STOP_ACTION_TYPE_CD", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_STOP_ACT_TYPE_CD_VAL");
    var LIST_STOPDT_OE_FIELD_ID = predicate_value_list(mpv, "STOPDTTM", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_STOPDT_OE_FIELD_ID_VAL");
    var LIST_ACTION_TYPE_CD = predicate_value_list(mpv, "ACTION_TYPE_CD", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_ACTION_TYPE_CD_VAL");
    var LIST_CANCEL_ACTION_TYPE_CD = predicate_value_list(mpv, "CANCEL", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_CANCEL_ACTION_TYPE_CD_VAL");
    var LIST_CR_OE_FIELD_ID = predicate_value_list(mpv, "CANCELREASON", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_CR_OE_FIELD_ID_VAL");
    var LIST_DISCONT_ACT_TYPE_CD = predicate_value_list(mpv, "DISCONTINUE", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_DISCONT_ACT_TYPE_CD_VAL");
    var LIST_DCREASON_OE_FIELD_ID = predicate_value_list(mpv, "DCREASON", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_DCREASON_OE_FIELD_ID_VAL");
    var LIST_SUSPEND_ACTION_TYPE_CD = predicate_value_list(mpv, "SUSPEND", "RXORDER", "ORDERS", "ACTION_TYPE_CD", "LIST_SUSPEND_ACTION_TYPE_CD_VAL");
    var LIST_SUSREASON_OE_FIELD_ID = predicate_value_list(mpv, "SUSPENDREASON", "RXORDER", "ORDER_DETAIL", "OE_FIELD_ID", "LIST_SUSREASON_OE_FIELD_ID_VAL");

    var OOAJ22 = OOAJ2.join(LIST_NO_ACTION_TYPE_CD, OOAJ2("ACTION_TYPE_CD") === LIST_NO_ACTION_TYPE_CD("LIST_NO_ACTION_TYPE_CD_VAL") && OOAJ2("GROUPID_oms") === LIST_NO_ACTION_TYPE_CD("GROUPID") && OOAJ2("CLIENT_DS_ID_oms") === LIST_NO_ACTION_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")

    var OOAJ23 = OOAJ22.join(LIST_IO_OE_FIELD_ID, OOAJ22("OE_FIELD_ID") === LIST_IO_OE_FIELD_ID("LIST_IO_OE_FIELD_ID_VAL") && OOAJ22("GROUPID_oms") === LIST_IO_OE_FIELD_ID("GROUPID") && OOAJ22("CLIENT_DS_ID_oms") === LIST_IO_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ24 = OOAJ23.join(LIST_IOUNIT_OE_FIELD_ID, OOAJ23("OE_FIELD_ID") === LIST_IOUNIT_OE_FIELD_ID("LIST_IOUNIT_OE_FIELD_ID_VAL") && OOAJ23("GROUPID_oms") === LIST_IOUNIT_OE_FIELD_ID("GROUPID") && OOAJ23("CLIENT_DS_ID_oms") === LIST_IOUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ25 = OOAJ24.join(LIST_RATE_OE_FIELD_ID, OOAJ24("OE_FIELD_ID") === LIST_RATE_OE_FIELD_ID("LIST_RATE_OE_FIELD_ID_VAL") && OOAJ24("GROUPID_oms") === LIST_RATE_OE_FIELD_ID("GROUPID") && OOAJ24("CLIENT_DS_ID_oms") === LIST_RATE_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ26 = OOAJ25.join(LIST_RUNIT_OE_FIELD_ID, OOAJ25("OE_FIELD_ID") === LIST_RUNIT_OE_FIELD_ID("LIST_RUNIT_OE_FIELD_ID_VAL") && OOAJ25("GROUPID_oms") === LIST_RUNIT_OE_FIELD_ID("GROUPID") && OOAJ25("CLIENT_DS_ID_oms") === LIST_RUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ27 = OOAJ26.join(LIST_TV_OE_FIELD_ID, OOAJ26("OE_FIELD_ID") === LIST_TV_OE_FIELD_ID("LIST_TV_OE_FIELD_ID_VAL") && OOAJ26("GROUPID_oms") === LIST_TV_OE_FIELD_ID("GROUPID") && OOAJ26("CLIENT_DS_ID_oms") === LIST_TV_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ28 = OOAJ27.join(LIST_DAW_OE_FIELD_ID, OOAJ27("OE_FIELD_ID") === LIST_DAW_OE_FIELD_ID("LIST_DAW_OE_FIELD_ID_VAL") && OOAJ27("GROUPID_oms") === LIST_DAW_OE_FIELD_ID("GROUPID") && OOAJ27("CLIENT_DS_ID_oms") === LIST_DAW_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ29 = OOAJ28.join(LIST_FREQ_OE_FIELD_ID, OOAJ28("OE_FIELD_ID") === LIST_FREQ_OE_FIELD_ID("LIST_FREQ_OE_FIELD_ID_VAL") && OOAJ28("GROUPID_oms") === LIST_FREQ_OE_FIELD_ID("GROUPID") && OOAJ28("CLIENT_DS_ID_oms") === LIST_FREQ_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ30 = OOAJ29.join(LIST_VDUNIT_OE_FIELD_ID, OOAJ29("OE_FIELD_ID") === LIST_VDUNIT_OE_FIELD_ID("LIST_VDUNIT_OE_FIELD_ID_VAL") && OOAJ29("GROUPID_oms") === LIST_VDUNIT_OE_FIELD_ID("GROUPID") && OOAJ29("CLIENT_DS_ID_oms") === LIST_VDUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ31 = OOAJ30.join(LIST_DUR_OE_FIELD_ID, OOAJ30("OE_FIELD_ID") === LIST_DUR_OE_FIELD_ID("LIST_DUR_OE_FIELD_ID_VAL") && OOAJ30("GROUPID_oms") === LIST_DUR_OE_FIELD_ID("GROUPID") && OOAJ30("CLIENT_DS_ID_oms") === LIST_DUR_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ32 = OOAJ31.join(LIST_DURUNIT_OE_FIELD_ID, OOAJ31("OE_FIELD_ID") === LIST_DURUNIT_OE_FIELD_ID("LIST_DURUNIT_OE_FIELD_ID_VAL") && OOAJ31("GROUPID_oms") === LIST_DURUNIT_OE_FIELD_ID("GROUPID") && OOAJ31("CLIENT_DS_ID_oms") === LIST_DURUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ33 = OOAJ32.join(LIST_DF_OE_FIELD_ID, OOAJ32("OE_FIELD_ID") === LIST_DF_OE_FIELD_ID("LIST_DF_OE_FIELD_ID_VAL") && OOAJ32("GROUPID_oms") === LIST_DF_OE_FIELD_ID("GROUPID") && OOAJ32("CLIENT_DS_ID_oms") === LIST_DF_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ34 = OOAJ33.join(LIST_VD_OE_FIELD_ID, OOAJ33("OE_FIELD_ID") === LIST_VD_OE_FIELD_ID("LIST_VD_OE_FIELD_ID_VAL") && OOAJ33("GROUPID_oms") === LIST_VD_OE_FIELD_ID("GROUPID") && OOAJ33("CLIENT_DS_ID_oms") === LIST_VD_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ35 = OOAJ34.join(LIST_RXROUTE_OE_FIELD_ID, OOAJ34("OE_FIELD_ID") === LIST_RXROUTE_OE_FIELD_ID("LIST_RXROUTE_OE_FIELD_ID_VAL") && OOAJ34("GROUPID_oms") === LIST_RXROUTE_OE_FIELD_ID("GROUPID") && OOAJ34("CLIENT_DS_ID_oms") === LIST_RXROUTE_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ36 = OOAJ35.join(LIST_SD_OE_FIELD_ID, OOAJ35("OE_FIELD_ID") === LIST_SD_OE_FIELD_ID("LIST_SD_OE_FIELD_ID_VAL") && OOAJ35("GROUPID_oms") === LIST_SD_OE_FIELD_ID("GROUPID") && OOAJ35("CLIENT_DS_ID_oms") === LIST_SD_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ37 = OOAJ36.join(LIST_SDUNIT_OE_FIELD_ID, OOAJ36("OE_FIELD_ID") === LIST_SDUNIT_OE_FIELD_ID("LIST_SDUNIT_OE_FIELD_ID_VAL") && OOAJ36("GROUPID_oms") === LIST_SDUNIT_OE_FIELD_ID("GROUPID") && OOAJ36("CLIENT_DS_ID_oms") === LIST_SDUNIT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ38 = OOAJ37.join(LIST_NBR_OE_FIELD_ID, OOAJ37("OE_FIELD_ID") === LIST_NBR_OE_FIELD_ID("LIST_NBR_OE_FIELD_ID_VAL") && OOAJ37("GROUPID_oms") === LIST_NBR_OE_FIELD_ID("GROUPID") && OOAJ37("CLIENT_DS_ID_oms") === LIST_NBR_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ39 = OOAJ38.join(LIST_LDS_OE_FIELD_ID, OOAJ38("OE_FIELD_ID") === LIST_LDS_OE_FIELD_ID("LIST_LDS_OE_FIELD_ID_VAL") && OOAJ38("GROUPID_oms") === LIST_LDS_OE_FIELD_ID("GROUPID") && OOAJ38("CLIENT_DS_ID_oms") === LIST_LDS_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ40 = OOAJ39.join(LIST_QTY_OE_FIELD_ID, OOAJ39("OE_FIELD_ID") === LIST_QTY_OE_FIELD_ID("LIST_QTY_OE_FIELD_ID_VAL") && OOAJ39("GROUPID_oms") === LIST_QTY_OE_FIELD_ID("GROUPID") && OOAJ39("CLIENT_DS_ID_oms") === LIST_QTY_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ41 = OOAJ40.join(LIST_RQSTARTDT_ACT_TYPE_CD, OOAJ40("ACTION_TYPE_CD") === LIST_RQSTARTDT_ACT_TYPE_CD("LIST_RQSTARTDT_ACT_TYPE_CD_VAL") && OOAJ40("GROUPID_oms") === LIST_RQSTARTDT_ACT_TYPE_CD("GROUPID") && OOAJ40("CLIENT_DS_ID_oms") === LIST_RQSTARTDT_ACT_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ42 = OOAJ41.join(LIST_RQSTARTDT_OE_FIELD_ID, OOAJ41("OE_FIELD_ID") === LIST_RQSTARTDT_OE_FIELD_ID("LIST_RQSTARTDT_OE_FIELD_ID_VAL") && OOAJ41("GROUPID_oms") === LIST_RQSTARTDT_OE_FIELD_ID("GROUPID") && OOAJ41("CLIENT_DS_ID_oms") === LIST_RQSTARTDT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ43 = OOAJ42.join(LIST_STOP_ACT_TYPE_CD, OOAJ42("ACTION_TYPE_CD") === LIST_STOP_ACT_TYPE_CD("LIST_STOP_ACT_TYPE_CD_VAL") && OOAJ42("GROUPID_oms") === LIST_STOP_ACT_TYPE_CD("GROUPID") && OOAJ42("CLIENT_DS_ID_oms") === LIST_STOP_ACT_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ44 = OOAJ43.join(LIST_STOPDT_OE_FIELD_ID, OOAJ43("OE_FIELD_ID") === LIST_STOPDT_OE_FIELD_ID("LIST_STOPDT_OE_FIELD_ID_VAL") && OOAJ43("GROUPID_oms") === LIST_STOPDT_OE_FIELD_ID("GROUPID") && OOAJ43("CLIENT_DS_ID_oms") === LIST_STOPDT_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ45 = OOAJ44.join(LIST_ACTION_TYPE_CD, OOAJ44("ACTION_TYPE_CD") === LIST_ACTION_TYPE_CD("LIST_ACTION_TYPE_CD_VAL") && OOAJ44("GROUPID_oms") === LIST_ACTION_TYPE_CD("GROUPID") && OOAJ44("CLIENT_DS_ID_oms") === LIST_ACTION_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ46 = OOAJ45.join(LIST_CANCEL_ACTION_TYPE_CD, OOAJ45("ACTION_TYPE_CD") === LIST_CANCEL_ACTION_TYPE_CD("LIST_CANCEL_ACTION_TYPE_CD_VAL") && OOAJ45("GROUPID_oms") === LIST_CANCEL_ACTION_TYPE_CD("GROUPID") && OOAJ45("CLIENT_DS_ID_oms") === LIST_CANCEL_ACTION_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ47 = OOAJ46.join(LIST_CR_OE_FIELD_ID, OOAJ46("OE_FIELD_ID") === LIST_CR_OE_FIELD_ID("LIST_CR_OE_FIELD_ID_VAL") && OOAJ46("GROUPID_oms") === LIST_CR_OE_FIELD_ID("GROUPID") && OOAJ46("CLIENT_DS_ID_oms") === LIST_CR_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ48 = OOAJ47.join(LIST_DISCONT_ACT_TYPE_CD, OOAJ47("ACTION_TYPE_CD") === LIST_DISCONT_ACT_TYPE_CD("LIST_DISCONT_ACT_TYPE_CD_VAL") && OOAJ47("GROUPID_oms") === LIST_DISCONT_ACT_TYPE_CD("GROUPID") && OOAJ47("CLIENT_DS_ID_oms") === LIST_DISCONT_ACT_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ49 = OOAJ48.join(LIST_DCREASON_OE_FIELD_ID, OOAJ48("OE_FIELD_ID") === LIST_DCREASON_OE_FIELD_ID("LIST_DCREASON_OE_FIELD_ID_VAL") && OOAJ48("GROUPID_oms") === LIST_DCREASON_OE_FIELD_ID("GROUPID") && OOAJ48("CLIENT_DS_ID_oms") === LIST_DCREASON_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ50 = OOAJ49.join(LIST_SUSPEND_ACTION_TYPE_CD, OOAJ49("ACTION_TYPE_CD") === LIST_SUSPEND_ACTION_TYPE_CD("LIST_SUSPEND_ACTION_TYPE_CD_VAL") && OOAJ49("GROUPID_oms") === LIST_SUSPEND_ACTION_TYPE_CD("GROUPID") && OOAJ49("CLIENT_DS_ID_oms") === LIST_SUSPEND_ACTION_TYPE_CD("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")
    var OOAJ51 = OOAJ50.join(LIST_SUSREASON_OE_FIELD_ID, OOAJ50("OE_FIELD_ID") === LIST_SUSREASON_OE_FIELD_ID("LIST_SUSREASON_OE_FIELD_ID_VAL") && OOAJ50("GROUPID_oms") === LIST_SUSREASON_OE_FIELD_ID("GROUPID") && OOAJ50("CLIENT_DS_ID_oms") === LIST_SUSREASON_OE_FIELD_ID("CLIENT_DS_ID"), "left_outer").drop("CLIENT_DS_ID").drop("GROUPID")

    var OOAJ52=OOAJ51.withColumn("LOCALMEDCODE", when(OOAJ51("CATALOG_CD").isNull, "0").otherwise(OOAJ51("CATALOG_CD")))
    OOAJ52=OOAJ52.withColumnRenamed("ORDER_DETAIL_DISPLAY_LINE", "SIGNATURE")
    OOAJ52=OOAJ52.withColumn("ORDERVSPRESCRIPTION", when(OOAJ52("ORIG_ORD_AS_FLAG") === "0", "O")
            .when(OOAJ52("ORIG_ORD_AS_FLAG") === "4", "O")
            .when(OOAJ52("ORIG_ORD_AS_FLAG") === "1", "P")
            .when(OOAJ52("ORIG_ORD_AS_FLAG") === "5", "P").otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALDESCRIPTION", OOAJ52("DISPLAY_zh"))

    OOAJ52=OOAJ52.withColumn("ISSUEDATE", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull, OOAJ52("ORDER_DT_TM")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALPROVIDERID", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull and (OOAJ52("ORDER_PROVIDER_ID") !== "0"), OOAJ52("ORDER_PROVIDER_ID")))

    //STOPPED here
    OOAJ52=OOAJ52.withColumn("ORDERSTATUS", when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG") === "1", concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("ORDER_STATUS_CD"))).otherwise(null))
    OOAJ52=OOAJ52.withColumn("INFUSEOVER", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_IO_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("INFUSEOVERUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_IOUNIT_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("RATE", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_RATE_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("RATEUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_RUNIT_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALINFUSIONVOLUME", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_TV_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALDAW", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_DAW_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"),  OOAJ52("OE_FIELD_DISPLAY_VALUE"))).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALDOSEFREQ", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_FREQ_OE_FIELD_ID_VAL").isNotNull && OOAJ52("DESCRIPTION").isNotNull, OOAJ52("DESCRIPTION")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALDOSEUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_VDUNIT_OE_FIELD_ID_VAL").isNotNull && OOAJ52("DESCRIPTION").isNotNull, OOAJ52("DESCRIPTION")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("DURATION", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_DUR_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("DURATIONUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_DURUNIT_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALFORM", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_DF_OE_FIELD_ID_VAL").isNotNull, OOAJ52("DESCRIPTION")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALQTYOFDOSEUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_VD_OE_FIELD_ID_VAL").isNotNull, safe_to_number(OOAJ52("OE_FIELD_DISPLAY_VALUE"))).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALROUTE", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_RXROUTE_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("OE_FIELD_VALUE"))).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALSTRENGTHPERDOSEUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_SD_OE_FIELD_ID_VAL").isNotNull,  safe_to_number(OOAJ52("OE_FIELD_DISPLAY_VALUE"))).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALSTRENGTHUNIT", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_SDUNIT_OE_FIELD_ID_VAL").isNotNull, OOAJ52("DESCRIPTION")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("FILLNUM", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_NBR_OE_FIELD_ID_VAL").isNotNull,  safe_to_number_with_default(OOAJ52("OE_FIELD_DISPLAY_VALUE"),"9999")).otherwise(null))
    OOAJ52=OOAJ52.withColumn("LOCALDAYSUPPLIED", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_LDS_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")))
    OOAJ52=OOAJ52.withColumn("QUANTITYPERFILL", when(OOAJ52("LIST_NO_ACTION_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_QTY_OE_FIELD_ID_VAL").isNotNull, OOAJ52("OE_FIELD_DISPLAY_VALUE")))
    OOAJ52=OOAJ52.withColumn("DISCONTINUEDATE", when((OOAJ52("LATEST_ACTION_SEQUENCE_FLG") === 1 && OOAJ52("LIST_RQSTARTDT_ACT_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_RQSTARTDT_OE_FIELD_ID_VAL").isNotNull).or(
        OOAJ52("LATEST_ACTION_SEQUENCE_FLG") === 1 && OOAJ52("LIST_STOP_ACT_TYPE_CD_VAL").isNotNull && OOAJ52("LIST_STOPDT_OE_FIELD_ID_VAL").isNotNull), OOAJ52("OE_FIELD_DT_TM_VALUE")).otherwise(null))

    OOAJ52=OOAJ52.withColumn("DISCONTINUEREASON",
        when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG")===1 and OOAJ52("LIST_ACTION_TYPE_CD_VAL").isNotNull, when(OOAJ52("STOP_TYPE_CD") === "0", null).otherwise(concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("STOP_TYPE_CD"))))
                .when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG")===1 and OOAJ52("LIST_CANCEL_ACTION_TYPE_CD_VAL").isNotNull and OOAJ52("LIST_CR_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("DESCRIPTION")))
                .when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG")===1 and OOAJ52("LIST_DISCONT_ACT_TYPE_CD_VAL").isNotNull and OOAJ52("LIST_DCREASON_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("DESCRIPTION")))
                .when(OOAJ52("LATEST_ACTION_SEQUENCE_FLG")===1 and OOAJ52("LIST_SUSPEND_ACTION_TYPE_CD_VAL").isNotNull and OOAJ52("LIST_SUSREASON_OE_FIELD_ID_VAL").isNotNull, concat_ws(".", OOAJ52("CLIENT_DS_ID_oms"), OOAJ52("DESCRIPTION")))
                .otherwise(null))

    //OOAJ52=OOAJ52.withColumn("DISCONTINUEREASON", when(cdn1.or(cdn2).or(cdn3), str).when(cdn4,  str2))
    OOAJ52=OOAJ52.withColumnRenamed("ORDER_ID", "RXID1").withColumn("ODDLS", when(OOAJ52("SIGNATURE").isin("0", "4"), "O").when(OOAJ52("SIGNATURE").isin("1", "5"), "P"))

    //        var OOAJ53 = OOAJ52.withColumnRenamed("PERSON_ID","PATIENTID").groupBy("RXID1", "GROUPID_oms", "CLIENT_DS_ID_oms", "PATIENTID", "ENCNTR_ID", "LOCALMEDCODE", "SIGNATURE", "ORDERVSPRESCRIPTION", "LOCALDESCRIPTION", "ORIG_ORD_AS_FLAG")
    var OOAJ53 = OOAJ52.withColumnRenamed("PERSON_ID","PATIENTID").groupBy("RXID1", "PATIENTID", "ENCNTR_ID",  "GROUPID_oms", "CLIENT_DS_ID_oms","CATALOG_CD", "ODDLS", "LOCALDESCRIPTION",  "ORIG_ORD_AS_FLAG", "ORIG_ORDER_DT_TM")
            .agg(max(OOAJ52("ORDERSTATUS")).as("ORDERSTATUS"), max(OOAJ52("DELETE_FLG")).as("DELETE_FLG"), max(OOAJ52("INFUSEOVER")).as("INFUSEOVER"), max(OOAJ52("INFUSEOVERUNIT")).as("INFUSEOVERUNIT"), max(OOAJ52("RATE")).as("RATE"), max(OOAJ52("RATEUNIT")).as("RATEUNIT"), max(OOAJ52("LOCALINFUSIONVOLUME")).as("LOCALINFUSIONVOLUME"),
                max(OOAJ52("LOCALDAW")).as("LOCALDAW"), max(OOAJ52("LOCALDOSEFREQ")).as("LOCALDOSEFREQ"), max(OOAJ52("LOCALDOSEUNIT")).as("LOCALDOSEUNIT"), max(OOAJ52("DURATION")).as("DURATION"), max(OOAJ52("DURATIONUNIT")).as("DURATIONUNIT"),
                max(OOAJ52("LOCALFORM")).as("LOCALFORM"), max(OOAJ52("LOCALQTYOFDOSEUNIT")).as("LOCALQTYOFDOSEUNIT"), max(OOAJ52("LOCALROUTE")).as("LOCALROUTE"), max(OOAJ52("LOCALSTRENGTHPERDOSEUNIT")).as("LOCALSTRENGTHPERDOSEUNIT"), max(OOAJ52("LOCALSTRENGTHUNIT")).as("LOCALSTRENGTHUNIT"),
                max(OOAJ52("FILLNUM")).as("FILLNUM"), max(OOAJ52("LOCALDAYSUPPLIED")).as("LOCALDAYSUPPLIED"), max(OOAJ52("QUANTITYPERFILL")).as("QUANTITYPERFILL"), max(OOAJ52("DISCONTINUEDATE")).as("DISCONTINUEDATE"), max(OOAJ52("DISCONTINUEREASON")).as("DISCONTINUEREASON"), max(OOAJ52("LOCALPROVIDERID")).as("LOCALPROVIDERID"),
                max(OOAJ52("ISSUEDATE")).as("ISSUEDATE"), max(OOAJ52("LOCALMEDCODE")).as("LOCALMEDCODE"), max(OOAJ52("SIGNATURE")).as("SIGNATURE"), max(OOAJ52("ORDERVSPRESCRIPTION")).as("ORDERVSPRESCRIPTION")).withColumnRenamed("GROUPID_oms", "GRPID2").withColumnRenamed("CLIENT_DS_ID_oms", "CLSID2")


    var OOAJ55 = OOAJ53.filter(OOAJ53("LOCALMEDCODE").isNotNull)
    var OOAJ56 = OOAJ55.withColumn("LOCALINFUSIONDURATION", concat_ws(" ", OOAJ55("INFUSEOVER"), OOAJ55("INFUSEOVERUNIT")))
            .withColumn("LOCALINFUSIONRATE", concat_ws(" ",OOAJ55("RATE"), OOAJ55("RATEUNIT")))
            .withColumn("LOCALDURATION", concat_ws(" ", OOAJ55("DURATION"), OOAJ55("DURATIONUNIT")))
            .withColumnRenamed("LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHPERDOSEUNIT1")
            .withColumnRenamed("LOCALQTYOFDOSEUNIT", "LOCALQTYOFDOSEUNIT1")

    var ENC = readGenerated(sqlContext,dateStr, "CR2CLINICALENCOUNTER",cfg).select("GROUPID", "CLIENT_DS_ID", "ENCOUNTERID", "LOCALPATIENTTYPE", "PATIENTID").withColumnRenamed("GROUPID", "ENC_GROUPID").withColumnRenamed("ENCOUNTERID", "ENC_ENCOUNTERID")
            .withColumnRenamed("CLIENT_DS_ID", "ENC_CLIENT_DS_ID")

    var MPT = readData(sqlContext, "MAP_PATIENT_TYPE", cfg).withColumnRenamed("CUI", "MPT_CUI")

    var TEMP_H416989_ENCOUNTER_PT = ENC.join(MPT, ENC("LOCALPATIENTTYPE") === MPT("LOCAL_CODE") and ENC("ENC_GROUPID") === MPT("GROUPID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID").withColumnRenamed("PATIENTID", "PATIENTID_TEMP")
    var OOAJ57 = OOAJ56.join(TEMP_H416989_ENCOUNTER_PT, OOAJ56("PATIENTID") === TEMP_H416989_ENCOUNTER_PT("PATIENTID_TEMP") &&
            OOAJ56("ENCNTR_ID") === TEMP_H416989_ENCOUNTER_PT("ENC_ENCOUNTERID") &&
            OOAJ56("GRPID2") === TEMP_H416989_ENCOUNTER_PT("ENC_GROUPID") &&
            OOAJ56("CLSID2") === TEMP_H416989_ENCOUNTER_PT("ENC_CLIENT_DS_ID"), "left_outer").drop("ENC_GROUPID").drop("ENC_CLIENT_DS_ID").drop("PATIENTID_TEMP")

    var MOT = readData(sqlContext, "MAP_ORDERTYPE", cfg).withColumnRenamed("CUI", "MOT_CUI").withColumnRenamed("LOCALCODE", "MOT_LOCALCODE")
    OOAJ57 = OOAJ57.join(MOT,OOAJ57("GRPID2")===MOT("GROUPID") and OOAJ57("ORIG_ORD_AS_FLAG")===MOT("MOT_LOCALCODE"), "left_outer")

    OOAJ57 = OOAJ57.withColumn("ORDERTYPE", OOAJ57("MOT_CUI"))
    var ZHOCIR = readDictionaryTable(sqlContext, "ZH_ORDER_CATALOG_ITEM_R",cfg)
    ZHOCIR=ZHOCIR.withColumnRenamed("ITEM_ID","ZHOCIR_ITEM_ID").select("GROUPID", "CLIENT_DS_ID",  "ZHOCIR_ITEM_ID", "CATALOG_CD")
    ZHOCIR = ZHOCIR.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_zc").withColumnRenamed("GROUPID", "GROUPID_zc")

    var MID  = readDictionaryTable(sqlContext, "ZH_MED_IDENTIFIER",cfg)
    MID=MID.select("GROUPID", "CLIENT_DS_ID",  "VALUE_KEY", "ITEM_ID", "MED_IDENTIFIER_TYPE_CD", "ACTIVE_IND", "VALUE")
    MID = MID.withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_mid").withColumnRenamed("GROUPID", "GROUPID_mid")


    var LIST_LOCALDRUGDESC = predicate_value_listlike(mpv, "LOCALDRUGDESC", "TEMP_MEDS_", "ZH_MED_IDENTIFIER", "MED_IDENTIFIER_TYPE_CD", "LIST_LOCALDRUGDESC_VAL");
    var LIST_LOCALNDC = predicate_value_listlike(mpv, "LOCALNDC", "TEMP_MEDS", "ZH_MED_IDENTIFIER", "MED_IDENTIFIER_TYPE_CD", "LIST_LOCALNDC_VAL");

    var TMJ0 = ZHOCIR.join(MID, ZHOCIR("ZHOCIR_ITEM_ID") === MID("ITEM_ID") && ZHOCIR("GROUPID_zc") === MID("GROUPID_mid") && ZHOCIR("CLIENT_DS_ID_zc") === MID("CLIENT_DS_ID_mid"), "left_outer").drop("GROUPID_mid").drop("CLIENT_DS_ID_mid")
    var TMJ01 = TMJ0.join(LIST_LOCALDRUGDESC, TMJ0("MED_IDENTIFIER_TYPE_CD") === LIST_LOCALDRUGDESC("LIST_LOCALDRUGDESC_VAL") && TMJ0("GROUPID_zc") === LIST_LOCALDRUGDESC("GROUPID") && TMJ0("CLIENT_DS_ID_zc") === LIST_LOCALDRUGDESC("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    var TMJ02 = TMJ01.join(LIST_LOCALNDC, TMJ01("MED_IDENTIFIER_TYPE_CD") === LIST_LOCALNDC("LIST_LOCALNDC_VAL") && TMJ01("GROUPID_zc") === LIST_LOCALNDC("GROUPID") && TMJ01("CLIENT_DS_ID_zc") === LIST_LOCALNDC("CLIENT_DS_ID"), "left_outer").drop("GROUPID").drop("CLIENT_DS_ID")
    var TMJ1 = TMJ02.filter("ACTIVE_IND == '1' AND ( LIST_LOCALDRUGDESC_VAL IS NOT NULL OR LIST_LOCALNDC_VAL IS NOT NULL)")
    var TMJ2 = TMJ1.withColumn("LEN", length(TMJ1("VALUE"))).withColumn("II",  TMJ1("ITEM_ID"))
    val group5 = Window.partitionBy(TMJ2("CATALOG_CD") , TMJ2("MED_IDENTIFIER_TYPE_CD"), TMJ2("GROUPID_zc"), TMJ2("CLIENT_DS_ID_zc")).orderBy(TMJ2("LEN"), TMJ2("II"))
    var TMJ3 = TMJ2.withColumn("ROWNUMBER", row_number().over(group5))

    val group6 = Window.partitionBy(TMJ3("CATALOG_CD"), TMJ3("MED_IDENTIFIER_TYPE_CD"), TMJ3("GROUPID_zc"), TMJ3("CLIENT_DS_ID_zc"))
    var TMJ6 = TMJ3.withColumn("CNT_NDC", count("VALUE_KEY").over(group6))

    var TMJ7 = TMJ6.withColumn("LOCALGENERICDESC", when(TMJ6("ROWNUMBER") === "1" and TMJ6("LIST_LOCALDRUGDESC_VAL").isNotNull, TMJ6("VALUE"))).withColumn("LOCALNDC", when(TMJ6("CNT_NDC") === "1" and TMJ6("LIST_LOCALNDC_VAL").isNotNull, TMJ6("VALUE_KEY")))
    var TMJ9 = TMJ7.groupBy("CATALOG_CD", "GROUPID_zc", "CLIENT_DS_ID_zc").agg(max(TMJ7("LOCALGENERICDESC")).as("LOCALGENERICDESC"), max(TMJ7("LOCALNDC")).as("LOCALNDC"))

    var OOAJ58 = OOAJ57.join(TMJ9, OOAJ57("LOCALMEDCODE") === TMJ9("CATALOG_CD") && OOAJ57("GRPID2") === TMJ9("GROUPID_zc") && OOAJ57("CLSID2") === TMJ9("CLIENT_DS_ID_zc"), "left_outer")

    var OOAJ59 = OOAJ58.withColumn("DATASRC", lit("orders"))
            .withColumn("FACILITYID", lit("NULL"))
            .withColumn("EXPIREDATE", lit("NULL"))
            .withColumn("ALTMEDCODE", lit("NULL"))
            .withColumn("MAPPEDNDC", lit("NULL"))
            .withColumn("MAPPEDGPI", lit("NULL"))
            .withColumn("MAPPEDNDC_CONF", lit("NULL"))
            .withColumn("MAPPEDGPI_CONF", lit("NULL"))
            .withColumn("VENUE", when(OOAJ58("ORIG_ORD_AS_FLAG") === "1" or OOAJ58("MPT_CUI").isin("CH000110", "CH000113", "CH000924", "CH000935", "CH000936", "CH000937"), "1").otherwise("0"))
            .withColumn("MAP_USED", lit("NULL"))
            .withColumn("HUM_MED_KEY", lit("NULL"))
            .withColumn("NDC11", lit("NULL"))
            .withColumn("LOCALGPI", lit("NULL"))
            .withColumn("LOCALDISCHARGEMEDFLG", lit("NULL"))
            .withColumn("HUM_GEN_MED_KEY", lit("NULL"))
            .withColumn("HTS_GENERIC", lit("NULL"))
            .withColumn("HTS_GENERIC_EXT", lit("NULL"))
            .withColumn("HGPID", lit("NULL"))
            .withColumn("GRP_MPI", lit("NULL"))
            .withColumn("MSTRPROVID", lit("NULL"))
            .withColumn("ALLOWEDAMOUNT", lit("NULL"))
            .withColumn("PAIDAMOUNT", lit("NULL"))
            .withColumn("CHARGE", lit("NULL"))
            .withColumn("DCC", lit("NULL"))
            .withColumn("RXNORM_CODE", lit("NULL"))
            .withColumn("ACTIVE_MED_FLAG", lit("NULL"))
            .withColumn("ROW_SOURCE", lit("NULL"))
            .withColumn("MODIFIED_DATE", lit("NULL"))
            .withColumn("LOCALTOTALDOSE", OOAJ58("LOCALSTRENGTHPERDOSEUNIT1").multiply(OOAJ58("LOCALQTYOFDOSEUNIT1"))).drop("GROUPID").drop("CLIENT_DS_ID").withColumnRenamed("GRPID2", "GROUPID").withColumnRenamed("CLSID2", "CLIENT_DS_ID")



    var OOAJ60 = OOAJ59.withColumnRenamed("GROUPID_oms", "GROUPID").withColumnRenamed("CLIENT_DS_ID_oms", "CLIENT_DS_ID").select("GROUPID", "DATASRC", "FACILITYID", "RXID1", "ENCNTR_ID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID", "ISSUEDATE", "DISCONTINUEDATE",
        "LOCALSTRENGTHPERDOSEUNIT1", "LOCALSTRENGTHUNIT", "LOCALROUTE", "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE", "ORDERSTATUS", "ALTMEDCODE", "MAPPEDNDC",
        "MAPPEDGPI", "MAPPEDNDC_CONF", "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED", "LOCALFORM", "LOCALQTYOFDOSEUNIT1", "LOCALTOTALDOSE", "LOCALDOSEFREQ",
        "LOCALDURATION", "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",
        "LOCALDAW", "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON",
        "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE", "ACTIVE_MED_FLAG", "ROW_SOURCE", "MODIFIED_DATE").withColumnRenamed("LOCALSTRENGTHPERDOSEUNIT1", "LOCALSTRENGTHPERDOSEUNIT").withColumnRenamed("LOCALQTYOFDOSEUNIT1", "LOCALQTYOFDOSEUNIT")
            .withColumnRenamed("RXID1", "RXID").withColumnRenamed("ENCNTR_ID", "ENCOUNTERID")
    OOAJ60 = OOAJ60.withColumn("FACILITYID", when(OOAJ60("FACILITYID")==="NULL", null).otherwise(OOAJ60("FACILITYID")))
    OOAJ60 = OOAJ60.withColumn("EXPIREDATE", when(OOAJ60("EXPIREDATE")==="NULL", null).otherwise(OOAJ60("EXPIREDATE")))
    OOAJ60 = OOAJ60.withColumn("ALTMEDCODE", when(OOAJ60("ALTMEDCODE")==="NULL", null).otherwise(OOAJ60("ALTMEDCODE")))
    OOAJ60 = OOAJ60.withColumn("MAPPEDNDC", when(OOAJ60("MAPPEDNDC")==="NULL", null).otherwise(OOAJ60("MAPPEDNDC")))
    OOAJ60 = OOAJ60.withColumn("MAPPEDGPI", when(OOAJ60("MAPPEDGPI")==="NULL", null).otherwise(OOAJ60("MAPPEDGPI")))
    OOAJ60 = OOAJ60.withColumn("MAPPEDNDC_CONF", when(OOAJ60("MAPPEDNDC_CONF")==="NULL", null).otherwise(OOAJ60("MAPPEDNDC_CONF")))
    OOAJ60 = OOAJ60.withColumn("MAPPEDGPI_CONF", when(OOAJ60("MAPPEDGPI_CONF")==="NULL", null).otherwise(OOAJ60("MAPPEDGPI_CONF")))
    OOAJ60 = OOAJ60 .withColumn("MAP_USED", when(OOAJ60("MAP_USED")==="NULL", null).otherwise(OOAJ60("MAP_USED")))
    OOAJ60 = OOAJ60.withColumn("HUM_MED_KEY", when(OOAJ60("HUM_MED_KEY")==="NULL", null).otherwise(OOAJ60("HUM_MED_KEY")))
    OOAJ60 = OOAJ60.withColumn("NDC11", when(OOAJ60("NDC11")==="NULL", null).otherwise(OOAJ60("NDC11")))
    OOAJ60 = OOAJ60.withColumn("LOCALGPI", when(OOAJ60("LOCALGPI")==="NULL", null).otherwise(OOAJ60("LOCALGPI")))
    OOAJ60 = OOAJ60.withColumn("LOCALDISCHARGEMEDFLG", when(OOAJ60("LOCALDISCHARGEMEDFLG")==="NULL", null).otherwise(OOAJ60("LOCALDISCHARGEMEDFLG")))
    OOAJ60 = OOAJ60.withColumn("HUM_GEN_MED_KEY", when(OOAJ60("HUM_GEN_MED_KEY")==="NULL", null).otherwise(OOAJ60("HUM_GEN_MED_KEY")))
    OOAJ60 = OOAJ60.withColumn("HTS_GENERIC", when(OOAJ60("HTS_GENERIC")==="NULL", null).otherwise(OOAJ60("HTS_GENERIC")))
    OOAJ60 = OOAJ60.withColumn("HTS_GENERIC_EXT", when(OOAJ60("HTS_GENERIC_EXT")==="NULL", null).otherwise(OOAJ60("HTS_GENERIC_EXT")))
    OOAJ60 = OOAJ60.withColumn("HGPID", when(OOAJ60("HGPID")==="NULL", null).otherwise(OOAJ60("HGPID")))
    OOAJ60 = OOAJ60.withColumn("GRP_MPI", when(OOAJ60("GRP_MPI")==="NULL", null).otherwise(OOAJ60("GRP_MPI")))
    OOAJ60 = OOAJ60.withColumn("MSTRPROVID", when(OOAJ60("MSTRPROVID")==="NULL", null).otherwise(OOAJ60("MSTRPROVID")))
    OOAJ60 = OOAJ60.withColumn("ALLOWEDAMOUNT", when(OOAJ60("ALLOWEDAMOUNT")==="NULL", null).otherwise(OOAJ60("ALLOWEDAMOUNT")))
    OOAJ60 = OOAJ60.withColumn("PAIDAMOUNT", when(OOAJ60("PAIDAMOUNT")==="NULL", null).otherwise(OOAJ60("PAIDAMOUNT")))
    OOAJ60 = OOAJ60.withColumn("CHARGE", when(OOAJ60("CHARGE")==="NULL", null).otherwise(OOAJ60("CHARGE")))
    OOAJ60 = OOAJ60.withColumn("DCC", when(OOAJ60("DCC")==="NULL", null).otherwise(OOAJ60("DCC")))
    OOAJ60 = OOAJ60 .withColumn("RXNORM_CODE", when(OOAJ60("RXNORM_CODE")==="NULL", null).otherwise(OOAJ60("RXNORM_CODE")))
    OOAJ60 = OOAJ60 .withColumn("ACTIVE_MED_FLAG", when(OOAJ60("ACTIVE_MED_FLAG")==="NULL", null).otherwise(OOAJ60("ACTIVE_MED_FLAG")))
    OOAJ60 = OOAJ60 .withColumn("ROW_SOURCE", when(OOAJ60("ROW_SOURCE")==="NULL", null).otherwise(OOAJ60("ROW_SOURCE")))
    OOAJ60 = OOAJ60 .withColumn("MODIFIED_DATE", when(OOAJ60("MODIFIED_DATE")==="NULL", null).otherwise(OOAJ60("MODIFIED_DATE")))

    OOAJ60 = OOAJ60.select("GROUPID", "DATASRC", "FACILITYID", "RXID",
        "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID",
        "ISSUEDATE", "DISCONTINUEDATE", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE",
        "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE",
        "ORDERSTATUS", "ALTMEDCODE", "MAPPEDNDC", "MAPPEDGPI", "MAPPEDNDC_CONF",
        "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED",
        "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ", "LOCALDURATION",
        "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11",
        "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",  "LOCALDAW",
        "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID",
        "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON",
        "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE",
        "ACTIVE_MED_FLAG", "ROW_SOURCE", "MODIFIED_DATE")
    writeGenerated(OOAJ60,dateStr, "CR2RXORDER",cfg)
}



def processCR2RxOrder(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String])={
    clinicalEncounterCernverV2(sqlContext,dateStr,  cfg)
    rxorder_cernerv2(sqlContext,dateStr,  cfg)
}


def rxorder_backend_med_mapping(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {
    var rxo = readGenerated(sqlContext, dateStr,"CONSOLIDATED_RXORDER", cfg)
    var RXORDER = rxo.drop("MAP_USED").drop("HUM_MED_KEY").drop("HUM_GEN_MED_KEY").drop("HUM_GEN_MED_KEY").drop("HTS_GENERIC").drop("HTS_GENERIC_EXT").drop("DCC").drop("RXNORM_CODE")

    var MED_MAP_DCC = readData(sqlContext, "ZH_MED_MAP_DCC", cfg).select("GROUPID", "DATASRC", "CLIENT_DS_ID", "LOCALMEDCODE", "MAP_USED", "HUM_MED_KEY", "HUM_GEN_MED_KEY", "HTS_GENERIC",
        "HTS_GENERIC_EXT", "HTS_GPI", "TOP_NDC", "DCC", "DTS_VERSION", "RXNORM_CODE").withColumnRenamed("CLIENT_DS_ID", "CLIENT_DS_ID_mmd").withColumnRenamed("GROUPID", "GROUPID_mmd")
            .withColumnRenamed("DATASRC", "DATASRC_mmd").withColumnRenamed("LOCALMEDCODE", "LOCALMEDCODE_mmd")

    var RXMED_MAP = RXORDER.join(MED_MAP_DCC, RXORDER("GROUPID") === MED_MAP_DCC("GROUPID_mmd") and RXORDER("DATASRC") === MED_MAP_DCC("DATASRC_mmd") and RXORDER("CLIENT_DS_ID") === MED_MAP_DCC("CLIENT_DS_ID_mmd") and RXORDER("LOCALMEDCODE") === MED_MAP_DCC("LOCALMEDCODE_mmd"), "left_outer")
    RXMED_MAP =RXMED_MAP.withColumn("NDC11", normalizeNDC(RXMED_MAP, "LOCALNDC"))
    RXMED_MAP = RXMED_MAP.withColumn("NDC11", when(RXMED_MAP("NDC11")==="NULL", null).otherwise(RXMED_MAP("NDC11")))

    var RXMED_MAP_FIN = RXMED_MAP.select("GROUPID", "DATASRC", "FACILITYID", "RXID", "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID", "ISSUEDATE", "DISCONTINUEDATE",
        "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE", "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE", "ORDERSTATUS", "ALTMEDCODE",
        "TOP_NDC", "HTS_GPI", "MAPPEDNDC_CONF", "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED", "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ",
        "LOCALDURATION", "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",
        "LOCALDAW", "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID", "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON",
        "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE", "ACTIVE_MED_FLAG").withColumn("MAPPEDNDC", RXMED_MAP("TOP_NDC")).withColumn("MAPPEDGPI", RXMED_MAP("HTS_GPI")).withColumn("RXMEDGROUPID", RXMED_MAP("GROUPID"))

    var MAP_MED_ORDER_STATUS = readData(sqlContext, "MAP_MED_ORDER_STATUS", cfg).select("GROUPID", "LOCALCODE", "CUI", "DTS_VERSION")

    var UPDATE_MEDS_1 = RXMED_MAP_FIN.join(MAP_MED_ORDER_STATUS, RXMED_MAP_FIN("RXMEDGROUPID") === MAP_MED_ORDER_STATUS("GROUPID") and RXMED_MAP_FIN("LOCALMEDCODE") === MAP_MED_ORDER_STATUS("LOCALCODE"), "left_outer")
    UPDATE_MEDS_1 = UPDATE_MEDS_1.drop("GROUPID").withColumnRenamed("RXMEDGROUPID", "GROUPID")
    var ORDERVSPRESCRIPTION = UPDATE_MEDS_1("ORDERVSPRESCRIPTION")
    var DISCONTINUEDATE =from_unixtime(UPDATE_MEDS_1("DISCONTINUEDATE").divide(1000))

    var ISSUEDATE = UPDATE_MEDS_1("ISSUEDATE")
    var EXPIREDATE = UPDATE_MEDS_1("EXPIREDATE")
    var UPDATE_ACTIVE_MEDS_RXORDER = UPDATE_MEDS_1.withColumn("ACTIVE_MED_FLAG", when((ORDERVSPRESCRIPTION === "P").and(DISCONTINUEDATE.isNotNull).and(
        ISSUEDATE.lt(DISCONTINUEDATE)).and(DISCONTINUEDATE < current_timestamp()), "N")
            .when((ORDERVSPRESCRIPTION === "P").and(DISCONTINUEDATE.isNotNull), "N")
            .when((ORDERVSPRESCRIPTION === "P").and(ISSUEDATE.lt(EXPIREDATE)).and(EXPIREDATE.lt(current_timestamp())), "N")
            .when((ORDERVSPRESCRIPTION === "P").and(UPDATE_MEDS_1("CUI").isin("CH001593", "CH001594", "CH001595", "CH001596")), "N").otherwise("Y"))
    writeGenerated(UPDATE_ACTIVE_MEDS_RXORDER, dateStr,"CONSOLIDATED_BACKEND_MED_MAPPING", cfg)
}



def rxorder_backend_seth_gpids(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {
    var RXORDER_BSH = readGenerated(sqlContext, dateStr,"CONSOLIDATED_BACKEND_MED_MAPPING", cfg).drop("HGPID").withColumnRenamed("GRP_MPI", "GRP_MPI1")

    var PATIENT_MPI = readData(sqlContext, "PATIENT_MPI", cfg).select("GROUPID","CLIENT_DS_ID","PATIENTID","HGPID","GRP_MPI").distinct()
    RXORDER_BSH = RXORDER_BSH.join(PATIENT_MPI, Seq("GROUPID","CLIENT_DS_ID","PATIENTID"), "left_outer")

    var RX_PAT_MPI_1 = RXORDER_BSH.select("GROUPID", "DATASRC", "FACILITYID", "RXID", "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID",
        "ISSUEDATE", "DISCONTINUEDATE", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE", "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE",
        "ORDERSTATUS", "ALTMEDCODE", "MAPPEDNDC", "MAPPEDGPI", "MAPPEDNDC_CONF", "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED", "LOCALFORM",
        "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ", "LOCALDURATION", "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11",
        "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",  "LOCALDAW", "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT",
        "CLIENT_DS_ID", "HGPID", "GRP_MPI","LOCALGENERICDESC", "DISCONTINUEREASON", "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE", "ACTIVE_MED_FLAG")

    var ZH_PROV_MASTER1 = readData(sqlContext, "ZH_PROVIDER_MASTER_XREF", cfg).select("GROUPID", "MASTER_HGPROVID", "LOCALPROVIDERID", "CLIENT_DS_ID")
    var RX_PAT_MPI_PROV_MASTER = RX_PAT_MPI_1.join(ZH_PROV_MASTER1, Seq("GROUPID", "CLIENT_DS_ID", "LOCALPROVIDERID"), "left_outer").withColumnRenamed("MASTER_HGPROVID", "MSTRPROVID")


    var RX_PAT_MPI_PROV_MASTER1 = RX_PAT_MPI_PROV_MASTER.withColumn("ORIGIN", when(RX_PAT_MPI_PROV_MASTER("GRP_MPI").isNull, "FILE").otherwise("CDR"))
    var CONSOLIDATED_BACKEND_SETH_GPIDS = RX_PAT_MPI_PROV_MASTER1.select("GROUPID", "DATASRC", "FACILITYID", "RXID", "ENCOUNTERID", "PATIENTID", "LOCALMEDCODE", "LOCALDESCRIPTION", "LOCALPROVIDERID", "ISSUEDATE",
        "DISCONTINUEDATE", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT", "LOCALROUTE", "EXPIREDATE", "QUANTITYPERFILL", "FILLNUM", "SIGNATURE", "ORDERTYPE", "ORDERSTATUS",
        "ALTMEDCODE", "MAPPEDNDC", "MAPPEDGPI", "MAPPEDNDC_CONF", "MAPPEDGPI_CONF", "VENUE", "MAP_USED", "HUM_MED_KEY", "LOCALDAYSUPPLIED", "LOCALFORM", "LOCALQTYOFDOSEUNIT",
        "LOCALTOTALDOSE", "LOCALDOSEFREQ", "LOCALDURATION", "LOCALINFUSIONRATE", "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "NDC11", "LOCALGPI",
        "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",  "LOCALDAW", "ORDERVSPRESCRIPTION", "HUM_GEN_MED_KEY", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID",
        "HGPID", "GRP_MPI", "LOCALGENERICDESC", "MSTRPROVID", "DISCONTINUEREASON", "ALLOWEDAMOUNT", "PAIDAMOUNT", "CHARGE", "DCC", "RXNORM_CODE", "ACTIVE_MED_FLAG", "ORIGIN")


    writeGenerated(CONSOLIDATED_BACKEND_SETH_GPIDS,dateStr, "CONSOLIDATED_CDR", cfg)

}



//ZH_MED_MAP_PREFNDC
def rxorder_dcdr_rxorder_export(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {

    var MAP_MED_DISC_REASON_D = readData(sqlContext, "MAP_MED_DISC_REASON", cfg).select("GROUPID", "LOCALCODE","CUI").withColumnRenamed("GROUPID","MDR_GROUPID").withColumnRenamed("LOCALCODE","MDR_LOCALCODE").withColumnRenamed("CUI","MDR_CUI")
    MAP_MED_DISC_REASON_D=MAP_MED_DISC_REASON_D.withColumn("MDR_GROUPID", upper(MAP_MED_DISC_REASON_D("MDR_GROUPID"))).distinct()

    var MAP_MED_ROUTE_D = readData(sqlContext, "MAP_MED_ROUTE",cfg).select("GROUPID", "LOCAL_CODE","CUI").withColumnRenamed("GROUPID", "MMR_GROUPID") .withColumnRenamed("LOCAL_CODE", "MMR_LOCAL_CODE").withColumnRenamed("CUI","MMR_CUI")
    MAP_MED_ROUTE_D=MAP_MED_ROUTE_D.withColumn("MMR_LOCAL_CODE", upper(MAP_MED_ROUTE_D("MMR_LOCAL_CODE"))).filter(MAP_MED_ROUTE_D("MMR_LOCAL_CODE").isin("TA", "NA") === false).distinct()

    var MAP_MED_SCHEDULE_D = readData(sqlContext, "MAP_MED_SCHEDULE", cfg).select("GROUPID", "LOCALCODE","CUI").withColumnRenamed("GROUPID", "MMS_GROUPID") .withColumnRenamed("LOCALCODE", "MMS_LOCALCODE").withColumnRenamed("CUI","MMS_CUI")
    MAP_MED_SCHEDULE_D=MAP_MED_SCHEDULE_D.withColumn("MMS_LOCALCODE", upper(MAP_MED_SCHEDULE_D("MMS_LOCALCODE"))).distinct()

    var MAP_MED_ORDER_STATUS_D = readData(sqlContext, "MAP_MED_ORDER_STATUS", cfg).select("GROUPID", "LOCALCODE","CUI").withColumnRenamed("GROUPID", "MMOS_GROUPID") .withColumnRenamed("LOCALCODE", "MMOS_LOCALCODE").withColumnRenamed("CUI","MMOS_CUI")
    MAP_MED_ORDER_STATUS_D=MAP_MED_ORDER_STATUS_D.withColumn("MMOS_LOCALCODE", upper(MAP_MED_ORDER_STATUS_D("MMOS_LOCALCODE"))).distinct()

    var ZCM_DCDR_EXCLUDE = readData(sqlContext, "ZCM_DCDR_EXCLUDE", cfg).select("CLIENT_DS_ID").withColumnRenamed("CLIENT_DS_ID", "EXC_CLIENT_DS_ID")

    var ZH_MED_MAP_PREFNDC = readData(sqlContext, "ZH_MED_MAP_PREFNDC", cfg).withColumnRenamed("GROUPID", "ZMMP_GROUPID").withColumnRenamed("LOCALROUTE", "ZMMP_LOCALROUTE").withColumnRenamed("PREFERREDNDC", "ZMMP_PREFERREDNDC")
            .withColumnRenamed("LOCALSTRENGTHPERDOSEUNIT", "ZMMP_LOCALSTRENGTHPERDOSEUNIT").withColumnRenamed("LOCALSTRENGTHUNIT", "ZMMP_LOCALSTRENGTHUNIT")
            .withColumnRenamed("LOCALFORM", "ZMMP_LOCALFORM").withColumnRenamed("DCC", "ZMMP_DCC")

    var V_RXORDER_PREFNDC = readGenerated(sqlContext,dateStr, "CONSOLIDATED_CDR", cfg)


    var clientDataSrc = sqlContext.read.parquet("/optum/crossix/data/rxorder/monthly/CDR_201710/METADATA/CLIENT_DATA_SRC").select("CLIENT_ID", "CLIENT_DATA_SRC_ID", "DCDR_EXCLUDE", "FINAL_RELEASE")
    clientDataSrc = clientDataSrc.filter("DCDR_EXCLUDE=='N' AND FINAL_RELEASE IS NULL")
    V_RXORDER_PREFNDC = V_RXORDER_PREFNDC.join(clientDataSrc, V_RXORDER_PREFNDC("GROUPID")===clientDataSrc("CLIENT_ID") and V_RXORDER_PREFNDC("CLIENT_DS_ID")===clientDataSrc("CLIENT_DATA_SRC_ID"), "inner")


    V_RXORDER_PREFNDC = V_RXORDER_PREFNDC.join(ZH_MED_MAP_PREFNDC,
        V_RXORDER_PREFNDC("GROUPID")===ZH_MED_MAP_PREFNDC("ZMMP_GROUPID") and V_RXORDER_PREFNDC("DCC")===ZH_MED_MAP_PREFNDC("ZMMP_DCC") and V_RXORDER_PREFNDC("LOCALROUTE").contains(ZH_MED_MAP_PREFNDC("ZMMP_LOCALROUTE")) and
                V_RXORDER_PREFNDC("LOCALFORM")===ZH_MED_MAP_PREFNDC("ZMMP_LOCALFORM") and V_RXORDER_PREFNDC("LOCALSTRENGTHUNIT")===ZH_MED_MAP_PREFNDC("ZMMP_LOCALSTRENGTHPERDOSEUNIT") and
                V_RXORDER_PREFNDC("LOCALSTRENGTHPERDOSEUNIT")===ZH_MED_MAP_PREFNDC("ZMMP_LOCALSTRENGTHPERDOSEUNIT"), "left_outer")

    var FACILITY_XREF = readData(sqlContext, "FACILITY_XREF", cfg).select("GROUPID", "CLIENT_DS_ID", "FACILITYID", "HGFACID").withColumnRenamed("GROUPID", "FX_GROUPID").withColumnRenamed("CLIENT_DS_ID", "FX_CLIENT_DS_ID").withColumnRenamed("FACILITYID", "FX_FACILITYID").withColumnRenamed("HGFACID", "FX_HGFACID")
    var PROVIDER_XREF = readData(sqlContext, "PROVIDER_XREF", cfg).select("GROUPID", "CLIENT_DS_ID", "PROVIDERID", "HGPROVID").withColumnRenamed("GROUPID", "PPX_GROUPID").withColumnRenamed("CLIENT_DS_ID", "PPX_CLIENT_DS_ID").withColumnRenamed("PROVIDERID", "PPX_PROVIDERID").withColumnRenamed("HGPROVID", "PPX_HGPROVID")

    var J1 = V_RXORDER_PREFNDC.join(ZCM_DCDR_EXCLUDE, V_RXORDER_PREFNDC("CLIENT_DS_ID")===ZCM_DCDR_EXCLUDE("EXC_CLIENT_DS_ID"), "left_outer")
    var J2 = J1.join(MAP_MED_DISC_REASON_D, J1("GROUPID")===MAP_MED_DISC_REASON_D("MDR_GROUPID") and upper(J1("DISCONTINUEREASON"))===MAP_MED_DISC_REASON_D("MDR_LOCALCODE"), "left_outer")
    var J3 = J2.join(MAP_MED_ROUTE_D, J2("GROUPID")===MAP_MED_ROUTE_D("MMR_GROUPID") and upper(J2("LOCALROUTE"))===MAP_MED_ROUTE_D("MMR_LOCAL_CODE"), "left_outer")
    var J4 = J3.join(MAP_MED_SCHEDULE_D, J3("GROUPID")===MAP_MED_SCHEDULE_D("MMS_GROUPID") and upper(J3("LOCALDOSEFREQ"))===MAP_MED_SCHEDULE_D("MMS_LOCALCODE"), "left_outer")
    var J5 = J4.join(MAP_MED_ORDER_STATUS_D, J4("GROUPID")===MAP_MED_ORDER_STATUS_D("MMOS_GROUPID") and upper(J4("ORDERSTATUS"))===MAP_MED_ORDER_STATUS_D("MMOS_LOCALCODE"), "left_outer")
    var J6 = J5.join(FACILITY_XREF, J5("GROUPID")===FACILITY_XREF("FX_GROUPID")  and J5("CLIENT_DS_ID")===FACILITY_XREF("FX_CLIENT_DS_ID") and J5("FACILITYID")===FACILITY_XREF("FX_FACILITYID"), "left_outer")
    var J7 = J6.join(PROVIDER_XREF, J6("GROUPID")===PROVIDER_XREF("PPX_GROUPID") and J6("CLIENT_DS_ID")===PROVIDER_XREF("PPX_CLIENT_DS_ID") and J6("LOCALPROVIDERID")===PROVIDER_XREF("PPX_PROVIDERID"), "left_outer")
    var J8 = J7.withColumn("PREFERREDNDC_SOURCE",  when(J7("LOCALNDC").isNotNull, "logicndc").when(J7("ZMMP_PREFERREDNDC").isNotNull, "logicndc").otherwise("mappedndc"))
            .select("ORDERVSPRESCRIPTION", "ENCOUNTERID", "GROUPID","FX_HGFACID","LOCALMEDCODE","PPX_HGPROVID","ISSUEDATE","ALTMEDCODE",
                "ORDERTYPE", "ORDERSTATUS","QUANTITYPERFILL","FILLNUM","RXID","LOCALSTRENGTHPERDOSEUNIT","LOCALSTRENGTHUNIT","LOCALROUTE", "MAPPEDNDC",
                "LOCALDESCRIPTION","DATASRC","VENUE","MAPPEDNDC_CONF","MAPPEDGPI","MAPPEDGPI_CONF","HUM_MED_KEY","MAP_USED","LOCALDAYSUPPLIED",
                "LOCALFORM","LOCALQTYOFDOSEUNIT","LOCALTOTALDOSE","LOCALDOSEFREQ","LOCALINFUSIONVOLUME","LOCALINFUSIONDURATION","LOCALNDC",
                "LOCALGPI","LOCALDOSEUNIT","LOCALDISCHARGEMEDFLG","LOCALDAW","HUM_GEN_MED_KEY","MSTRPROVID","GRP_MPI","HTS_GENERIC","HTS_GENERIC_EXT",
                "CLIENT_DS_ID","DCC","NDC11","MMR_CUI","MMS_CUI","MMOS_CUI","RXNORM_CODE", "DISCONTINUEDATE","EXPIREDATE", "MDR_CUI", "ZMMP_PREFERREDNDC",
                "PREFERREDNDC_SOURCE","PATIENTID","FACILITYID", "DISCONTINUEREASON")
            .withColumn("FACILITYID",J7("FX_HGFACID"))
            .withColumnRenamed("PPX_HGPROVID","LOCALPROVIDERID")
            .withColumnRenamed("LOCALDESCRIPTION","LOCALDESCRIPTION_PHI")
            .withColumn("DISCONTINUEREASON", J7("MDR_CUI"))
            .withColumnRenamed("MMR_CUI","ROUTE_CUI")
            .withColumnRenamed("MMS_CUI","DOSEFREQ_CUI")
            .withColumnRenamed("MMOS_CUI","MAPPED_ORDERSTATUS")
            .withColumn("ORDERTYPE", when(J7("ORDERTYPE").isin("CH002045", "CH002046", "CH002047"), J7("ORDERTYPE"))
                    .when(((J7("ORDERTYPE").isin("CH002045", "CH002046", "CH002047")===false) or (J7("ORDERTYPE").isNull)) and (J7("ORDERVSPRESCRIPTION")==="P"), "CH002047")
                    .when(((J7("ORDERTYPE").isin("CH002045", "CH002046", "CH002047")===false) or (J7("ORDERTYPE").isNull)) and (J7("ORDERVSPRESCRIPTION")==="O"), "CH002045"))
            .withColumn("DISCONTINUEDATE", J7("DISCONTINUEDATE"))
            .withColumn("EXPIREDATE", J7("EXPIREDATE"))
            .withColumn("PREFERREDNDC", when(J7("LOCALNDC").isNotNull, J7("LOCALNDC")).when(J7("ZMMP_PREFERREDNDC").isNotNull, J7("ZMMP_PREFERREDNDC")).otherwise(J7("MAPPEDNDC")))
            //.filter("EXC_CLIENT_DS_ID is null and GRP_MPI is not null")
            .filter("EXC_CLIENT_DS_ID is null")

    writeGenerated(J8, dateStr,"CONSOLIDATED_DCDR_EXPORT", cfg)
}

def fIsPhoneNumber(df:DataFrame, col:String ) :Column = {
    var column = df(col)
    var res = regexp_extract(column, "(^|[^0-9])1?([2-9][0-9]{2}(\\W|_){0,2}){2}[0-9]{4}($|[^0-9])", 0)
    when(trim(res)==="", column)
}




def rxorder_dcdr_rxorder_whitelist(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {
    var LDS_RXORDER_FE = readGenerated(sqlContext, dateStr,"CONSOLIDATED_DCDR_EXPORT", cfg)
    var GRP_MPI_XREF_IOT = readCommonData(sqlContext, "GRP_MPI_XREF_IOT", cfg).select("GROUPID", "GRP_MPI", "NEW_ID").withColumnRenamed("GROUPID", "G_GROUPID").withColumnRenamed("GRP_MPI", "G_GRP_MPI").withColumnRenamed("NEW_ID", "G_NEW_ID")
    var ENCOUNTER_XREF_IOT = readCommonData(sqlContext, "ENCOUNTER_XREF_IOT", cfg).select("GROUPID", "GRP_MPI", "ENCOUNTERID", "CLIENT_DS_ID", "HGENCID").withColumnRenamed("HGENCID", "E_HGENCID").withColumnRenamed("GROUPID", "E_GROUPID").withColumnRenamed("GRP_MPI", "E_GRP_MPI").withColumnRenamed("ENCOUNTERID", "E_ENCOUNTERID").withColumnRenamed("CLIENT_DS_ID", "E_CLIENT_DS_ID")
    var RXORDER_XREF_IOT = readCommonData(sqlContext, "RXORDER_XREF_IOT", cfg).select("GROUPID", "CLIENT_DS_ID", "RXID", "NEW_ID").withColumnRenamed("NEW_ID", "T1_NEW_ID").withColumnRenamed("GROUPID", "T1_GROUPID").withColumnRenamed("CLIENT_DS_ID", "T1_CLIENT_DS_ID").withColumnRenamed("RXID", "T1_RXID")

    var LOCALMEDCODE_XREF_IOT = readCommonData(sqlContext, "LOCALMEDCODE_XREF_IOT", cfg).select("GROUPID", "CLIENT_DS_ID", "LOCALMEDCODE", "MAPPED_VAL").withColumnRenamed("MAPPED_VAL", "LMC_MAPPED_VAL").withColumnRenamed("GROUPID", "LMC_GROUPID").withColumnRenamed("CLEANVAL", "LMC_CLEANVAL").withColumnRenamed("LOCALMEDCODE", "LMC_LOCALMEDCODE").withColumnRenamed("CLIENT_DS_ID", "LMC_CLIENT_DS_ID")

    var W_RXORDER_LOCALDOSEFREQ = readCommonData(sqlContext, "W_RXORDER_LOCALDOSEFREQ", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "LDF_GROUPID").withColumnRenamed("RAWVAL", "LDF_RAWVAL").withColumnRenamed("CLEANVAL", "LDF_CLEANVAL")

    var W_RXORDER_LOCALDESCRIPTION = readCommonData(sqlContext, "W_RXORDER_LOCALDESCRIPTION", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "LD_GROUPID").withColumnRenamed("RAWVAL", "LD_RAWVAL").withColumnRenamed("CLEANVAL", "LD_CLEANVAL")
    var W_RXORDER_LOCALDOSEUNIT  = readCommonData(sqlContext, "W_RXORDER_LOCALDOSEUNIT", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "LDU_GROUPID").withColumnRenamed("RAWVAL", "LDU_RAWVAL").withColumnRenamed("CLEANVAL", "LDU_CLEANVAL")

    var W_RXORDER_LOCALFORM  = readCommonData(sqlContext, "W_RXORDER_LOCALFORM", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "LF_GROUPID").withColumnRenamed("RAWVAL", "LF_RAWVAL").withColumnRenamed("CLEANVAL", "LF_CLEANVAL")
    var W_RXORDER_LOCALQTYOFDOSEUNIT  = readCommonData(sqlContext, "W_RXORDER_LOCALQTYOFDOSEUNIT", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "LQU_GROUPID").withColumnRenamed("RAWVAL", "LQU_RAWVAL").withColumnRenamed("CLEANVAL", "LQU_CLEANVAL")
    var W_RXORDER_LOCALSTRENGTHPERDO  = readCommonData(sqlContext, "W_RXORDER_LOCALSTRENGTHPERDO", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "LPU_GROUPID").withColumnRenamed("RAWVAL", "LPU_RAWVAL").withColumnRenamed("CLEANVAL", "LPU_CLEANVAL")
    var W_RXORDER_LOCALSTRENGTHUNIT  = readCommonData(sqlContext, "W_RXORDER_LOCALSTRENGTHUNIT", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "LSU_GROUPID").withColumnRenamed("RAWVAL", "LSU_RAWVAL").withColumnRenamed("CLEANVAL", "LSU_CLEANVAL")
    var W_RXORDER_QUANTITYPERFILL  = readCommonData(sqlContext, "W_RXORDER_QUANTITYPERFILL", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "QPF_GROUPID").withColumnRenamed("RAWVAL", "QPF_RAWVAL").withColumnRenamed("CLEANVAL", "QPF_CLEANVAL")
    var W_RXORDER_LOCALROUTE  = readCommonData(sqlContext, "W_RXORDER_LOCALROUTE", cfg).select("GROUPID", "RAWVAL", "CLEANVAL").withColumnRenamed("GROUPID", "RTE_GROUPID").withColumnRenamed("RAWVAL", "RTE_RAWVAL").withColumnRenamed("CLEANVAL", "RTE_CLEANVAL")
    var RESTRICTED_MEDS  = readCommonData(sqlContext, "RESTRICTED_MEDS", cfg).select("GROUPID", "SIMPLE_GENERIC_C").withColumnRenamed("GROUPID", "RM_GROUPID").withColumnRenamed("SIMPLE_GENERIC_C", "RM_SIMPLE_GENERIC_C")
    var ZH_MED_MAP_CDR_NEXT   = readCommonData(sqlContext, "ZH_MED_MAP_CDR_NEXT", cfg).select("GROUPID", "CLIENT_DS_ID", "DATASRC", "MAPPEDLOCALMEDCODE", "HTS_GPI").withColumnRenamed("HTS_GPI", "ZMM_HTS_GPI").withColumnRenamed("GROUPID", "ZMM_GROUPID").withColumnRenamed("CLIENT_DS_ID", "ZMM_CLIENT_DS_ID").withColumnRenamed("DATASRC", "ZMM_DATASRC").withColumnRenamed("MAPPEDLOCALMEDCODE", "ZMM_MAPPEDLOCALMEDCODE")

    LDS_RXORDER_FE = LDS_RXORDER_FE.withColumn("LOCALDOSEFREQ", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("LOCALDOSEFREQ"), "\\n", " "), "\\r", " "))))
            .withColumn("LOCALDESCRIPTION_PHI", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("LOCALDESCRIPTION_PHI"), "\\n", " "), "\\r", " "))))
            .withColumn("LOCALDOSEUNIT", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("LOCALDOSEUNIT"), "\\n", " "), "\\r", " "))))
            .withColumn("LOCALFORM", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("LOCALFORM"), "\\n", " "), "\\r", " "))))
            .withColumn("LOCALQTYOFDOSEUNIT", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("LOCALQTYOFDOSEUNIT"), "\\n", " "), "\\r", " "))))
            .withColumn("LOCALSTRENGTHUNIT", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("LOCALSTRENGTHUNIT"), "\\n", " "), "\\r", " "))))
            .withColumn("QUANTITYPERFILL", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("QUANTITYPERFILL"), "\\n", " "), "\\r", " "))))
            .withColumn("LOCALROUTE", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("LOCALROUTE"), "\\n", " "), "\\r", " "))))
            .withColumn("LOCALSTRENGTHPERDOSEUNIT", ltrim(rtrim(regexp_replace(regexp_replace(LDS_RXORDER_FE("LOCALSTRENGTHPERDOSEUNIT"), "\\n", " "), "\\r", " "))))

    var J1 = LDS_RXORDER_FE.join(GRP_MPI_XREF_IOT, LDS_RXORDER_FE("GROUPID")===GRP_MPI_XREF_IOT("G_GROUPID") and LDS_RXORDER_FE("GRP_MPI")===GRP_MPI_XREF_IOT("G_GRP_MPI"), "left_outer")
    var J2 = J1.join(ENCOUNTER_XREF_IOT, J1("GROUPID")===ENCOUNTER_XREF_IOT("E_GROUPID") and J1("CLIENT_DS_ID")===ENCOUNTER_XREF_IOT("E_CLIENT_DS_ID") and J1("ENCOUNTERID")===ENCOUNTER_XREF_IOT("E_ENCOUNTERID"), "left_outer")
    //var J2 = J1.join(ENCOUNTER_XREF_IOT, J1("GROUPID")===ENCOUNTER_XREF_IOT("E_GROUPID") and J1("CLIENT_DS_ID")===ENCOUNTER_XREF_IOT("E_CLIENT_DS_ID") and J1("ENCOUNTERID")===ENCOUNTER_XREF_IOT("E_ENCOUNTERID")and J1("G_NEW_ID")===ENCOUNTER_XREF_IOT("E_GRP_MPI"), "left_outer")
    var J3 = J2.join(RXORDER_XREF_IOT, J2("GROUPID")===RXORDER_XREF_IOT("T1_GROUPID") and J2("CLIENT_DS_ID")===RXORDER_XREF_IOT("T1_CLIENT_DS_ID") and J2("RXID")===RXORDER_XREF_IOT("T1_RXID"), "left_outer")
    var J4 = J3.join(LOCALMEDCODE_XREF_IOT, J3("GROUPID")===LOCALMEDCODE_XREF_IOT("LMC_GROUPID") and J3("CLIENT_DS_ID")===LOCALMEDCODE_XREF_IOT("LMC_CLIENT_DS_ID") and J3("LOCALMEDCODE")===LOCALMEDCODE_XREF_IOT("LMC_LOCALMEDCODE"), "left_outer")
    var J5 = J4.join(W_RXORDER_LOCALDOSEFREQ, J4("GROUPID")===W_RXORDER_LOCALDOSEFREQ("LDF_GROUPID") and trim(J4("LOCALDOSEFREQ"))===W_RXORDER_LOCALDOSEFREQ("LDF_RAWVAL"), "left_outer")
    var J6 = J5.join(W_RXORDER_LOCALDESCRIPTION, J5("GROUPID")===W_RXORDER_LOCALDESCRIPTION("LD_GROUPID") and trim(J5("LOCALDESCRIPTION_PHI"))===W_RXORDER_LOCALDESCRIPTION("LD_RAWVAL"), "left_outer")
    var J7 = J6.join(W_RXORDER_LOCALDOSEUNIT, J6("GROUPID")===W_RXORDER_LOCALDOSEUNIT("LDU_GROUPID") and trim(J6("LOCALDOSEUNIT"))===W_RXORDER_LOCALDOSEUNIT("LDU_RAWVAL"), "left_outer")
    J7 = J7.join(W_RXORDER_LOCALFORM, J7("GROUPID")===W_RXORDER_LOCALFORM("LF_GROUPID") and trim(J7("LOCALFORM"))===W_RXORDER_LOCALFORM("LF_RAWVAL"), "left_outer")
    J7 = J7.join(W_RXORDER_LOCALQTYOFDOSEUNIT, J7("GROUPID")===W_RXORDER_LOCALQTYOFDOSEUNIT("LQU_GROUPID") and J7("LOCALQTYOFDOSEUNIT")===W_RXORDER_LOCALQTYOFDOSEUNIT("LQU_RAWVAL"), "left_outer")
    var J8 = J7.join(W_RXORDER_LOCALSTRENGTHPERDO, J7("GROUPID")===W_RXORDER_LOCALSTRENGTHPERDO("LPU_GROUPID") and J7("LOCALSTRENGTHPERDOSEUNIT")===W_RXORDER_LOCALSTRENGTHPERDO("LPU_RAWVAL"), "left_outer")
    var J9 = J8.join(W_RXORDER_LOCALSTRENGTHUNIT, J8("GROUPID")===W_RXORDER_LOCALSTRENGTHUNIT("LSU_GROUPID") and J8("LOCALSTRENGTHUNIT")===W_RXORDER_LOCALSTRENGTHUNIT("LSU_RAWVAL"), "left_outer")
    var J10 = J9.join(W_RXORDER_QUANTITYPERFILL, J9("GROUPID")===W_RXORDER_QUANTITYPERFILL("QPF_GROUPID") and J9("QUANTITYPERFILL")===W_RXORDER_QUANTITYPERFILL("QPF_RAWVAL"), "left_outer")
    var J11 = J10.join(W_RXORDER_LOCALROUTE, J10("GROUPID")===W_RXORDER_LOCALROUTE("RTE_GROUPID") and J10("LOCALROUTE")===W_RXORDER_LOCALROUTE("RTE_RAWVAL"), "left_outer")
    var J12 = J11.join(RESTRICTED_MEDS, J11("GROUPID")===RESTRICTED_MEDS("RM_GROUPID") and J11("DCC")===RESTRICTED_MEDS("RM_SIMPLE_GENERIC_C"), "left_outer")
    var J13 = J12.join(ZH_MED_MAP_CDR_NEXT, J12("GROUPID")===ZH_MED_MAP_CDR_NEXT("ZMM_GROUPID") and J12("CLIENT_DS_ID")===ZH_MED_MAP_CDR_NEXT("ZMM_CLIENT_DS_ID")and J12("DATASRC")===ZH_MED_MAP_CDR_NEXT("ZMM_DATASRC")and J12("LMC_MAPPED_VAL")===ZH_MED_MAP_CDR_NEXT("ZMM_MAPPEDLOCALMEDCODE"), "left_outer")
    var J15 = J13.filter("RM_SIMPLE_GENERIC_C IS NULL")
    var J16 = J15
            .withColumn("ORDERVSPRESCRIPTION", trim(J15("ORDERVSPRESCRIPTION")))
            .withColumn("ENCOUNTERID", trim(J15("E_HGENCID")))
            .withColumn("GROUPID", trim(J15("GROUPID")))
            .withColumn("FACILITYID", trim(J15("FACILITYID")))
            .withColumn("LOCALMEDCODE", trim(J15("LMC_MAPPED_VAL")))
            .withColumn("LOCALPROVIDERID", trim(J15("LOCALPROVIDERID")))
            .withColumn("ALTMEDCODE", trim(J15("ALTMEDCODE")))
            .withColumn("ORDERTYPE", trim(J15("ORDERTYPE")))
            .withColumn("ORDERSTATUS", trim(J15("ORDERSTATUS")))
            .withColumn("QUANTITYPERFILL", trim(J15("QPF_CLEANVAL")))
            .withColumn("RXID", trim(J15("T1_NEW_ID")))
            .withColumn("LOCALSTRENGTHPERDOSEUNIT", trim(J15("LPU_CLEANVAL")))
            .withColumn("LOCALSTRENGTHUNIT", trim(J15("LSU_CLEANVAL")))
            .withColumn("LOCALROUTE", trim(J15("RTE_CLEANVAL")))
            .withColumn("MAPPEDNDC", trim(J15("MAPPEDNDC")))
            .withColumn("LOCALDESCRIPTION_PHI", trim(J15("LD_CLEANVAL")))
            .withColumn("DATASRC", trim(J15("DATASRC")))
            .withColumn("VENUE", trim(J15("VENUE")))
            .withColumn("MAPPEDGPI", trim(J15("ZMM_HTS_GPI")))
            .withColumn("HUM_MED_KEY", trim(J15("HUM_MED_KEY")))
            .withColumn("MAP_USED", trim(J15("MAP_USED")))
            .withColumn("LOCALFORM", trim(J15("LF_CLEANVAL")))
            .withColumn("LOCALQTYOFDOSEUNIT", trim(J15("LQU_CLEANVAL")))
            .withColumn("LOCALTOTALDOSE", fIsPhoneNumber(J15, "LOCALTOTALDOSE"))
            .withColumn("LOCALINFUSIONVOLUME", trim(J15("LOCALINFUSIONVOLUME")))
            .withColumn("LOCALINFUSIONDURATION", trim(J15("LOCALINFUSIONDURATION")))
            .withColumn("LOCALNDC", trim(J15("LOCALNDC")))
            .withColumn("LOCALGPI", trim(J15("LOCALGPI")))
            .withColumn("LOCALDOSEUNIT", trim(J15("LDU_CLEANVAL")))
            .withColumn("LOCALDISCHARGEMEDFLG", trim(J15("LOCALDISCHARGEMEDFLG")))
            .withColumn("LOCALDAW", trim(J15("LOCALDAW")))
            .withColumn("HUM_GEN_MED_KEY", trim(J15("HUM_GEN_MED_KEY")))
            .withColumn("MSTRPROVID", trim(J15("MSTRPROVID")))
            .withColumn("HTS_GENERIC", trim(J15("HTS_GENERIC")))
            .withColumn("HTS_GENERIC_EXT", trim(J15("HTS_GENERIC_EXT")))
            .withColumn("DISCONTINUEREASON", trim(J15("DISCONTINUEREASON")))
            .withColumn("DCC", trim(J15("DCC")))
            .withColumn("NDC11", trim(J15("NDC11")))
            .withColumn("ROUTE_CUI", trim(J15("ROUTE_CUI")))
            .withColumn("DOSEFREQ_CUI", trim(J15("DOSEFREQ_CUI")))
            .withColumn("MAPPED_ORDERSTATUS", trim(J15("MAPPED_ORDERSTATUS")))
            .withColumn("RXNORM_CODE", trim(J15("RXNORM_CODE")))
            .withColumn("PREFERREDNDC", trim(J15("PREFERREDNDC")))
            .withColumn("LOCALDOSEFREQ", trim(J15("LDF_CLEANVAL")))
            .withColumn("CDR_GRP_MPI", trim(J15("GRP_MPI")))
            .withColumn("DCDR_GRP_MPI", trim(J15("G_NEW_ID")))
            .withColumn("ORIGIN", when(J15("G_NEW_ID").isNotNull, "DCDR").when(J15("GRP_MPI").isNotNull, "CDR")otherwise("SOURCE"))


    var J17 = J16.select("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI", "ORDERVSPRESCRIPTION",
        "ENCOUNTERID", "GROUPID", "FACILITYID", "LOCALMEDCODE", "LOCALPROVIDERID", "ISSUEDATE",
        "ALTMEDCODE", "ORDERTYPE", "ORDERSTATUS", "QUANTITYPERFILL", "FILLNUM", "RXID", "LOCALSTRENGTHPERDOSEUNIT", "LOCALSTRENGTHUNIT",
        "LOCALROUTE", "MAPPEDNDC", "LOCALDESCRIPTION_PHI", "DATASRC", "VENUE", "MAPPEDNDC_CONF", "MAPPEDGPI", "MAPPEDGPI_CONF",
        "HUM_MED_KEY", "MAP_USED", "LOCALDAYSUPPLIED", "LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ",
        "LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION", "LOCALNDC", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG",
        "LOCALDAW", "HUM_GEN_MED_KEY", "MSTRPROVID", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID", "DISCONTINUEDATE", "EXPIREDATE",
        "DISCONTINUEREASON", "DCC", "NDC11", "ROUTE_CUI", "DOSEFREQ_CUI", "MAPPED_ORDERSTATUS", "RXNORM_CODE", "PREFERREDNDC",
        "PREFERREDNDC_SOURCE", "ORIGIN")
    writeGenerated(J17, dateStr,"CONSOLIDATED_DCDR_WHITELIST", cfg)
}




def rxorder_dcdr_rxorder_import(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {
    var IMPORT_RXORDER = readGenerated(sqlContext, dateStr,"CONSOLIDATED_DCDR_WHITELIST", cfg)
    var RXORDER_FOR_LS = IMPORT_RXORDER.select("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI", "ORDERVSPRESCRIPTION", "ENCOUNTERID",
        "GROUPID", "FACILITYID", "LOCALMEDCODE", "LOCALPROVIDERID", "ISSUEDATE", "ALTMEDCODE", "ORDERTYPE", "ORDERSTATUS", "QUANTITYPERFILL", "FILLNUM", "RXID", "LOCALSTRENGTHPERDOSEUNIT",
        "LOCALSTRENGTHUNIT", "LOCALROUTE", "MAPPEDNDC", "LOCALDESCRIPTION_PHI","DATASRC", "VENUE", "MAPPEDNDC_CONF", "MAPPEDGPI","MAPPEDGPI_CONF", "HUM_MED_KEY",
        "MAP_USED", "LOCALDAYSUPPLIED","LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ","LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION",
        "LOCALNDC", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG","LOCALDAW", "HUM_GEN_MED_KEY","MSTRPROVID", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID",
        "DISCONTINUEDATE", "EXPIREDATE","DISCONTINUEREASON", "DCC", "NDC11", "ROUTE_CUI","DOSEFREQ_CUI", "MAPPED_ORDERSTATUS","RXNORM_CODE", "PREFERREDNDC", "PREFERREDNDC_SOURCE", "ORIGIN")

    var ALLOBJECTIDS = readCommonData(sqlContext, "ALL_GROUPIDS", cfg).select("GROUPID").distinct()
    var j1 = RXORDER_FOR_LS.join(ALLOBJECTIDS, Seq("GROUPID"), "inner")

    j1 = j1.select("PATIENTID", "CDR_GRP_MPI", "DCDR_GRP_MPI", "ORDERVSPRESCRIPTION", "ENCOUNTERID",
        "GROUPID", "FACILITYID", "LOCALMEDCODE", "LOCALPROVIDERID", "ISSUEDATE", "ALTMEDCODE", "ORDERTYPE", "ORDERSTATUS", "QUANTITYPERFILL", "FILLNUM", "RXID", "LOCALSTRENGTHPERDOSEUNIT",
        "LOCALSTRENGTHUNIT", "LOCALROUTE", "MAPPEDNDC", "LOCALDESCRIPTION_PHI","DATASRC", "VENUE", "MAPPEDNDC_CONF", "MAPPEDGPI","MAPPEDGPI_CONF", "HUM_MED_KEY",
        "MAP_USED", "LOCALDAYSUPPLIED","LOCALFORM", "LOCALQTYOFDOSEUNIT", "LOCALTOTALDOSE", "LOCALDOSEFREQ","LOCALINFUSIONVOLUME", "LOCALINFUSIONDURATION",
        "LOCALNDC", "LOCALGPI", "LOCALDOSEUNIT", "LOCALDISCHARGEMEDFLG","LOCALDAW", "HUM_GEN_MED_KEY","MSTRPROVID", "HTS_GENERIC", "HTS_GENERIC_EXT", "CLIENT_DS_ID",
        "DISCONTINUEDATE", "EXPIREDATE","DISCONTINUEREASON", "DCC", "NDC11", "ROUTE_CUI","DOSEFREQ_CUI", "MAPPED_ORDERSTATUS","RXNORM_CODE", "PREFERREDNDC", "PREFERREDNDC_SOURCE", "ORIGIN")

    j1 = j1.withColumn("LOCALTOTALDOSE", when(j1("DATASRC")==="medorders", "NULL").otherwise(j1("LOCALTOTALDOSE")))
    j1 = j1.withColumn("LOCALTOTALDOSE", when(j1("LOCALTOTALDOSE")!=="NULL", j1("LOCALTOTALDOSE")))

    writeGenerated(j1, dateStr,"RXORDER", cfg)

}



def runRxOrder(sqlContext: SQLContext, dateStr:String, cfg: Map[String,String]) = {

    println ("Running AS Rxorder...")
    processASRxOrder(sqlContext, dateStr, cfg, new RxordersandprescriptionsErx(cfg))
    println ("Running EP2 Rxorder...")
    processEP2RxOrder(sqlContext,  dateStr, new RxordersandprescriptionsMedorders(cfg), cfg)
    println ("Running CernerV2 Rxorder...")
    processCR2RxOrder(sqlContext, dateStr,cfg)
    println ("Merging all RxOrders..")
    mergeAllRxOrders(sqlContext,dateStr, cfg)

    println ("Running rxorder_backend_med_mapping..")
    rxorder_backend_med_mapping(sqlContext,dateStr, cfg)

    println ("Running rxorder_backend_seth_gpids..")
    rxorder_backend_seth_gpids(sqlContext,dateStr, cfg)

    println ("Running rxorder_dcdr_rxorder_export..")
    rxorder_dcdr_rxorder_export(sqlContext, dateStr,cfg)

    println ("Running rxorder_dcdr_rxorder_whitelist..")
    rxorder_dcdr_rxorder_whitelist(sqlContext,dateStr, cfg)

    println ("Running rxorder_dcdr_rxorder_import..")
    rxorder_dcdr_rxorder_import(sqlContext,dateStr, cfg)
}



