package com.humedica.mercury.etl.crossix.util

import java.sql.DriverManager
import java.util.{ArrayList, HashMap}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

// processRxORderDataPreparation(sqlContext, "20180503,20180504,20180505,20180506,20180507,20180508,20180509", cfg)

/**
  * Created by rbabu on 12/2/17.
  */
object CrossixRxOrderDataPrepUtil {

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

    def fetchAllFileIdMap(sqlContext: SQLContext, dateStr: String, module: String, table: String, cfg: Map[String, String], supportedGroupIds: String) = {
        var map = new HashMap[String, ArrayList[String]]()

        var connectstr = cfg.get("FASTTRACT_CONNECTSTR").get
        var username = cfg.get("FASTTRACT_USER").get
        var pwd = cfg.get("FASTTRACT_PWD").get

        Class.forName("oracle.jdbc.driver.OracleDriver");
        var conn = DriverManager.getConnection(connectstr, username, pwd)
        var stmt = conn.createStatement()
        var dateStr1 = "'" + dateStr.replaceAll(",", "','") + "'"

        var qry = "SELECT SME.FILE_ID, FTM.SCHEMA, SME.GROUPID FROM SHELF_MANIFEST_ENTRY_FT SME, FILE_TRANSFORMED_MESSAGES FTM WHERE  " +
                "SME.FILE_ID=FTM.FILE_ID AND UPPER(FTM.SCHEMA) LIKE '%" + module + "%' AND UPPER(FTM.TABLE_NAME) = '" + table + "' " +
                "AND SUBSTR(TO_CHAR(SME.TRACK_TIME, 'YYYYMMDD'),1,9) IN (" + dateStr1 + ") AND SME.GROUPID IN (" + supportedGroupIds + ")"
        println("Running query :"+qry)
        var rs = stmt.executeQuery(qry)

        while (rs.next()) {
            var fileId = rs.getString(1)
            var schemaId = rs.getString(2)
                var group = rs.getString(3)
                var key = schemaId + ":" + table
                var value = map.get(key)
                if (value == null) {
                    value = new ArrayList[String]()
                    value.add(fileId)
                }
                else {
                    value.add(fileId)
                }
                map.put(key, value)
        }
        stmt.close()
        conn.close()
        println(map)
        writeRxOrderStageFiles(sqlContext, map, dateStr, cfg)
    }


    def writeRxOrderStageFiles(sqlContext: SQLContext, map: HashMap[String, ArrayList[String]], dateStr: String, cfg: Map[String, String]) = {
        var home_dir = cfg.get("HOME_DIR").get
        var dt1 = dateStr.split(",")
        var dt = dt1(0) + "_" + dt1(dt1.length - 1)
        map.keySet().toArray().foreach(f => {
            var key = f.toString.split(":");
            var value = map.get(f)
            var schemaid = key(0)
            var table = key(1)
            var destDir = home_dir + "/data/rxorder/weekly/" + dt + "/" + table + "/STAGING/"
            var src = cfg.get("HADOOP_STAGING_SRC").get.format(schemaid, table)
            println("HADOOP_STAGING_SRC ==> "+src)
            src.split(",").foreach(file => {
                try {
                    var df = sqlContext.read.parquet(file)
                    var vals = "('" + String.join("','", value) + "')"
                    println("FILEID in " + vals)
                    var fdf = df.filter("FILEID in " + vals)
                    fdf = fdf.withColumn("FILE_ID", fdf("FILEID").cast("String"))
                    var c = fdf.count()
                    if (c == 0) {
                        println(" FAILED# ### " + src + " ==> " + vals + " ==> " + c)
                    }
                    else {
                        println(" SUCCESS ### " + src + " ==> " + vals + " ==> " + c + " AND WRITING TO " + destDir)
                        fdf.write.mode("append").parquet(destDir)
                    }
                }catch {
                    case ex:Exception=> println(ex.getMessage)
                }
                })
        })
    }
//processRxORderDataPreparation(sqlContext, "20180503,20180504,20180505,20180506,20180507,20180508,20180509", cfg, "_EP2_#PATREG)
    // processRxORderDataPreparation(sqlContext, "20180503,20180504,20180505,20180506,20180507,20180508,20180509", cfg, "_EP2_#PATREG,_EP2_#MEDORDERS,_AS_#AS_ERX,_CR2_#ORDERS")
    // processRxORderDataPreparation(sqlContext, "20180503,20180504,20180505,20180506,20180507,20180508,20180509", cfg, "_AS_#AS_PATDEM,_CR2_#ENCOUNTER,_CR2_#ENC_ALIAS,_CR2_#HUM_DRG")
    // processRxORderDataPreparation(sqlContext, "20180503,20180504,20180505,20180506,20180507,20180508,20180509", cfg, "_CR2_#ORDER_ACTION,_CR2_#ORDER_DETAIL,_CR2_#PERSON,_CR2_#PERSON_ALIAS")
    // processRxORderDataPreparation(sqlContext, "20180503,20180504,20180505,20180506,20180507,20180508,20180509", cfg, "_EP2_#PATIENT_FYI_FLAGS,_EP2_#PAT_RACE,_EP2_#PATIENT_3,_EP2_#IDENTITY_ID")

    //processRxORderDataPreparation(sqlContext, "20180501", cfg, "_EP2_#PATREG,_EP2_#MEDORDERS,_AS_#AS_ERX,_CR2_#ORDERS")

    def processRxORderDataPreparation(sqlContext: SQLContext, dateStr: String, cfg: Map[String, String], RXORDER_TABLE_PAIR:String) = {
        try {
            var dt1 = dateStr.split(",")
            var dt = dt1(0) + "_" + dt1(dt1.length - 1)

            var supportedGroupIds = fetchAllSupportedGroupIds(cfg)
            var tablePairs = RXORDER_TABLE_PAIR
            var successList = new ArrayList[String]()
            var failedList = new ArrayList[String]()
            tablePairs.split(",").foreach(tblPair => {
                var g = tblPair.split("#")
                var module = g(0)
                var hdp_table = g(1)
                try {
                    fetchAllFileIdMap(sqlContext, dateStr, module, hdp_table, cfg, supportedGroupIds)
                } catch {
                    case ex: Exception => println("ERROR :" + module + ":" + hdp_table + " Please check and add manually ===> " + ex.getMessage, dt)
                }
            })
        } catch {
            case ex: Exception => ex.printStackTrace()
        }
    }


    def processAddress(sqlContext: SQLContext, dateStr: String, cfg: Map[String, String])={
        var home_dir = cfg.get("HOME_DIR").get

        var s = sqlContext.read.parquet(home_dir+"/data/rxorder/weekly/"+dateStr+"/ADDRESS/*")
        s.select("FILEID", "ZIPCODE", "PARENT_ENTITY_ID").write.parquet(home_dir + "/data/rxorder/weekly/"+dateStr+"/ADDRESS_V1")
    }


    def writeDictionaryTable(sQLContext: SQLContext,dateStr:String, module:String, table:String, cfg: Map[String, String]) = {
        println("Preparing dictionary tables "+table)
        var src = cfg.get("STAGING_BASEDIR").get
        var home_dir = cfg.get("HOME_DIR").get
        var fs = FileSystem.get(new Configuration())

        var files = fs.listStatus(new Path(src))
        files.foreach(f=> {
            if(f.getPath.toString.contains(module)) {
                var path = new Path(f.getPath, table + "/*")
                var dest = home_dir+"/data/rxorder/weekly/"+dateStr+"/"+table
                println("Copying " + path.toString +" to "+dest)
                var s = sQLContext.read.parquet(path.toString)
                s.withColumn("FILE_ID", s("FILEID").cast("String")).write.mode("append").parquet(dest)
            }
        })
    }

    def prepareDictionaryTables(sqlContext: SQLContext, dateStr: String, cfg: Map[String, String])={
        writeDictionaryTable(sqlContext, dateStr, "_AS_", "AS_ZC_MEDICATION_DE", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_EP2_", "ZH_STATE", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_EP2_", "ZH_CLARITYDRG", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_EP2_", "MEDADMINREC", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_EP2_", "ZH_CLARITYDEPT", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_EP2_", "ZH_BENEPLAN", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_EP2_", "ZH_MED_UNIT", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_CR2_", "ZH_CODE_VALUE",cfg)
        writeDictionaryTable(sqlContext, dateStr, "_CR2_", "ZH_MED_IDENTIFIER", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_CR2_", "ZH_NOMENCLATURE", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_CR2_", "ZH_ORDER_CATALOG_ITEM_R", cfg)
        writeDictionaryTable(sqlContext, dateStr, "_CR2_", "ADDRESS", cfg)
    }




    def createSHELF_MANIFEST_ENTRY_WCLSID(sqlContext:SQLContext, dateStr:String, cfg: Map[String, String])={
        var home_dir = cfg.get("HOME_DIR").get

        var src = cfg.get("STAGING_BASEDIR").get

        var shmPath = cfg.get("SHELF_MANIFEST_ENTRY_FT").get //"/optum/crossix/data/prov/201804/SHELF_MANIFEST_ENTRY_FT/*"
        var clientDataSrcPath=cfg.get("CLIENT_DATA_SRC").get //"/optum/crossix/data/rxorder/monthly/CDR_201804/CLIENT_DATA_SRC/*"
        var clientDataStreamPath = cfg.get("CLIENT_DATA_STREAM").get //"/optum/crossix/data/rxorder/monthly/CDR_201804/CLIENT_DATA_STREAM/*"

        //var s = readGenerated(sqlContext, "SHELF_MANIFEST_ENTRY", cfg).select("FILE_ID", "GROUPID", "DATA_STREAM_ID")
        var s = sqlContext.read.parquet(shmPath).select("FILE_ID", "GROUPID", "DATA_STREAM_ID","TRACK_TIME")
        var clientDataSrc = sqlContext.read.parquet(clientDataSrcPath).select("client_data_src_id","client_id", "DCDR_EXCLUDE")
        clientDataSrc = clientDataSrc.filter("DCDR_EXCLUDE == 'N'")
        var clientDataStream = sqlContext.read.parquet(clientDataStreamPath).select("client_data_src_id","client_data_stream_id")
        var j = clientDataSrc.join(clientDataStream, Seq("client_data_src_id"), "inner")
        s = s.join(j, s("GROUPID")===j("CLIENT_ID") and s("DATA_STREAM_ID")===j("client_data_stream_id") , "inner")

        //var j2=j1.select("FILE_ID", "GROUPID","CLIENT_DATA_SRC_ID")
        s = s.withColumn("CLIENT_DATA_SRC_ID", when(s("GROUPID")=== "H340651" and s("CLIENT_DATA_SRC_ID")=== "1741", "46")
                .when(s("GROUPID")=== "H262866" and s("CLIENT_DATA_SRC_ID")=== "1241", "581")
                .when(s("GROUPID")=== "H416989" and s("CLIENT_DATA_SRC_ID")=== "962", "221")
                .when(s("GROUPID")=== "H984442" and s("CLIENT_DATA_SRC_ID")=== "961", "421")
                .when(s("GROUPID")=== "H667594" and s("CLIENT_DATA_SRC_ID")=== "963", "57").otherwise(s("CLIENT_DATA_SRC_ID")))
        s.write.parquet(home_dir + "/data/rxorder/weekly/"+dateStr+"/SHELF_MANIFEST_ENTRY_WCLSID")
    }


    def convertParquet2csv(sqlContext: SQLContext, dateStr: String, cfg: Map[String, String]): Unit =
    {
        parquet2csv(sqlContext,dateStr,"RXORDER", cfg)
        parquet2csv(sqlContext,dateStr,"RXORDER_PHI", cfg)
    }

    def parquet2csv(sqlContext: SQLContext, dateStr: String, table:String, cfg: Map[String, String]): String = {
        var home_dir = cfg.get("HOME_DIR").get

        var src = home_dir +"/data/rxorder/weekly/"+dateStr+"/GENERATED/"+table
        var upload = home_dir + "/data/rxorder/weekly/"+dateStr+"/UPLOAD/"+table+"_"+dateStr+".gz"
        var dest = home_dir + "/data/rxorder/weekly/"+dateStr+"/UPLOAD/"+table+"_OUTPUT"
        sqlContext.read.parquet(src).write.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\001").option("codec", "gzip").save(dest)
        var hdp =  "hadoop fs -text "+dest+"/*.gz  | gzip | hadoop fs -put - "+upload
        println(hdp)
        hdp
    }

    def convertCsv2GZ(sqlContext: SQLContext, dateStr: String, sc:SparkContext, cfg: Map[String, String]): Unit =
    {
        csv2gz(sqlContext,dateStr,"RXORDER", sc, cfg)
        csv2gz(sqlContext,dateStr,"RXORDER_PHI", sc, cfg)
    }

    def csv2gz(sqlContext: SQLContext, dateStr: String, table:String, sc:SparkContext, cfg: Map[String, String]) = {
        var home_dir = cfg.get("HOME_DIR").get

        var src = home_dir + "/data/rxorder/weekly/"+dateStr+"/GENERATED/"+table
        var upload = home_dir + "/data/rxorder/weekly/"+dateStr+"/UPLOAD/"+table+"_"+dateStr+".gz"
        var c1 = sqlContext.read.parquet(src).count
        var c2 = sc.textFile(upload).count
        println(src +"===>"+c1)
        println(upload +"===>"+c2)

        if(c1 == c2) {
            println("hadoop fs -copyToLocal "+upload)
            println(table+"_"+dateStr+".gz" + " : " + c1)

        }
        else {
            println("ERROR "+dateStr+ " : "+table)
        }
    }

//

    //investigatemain2(sqlContext, "20180412,20180413,20180414,20180415,20180416,20180417,20180418", cfg)
    //investigatemain2(sqlContext, "20180405,20180406,20180407,20180408,20180409,20180410,20180411", cfg)
    //investigatemain2(sqlContext, "20180329,20180330,20180331,20180401,20180402,20180403,20180404", cfg)

    def investigatemain2(sqlContext: SQLContext, dateStr:String, cfg: Map[String, String]) = {

        var supportedGroupIds = fetchAllSupportedGroupIds(cfg)

        println("--------- BEGIN ------- ")
        println("Supported GroupIds "+supportedGroupIds)
        investigatemain1(sqlContext, "MEDORDERS", dateStr, "_EP2_", cfg,supportedGroupIds)
        investigatemain1(sqlContext, "AS_ERX", dateStr, "_AS_", cfg,supportedGroupIds)
        investigatemain1(sqlContext, "ORDERS", dateStr, "_CR2_", cfg,supportedGroupIds)
        println("--------- END ------- ")
    }

    def investigatemain1(sqlContext: SQLContext, table:String, dates:String, module:String, cfg: Map[String, String], supportedGroupIds:String) = {
        var dtArray = dates.split(",")
        var dateStr = dtArray(0)+"_"+dtArray(6)
        var home_dir = "/user/rbabu/data/rxorder/weekly/"+dateStr
        println(home_dir)

        dtArray.foreach(d=>{
            investigate(sqlContext, home_dir, d, module, table, cfg, supportedGroupIds);
        })

    }


    def investigate(sqlContext: SQLContext, homedir:String, dateStr: String, module: String, table: String, cfg: Map[String, String], supportedGroupIds:String) = {

        var map = new HashMap[String, ArrayList[String]]()

        var connectstr = cfg.get("FASTTRACT_CONNECTSTR").get
        var username = cfg.get("FASTTRACT_USER").get
        var pwd = cfg.get("FASTTRACT_PWD").get

        Class.forName("oracle.jdbc.driver.OracleDriver");
        var conn = DriverManager.getConnection(connectstr, username, pwd)
        var stmt = conn.createStatement()
        var dateStr1 = "'" + dateStr.replaceAll(",", "','") + "'"

        var qry = "SELECT SME.FILE_ID, FTM.SCHEMA, SME.GROUPID FROM SHELF_MANIFEST_ENTRY_FT SME, FILE_TRANSFORMED_MESSAGES FTM WHERE  " +
                "SME.FILE_ID=FTM.FILE_ID AND UPPER(FTM.SCHEMA) LIKE '%" + module + "%' AND UPPER(FTM.TABLE_NAME) = '" + table + "' " +
                "AND SUBSTR(TO_CHAR(SME.TRACK_TIME, 'YYYYMMDD'),1,9) IN (" + dateStr1 + ") AND SME.GROUPID IN (" + supportedGroupIds +")"
        //println("\n\nRunning query :"+qry)
        var rs = stmt.executeQuery(qry)
        var list = ListBuffer[String]()
        while (rs.next()) {
            var fileId = rs.getString(1)
            var schemaId = rs.getString(2)
            var group = rs.getString(3)
            var key = schemaId + ":" + fileId+ " : "+group
            list  += key
        }
        stmt.close()
        conn.close()
        var expectedCount = list.size

        //println(dateStr+" : EXPECTED FILEIDS "+list)
        var df = sqlContext.read.parquet(homedir+"/"+table+"/STAGING/*")
        var list1 = ListBuffer[String]()

        list.foreach(fKey=> {
            var fId = fKey.split(":")(1)
            var count = df.filter("FILE_ID ="+fId).count()
           // println(fId+"==>"+count)
            if(count == 0) {
                list1 += fKey
            }
        })
        println(dateStr+" : MISSING COUNT for table==>"+table+"==>"+list1.size+"/"+expectedCount+"===>"+list1)

    }




    def rxphi_inv(sqlContext: SQLContext) = {
        var baseDir = "/user/rbabu/data/rxorder/weekly/"
        var destDir = "/user/rbabu/data/rxorder/LSDIP29/RXORDER_PHI_FIX"
        var fs = FileSystem.get(new Configuration())
        fs.listStatus(new Path(baseDir)).foreach(f=> {
            try {
                var rxop = f.getPath.toString + "/GENERATED/RXORDER_PHI_FIX/*"
                    if (rxop.contains("2017") || rxop.contains("2018")) {
                        var destDirPath = sqlContext.read.parquet(rxop)
                        var count = destDirPath.count
                        println("Writing " + rxop + "==> " + count)
                        destDirPath.write.mode("append").parquet(destDir)
                    }

            }catch {
                case ex:Exception => println(ex.getMessage)
            }
        })
    }



}


