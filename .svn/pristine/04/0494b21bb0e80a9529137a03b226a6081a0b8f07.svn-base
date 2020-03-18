package com.humedica.mercury.etl.core.engine

import scala.collection.mutable
import org.apache.spark.sql._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.language.postfixOps
import scala.sys.process._
import java.util.{Calendar, Date, TimeZone}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit


class TableCache {

  val cache = mutable.Map.empty[String, (DataFrame, Date)]
  val log = LogManager.getRootLogger
  log.setLevel(Level.WARN)
  var cacheTimeout = -1
  var rootPath: String = _

  def setTableCache(path: String, timeout: Int, resetCache: Boolean): Unit = {
    if(path != null){
      setRootPath(path)
      if(resetCache) clear()
    }else{
      log.warn("No cache path set. Will not cache to hdfs")
    }
    if(timeout > 0){
      cacheTimeout = timeout
    }
  }

  private def setRootPath(path: String): Unit = {
    if(((("hadoop fs -test -d " + path).!) ==0) || (("hadoop fs -mkdir -p " + path).! ==0)){
      if (!path.endsWith("/")) {
        rootPath = path + "/"
      } else {
        rootPath = path
      }
    }else{
      log.error("Cache root path is invalid: " + path)
      sys.exit(1)
    }
  }


  def add(name: String, df: DataFrame): Unit = {
    var date = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime()
    if(rootPath != null) {
      if (((("hdfs dfs -ls " + rootPath + name) #| "wc -l").!!).replaceAll("\\s", "").toInt == 0) {
        df.write.parquet(rootPath + name)
      } else {
        val removed = ("hdfs dfs -rm -r " + rootPath + name).!
        df.write.parquet("/user/eleanasd/data/CACHE/" + name)
      }
      date = getFileTime(rootPath + name)
    }
    cache += (name -> (df, date))
  }

  def get(name: String): DataFrame = {

    var mapValue = cache.getOrElse(name, null)

    if(mapValue != null){       //in map
    val df = mapValue._1
      val date = mapValue._2
      if(isStale(date)){
        log.warn("Entry was stale: " + name)
        remove(name)            //delete from map and hdfs
        return null
      }else{
        return df
      }
    }else {
      var inHdfs = false
      if(rootPath != null) inHdfs = ((("hdfs dfs -ls " + rootPath + name) #| "wc -l").!!).replaceAll("\\s", "").toInt > 0
      if (inHdfs) {
        if (isStale(getFileTime(rootPath + name))) {
          log.warn("Entry was stale: " + name)
          remove(name)
          return null
        } else {
          val df = Engine.spark.read.parquet(rootPath + name)
          cache += (name -> (df, getFileTime(rootPath + name)))
          return df
        }
      } else return null
    }
  }

  def remove(name: String): Unit = {
    val df = cache.getOrElse(name, null)
    if (df != null) {
      cache.remove(name)
    }
    if(rootPath != null) ("hdfs dfs -rm -r " + rootPath + name).!
  }

  def clear(): Unit = {
    cache.clear()
    if(rootPath != null && (((("hdfs dfs -ls " + rootPath) #| "wc -l").!!).replaceAll("\\s","").toInt > 0)){
      val cleared = ("hdfs dfs -rm -r " + rootPath + "/*").!
    }
  }

  private def getFileTime(file: String): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("GMT"))
    val fileStamp = ("hadoop fs -stat " + file).!!
    return format.parse(fileStamp)
  }

  private def isStale(date: Date): Boolean = {
    if(cacheTimeout > 0) {
      val now = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime()
      val milliDiff = now.getTime() - date.getTime()
      val minsDiff = TimeUnit.MILLISECONDS.toMinutes(milliDiff).toInt
      return minsDiff >= cacheTimeout
    }
    return false
  }




}
