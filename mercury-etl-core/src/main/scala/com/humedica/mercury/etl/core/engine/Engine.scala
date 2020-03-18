package com.humedica.mercury.etl.core.engine

import scala.collection.JavaConversions._
import org.apache.spark.sql._
import com.typesafe.config.{ConfigException, ConfigFactory}
import Constants.{EMR, _}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.ArrayBuffer
import com.humedica.mercury.etl.core.engine.Functions._

import scala.util.Try

object Engine {

  val log: Logger = LoggerFactory.getLogger(this.getClass)
  var spark: SparkSession = _
  val cache: TableCache = new TableCache()

  val schema = ConfigFactory.load("schema.conf")
  val overrides = ConfigFactory.load("overrides.conf")
  val sources = ConfigFactory.load("sources.conf").getConfig("mercury")
  val includes = ConfigFactory.load("includes.conf")
  val excludes = ConfigFactory.load("excludes.conf")

  def getSession() = spark

  def setSession(spk: SparkSession) {
    spark = spk
    spark.conf.set("spark.sql.crossJoin.enabled",true)
    spark.conf.set("spark.sql.codegen.wholeStage", false)
    spark.conf.set("parquet.enable.summary-metadata", true)
  }

  def setTableCache(cachePath: String, cacheTimeout: Int = -1, resetCache: Boolean = false): Unit = {
    cache.setTableCache(cachePath, cacheTimeout, resetCache)
  }

  /*
    def cacheTable(name: String, df: DataFrame): Unit = {
      cache.add(name,df)
    }

    def uncacheTable(name: String): Unit = {
      cache.remove(name)
    }

    def getCachedTable(name: String): DataFrame = {
      cache.get(name)
    }

  */

  def roundAt(p: Int)(n: Double): Double = {
    val s = math pow(10, p); (math round n * s) / s
  }

  def extractSource(className: String): String = {
    val toks = className.split("\\.")
    val toks2 = toks(toks.length - 1).split("\\$")
    val name = toks2(toks2.length - 1)
    val parts = name.split("[A-Z]")

    (name takeRight parts(2).length + 1).toLowerCase
  }


  def extractEntity(className: String): String = {
    val toks = className.split("\\.")
    val toks2 = toks(toks.length - 1).split("\\$")
    val name = toks2(toks2.length - 1)
    val parts = name.split("[A-Z]")
    name.substring(0, parts(1).length + 1)
  }

  def getEntitySourceClass(name: String, config: Map[String, String]): EntitySource = {
    val className = if (name.indexOf(':') != -1) config(PACKAGE_ROOT) + "." + name.split(":")(1) else name
    Class.forName(className).getDeclaredConstructor(classOf[Map[String, String]]).newInstance(config).asInstanceOf[EntitySource]
  }

  /**
    * Resolution order:
    * - ROOT.emr.entity.EntitySource
    * - ROOT.emr.entity.Source
    * - ROOT.emr.Source
    */
  def getEntitySourceClass2(emr: String, entity: String, source: String, config: Map[String, String]): EntitySource = {
    val classNameRoot = config(PACKAGE_ROOT) + "." + emr + "."
    val className1 = classNameRoot + entity.toLowerCase + "." + entity.toLowerCase.capitalize + source.toLowerCase.capitalize
    val className2 = classNameRoot + entity.toLowerCase + "." + source.toLowerCase.capitalize
    val className3 = classNameRoot + source.toLowerCase.capitalize
    val classes = Seq(className1, className2, className3)
    log.info(s"Resolving classes: $classes")
    val oClasses = classes.map {
      cn => Try(Class.forName(cn)
        .getDeclaredConstructor(classOf[Map[String, String]])
        .newInstance(config).asInstanceOf[EntitySource]).toOption
    }
    val oClass = oClasses.find(_.isDefined)
    return if (oClass.isDefined) oClass.get.get else null
  }

  def buildSpec(es: EntitySource, cfg: Map[String, String], depth:Int = 0): DataFrame = {

    log.warn(" "*4*depth+"Starting to build spec: " + es.getClass.getCanonicalName)
    val startTime = System.currentTimeMillis()

    // Is the table cached?
    val clsName = es.getClass.getCanonicalName
    val cached = if (es.cacheMe == true) Engine.cache.get(clsName) else null;
    val result =
      if (cached != null) cached // cached value found - we're done!
      else { // no cached value found - build the spec

        // Initialize the entity source
        es.initialize()

        // Read staging tables
        for (t <- es.tables) {

          // Read or build each table
          val df = if (t.contains(':')) {
            val tableName = t.split(":")(0)
            val target = t.split(":")(1)
            if (target.contains("@"))
              buildEntity(target, cfg,false).as(tableName) //todo -- do we want to check for override here?
            else buildSpec(getEntitySourceClass(t, cfg), cfg, depth + 1).as(tableName) // building table from another spec
          }
          else Functions.readTable(t, cfg).as(t) // reading a staging table from hdfs

          // Store the table in the entity source table map (tableName -> DataFrame)
          if (t.contains(":"))
            es.table += (t.split(':')(0) -> df)
          else es.table += (t -> df)

        }

        // Filter dataframes for selected columns
        for ((t, clis) <- es.columnSelect)
          es.table += (t -> es.table(t).select(clis.head, clis.tail: _*))

        // Do before join operations (deduping, filtering, inclusion/exclusion rules)
        es.table = es.table.toList.map { case (table: String, df: DataFrame) => (table, es.beforeJoin.getOrElse(table, Functions.noFilter())(df).as(table)) }.toMap

        // Do the join
        val joined = es.join(es.table)

        // Do the after join
        var df = es.afterJoin(joined)

        // Get column list either from the listed columns in the spec or the sources.conf or
        // if all else fails, the columns for the first listed staging table
        val columns: Seq[String] =
          if (es.columns.nonEmpty) es.columns
          else {
            try {
              schema.getStringList(extractEntity(es.getClass.getName())).map(_.split("-")(0).toUpperCase()).toList
            } catch {
              case ex: Exception => es.table(es.tables.head).columns
            }
          }

        // Map each column - if nothing is specified in the map variable, the column is assigned NULL
        for (col <- columns)
          df = es.map.getOrElse(col, if (df.columns.contains(col)) Functions.noop() else Functions.nullValue())(col, df)

        // Do the aftermap
        df = es.afterMap(df)

        // Select out the target columns
        if (columns.nonEmpty)
          df = df.select(columns.head, columns.tail: _*)


        // Add result to cache (if requested)
        if (es.cacheMe) Engine.cache.add(clsName, df)

        // Final result of build
        df
      }

    // Compute and report running time
    val endTime = System.currentTimeMillis()
    val diffTime = roundAt(2)((endTime - startTime) / 1000.0)
    log.warn(" " * 4 * depth + "Completed building spec: " + es.getClass.getCanonicalName + " (" + diffTime + " Sec)")

    // Return the result

    //This is a piece of shit
    if (es.columns.nonEmpty)  result
    else
      datatypematch(result, extractEntity(es.getClass.getName()))
  }

  def activateConfigurationSettingsForEMR(cfg: Map[String, String], emr: String): Map[String, String] = {

    val emrUpper = emr.toUpperCase
    cfg +
      (EMR -> cfg.getOrElse(emrUpper+"_EMR",cfg(EMR))) +
      (CLIENT_DS_ID -> cfg.getOrElse(emrUpper+"_CLIENT_DS_ID", cfg(CLIENT_DS_ID))) +
      (CLIENT_DS_NAME -> cfg.getOrElse(emrUpper+"_CLIENT_DS_NAME", cfg(CLIENT_DS_NAME))) +
      (EMR_DATA_ROOT -> cfg.getOrElse(emrUpper+"_EMR_DATA_ROOT", cfg.getOrElse(EMR_DATA_ROOT, "")))
  }

  /**
    * Construct an entity in a collection
    *
    * @param target    Format: collection.name@table
    * @param cfg
    * @return
    */
  def buildEntity(target: String, cfg: Map[String,String],checkForSpecOverride: Boolean): DataFrame = {

    var utable: DataFrame = null
    val collection = target.split("@")(0)
    val collectionOverride = target.split("@")(1)
    var entity = target.split("@")(2)
    var emr = cfg(EMR)
    val updatedCfg = activateConfigurationSettingsForEMR(cfg, emr)

    val exclSourcesArr = ArrayBuffer[String]()
    try {
      val excl = excludes.getConfig(collection).getConfig(cfg(CLIENT_DS_NAME)).root.unwrapped.toMap
      val exclmap = excl.map { case (k: String, v: Object) => val vs = v.toString; k -> vs.substring(1, vs.length - 1).split(",").map {
        _.trim}}
      val exclSources = exclmap(entity)
      for (src <- exclSources) {
        exclSourcesArr += src
      }
    } catch {
      case ex: Exception => null
    }

    try {
      val m = sources.getConfig(collection).getConfig(collectionOverride).root.unwrapped.toMap
      val esmap = m.map { case (k: String, v: Object) => val vs = v.toString;
        k -> vs.substring(1, vs.length - 1).split(",").map {
          _.trim}}
      val provSources = esmap(entity)
      for (src <- provSources) {
        log.warn("eventName:Start-Entity:"+entity+"-Datasrc:"+src+";eventTime:"+java.time.LocalDate.now + "T"+java.time.LocalTime.now+";emr:"+emr+";client:"+cfg(GROUPID)+";client_ds_id:"+cfg(CLIENT_DS_ID)+";client_ds_name:"+cfg(CLIENT_DS_NAME))
        log.warn("------ Started processing entity: " + entity + " - datasrc: " + src + " ------")
        var sourceName = src
        val df =
          if (src.contains("@")) { // is this cross-collection dependencies?
            buildEntity(src, updatedCfg, false)
          } else {
            if (checkForSpecOverride) {
              val (overrideEntity, overrideSrc) = getSpecOverride(entity, src, updatedCfg)

              val es = Engine.getEntitySourceClass2(emr, overrideEntity, overrideSrc, updatedCfg)
              buildSpec(es, updatedCfg)
            } else {
              log.warn("emr" +emr+ "entity" +entity+ "src" +src+ "Updatedcfg" +updatedCfg) //Remove later
              val es = Engine.getEntitySourceClass2(emr, entity, src, updatedCfg)
              buildSpec(es, updatedCfg)
            }
          }

        if (exclSourcesArr.contains(src)) {
          df.filter("1=2")
          log.warn("Excluded the " + entity + " - datasrc: " + src + " from the build per excludes.conf settings")
        } else {
          df
        }
        utable = if (utable == null) df else utable.union(df)
        log.warn("eventName:End-Entity:"+entity+"-Datasrc:"+src+";eventTime:"+java.time.LocalDate.now + "T"+java.time.LocalTime.now+";emr:"+emr+";client:"+cfg(GROUPID)+";client_ds_id:"+cfg(CLIENT_DS_ID)+";client_ds_name:"+cfg(CLIENT_DS_NAME))
      }
    } catch {
      case ex: Exception => utable
    }

    try {
      val inc = includes.getConfig(collection).getConfig(cfg(CLIENT_DS_NAME)).root.unwrapped.toMap
      val incmap = inc.map { case (k: String, v: Object) => val vs = v.toString; k -> vs.substring(1, vs.length - 1).split(",").map {
        _.trim}}
      val incSources = incmap(entity)
      for (src <- incSources) {
        log.warn("eventName:Start-Entity:"+entity+"-Datasrc:"+src+";eventTime:"+java.time.LocalDate.now + "T"+java.time.LocalTime.now+";emr:"+emr+";client:"+cfg(GROUPID)+";client_ds_id:"+cfg(CLIENT_DS_ID)+";client_ds_name:"+cfg(CLIENT_DS_NAME))
        log.warn("------ Started processing entity: "+entity +" - datasrc: "+src + " ------")
        var sourceName = src
        val df =
          if (src.contains("@")) { // is this cross-collection dependencies?
            buildEntity(src, updatedCfg, false)
          } else {
            if (checkForSpecOverride) {
              val (overrideEntity, overrideSrc) = getSpecOverride(entity, src, updatedCfg)
              val es = Engine.getEntitySourceClass2(emr, overrideEntity, overrideSrc, updatedCfg)
              buildSpec(es, updatedCfg)
            } else {
              val es = Engine.getEntitySourceClass2(emr, entity, src, updatedCfg)
              buildSpec(es, updatedCfg)
            }
          }

        if (exclSourcesArr.contains(src)) {
          df.filter("1=2")
          log.warn("Excluded the " + entity + " - datasrc: " + src + " from the build per excludes.conf settings")
        } else {
          df
        }
        utable = if (utable == null) df else utable.union(df)
        log.warn("eventName:End-Entity:"+entity+"-Datasrc:"+src+";eventTime:"+java.time.LocalDate.now + "T"+java.time.LocalTime.now+";emr:"+emr+";client:"+cfg(GROUPID)+";client_ds_id:"+cfg(CLIENT_DS_ID)+";client_ds_name:"+cfg(CLIENT_DS_NAME))
      }
    }
    catch
      {
        case ex: Exception => utable
      }
    utable
  }

  /**
    * Build a collection of specs defined in sources.conf
    *
    * @param collection The name of the collection to build
    * @param cfg The build configuration
    */
  def buildCollection(collection: String, cfg: Map[String, String]): Unit = {

    var isCollectionOverride = false
    var collectionRoot = collection
    var collectionOverride = DEFAULT_COLLECTION

    if (collection.contains("@"))  {
      collectionRoot = collection.split("@")(0)
      collectionOverride = collection.split("@")(1)
      isCollectionOverride = true
    }

    val m = sources.getConfig(collectionRoot).getConfig(collectionOverride).root.unwrapped.toMap
    val esmap = m.map{case (k:String,v:Object) => val vs = v.toString; k-> vs.substring(1, vs.length-1).split(",").map{_.trim}}
    var esincmap = esmap
    try {
      val inc = includes.getConfig(collectionRoot).getConfig(cfg(CLIENT_DS_NAME)).root.unwrapped.toMap
      val incmap = inc.map { case (k: String, v: Object) => val vs = v.toString; k -> vs.substring(1, vs.length - 1).split(",").map {
        _.trim}}
      esincmap = esmap ++ incmap
    } catch {
      case ex: ConfigException => {
        esincmap
      }
    }

    val esincmap_keys = esincmap.keys

    for (entity <- esincmap_keys) {
      val be = collectionRoot+"@"+collectionOverride+"@"+entity
      log.warn("eventName:Start-Entity:"+entity+";eventTime:"+java.time.LocalDate.now + "T"+java.time.LocalTime.now+";emr:"+collection+";client:"+cfg(GROUPID)+";client_ds_id:"+cfg(CLIENT_DS_ID)+";client_ds_name:"+cfg(CLIENT_DS_NAME))
      log.warn("------ Started processing entity : "+entity +" for " +cfg(CLIENT_DS_NAME) +" ("+ be +") "+ " ------")
      val df = buildEntity(be, cfg, !isCollectionOverride)
      val outName = cfg(OUT_DATA_ROOT) + "/" + entity.toUpperCase()
      df.write.mode("overwrite").parquet(cfg(OUT_DATA_ROOT) + "/" + entity.toUpperCase())
      log.warn("Completed build for the entity "+entity+" - parquet file: "+outName)
      log.warn("eventName:End-Entity:"+entity+";eventTime:"+java.time.LocalDate.now + "T"+java.time.LocalTime.now+";emr:"+collection+";client:"+cfg(GROUPID)+";client_ds_id:"+cfg(CLIENT_DS_ID)+";client_ds_name:"+cfg(CLIENT_DS_NAME))
      //val formatter = java.text.NumberFormat.getIntegerInstance
      //log.warn("Total # of records written for "+ entity + ": " + formatter.format(df.count))
      /* try {
            val srccnt = df.groupBy("DATASRC").count.orderBy("DATASRC").show(false)
            log.warn("Total # of records by DataSrc for: " + entity + " are: "+srccnt)
            //log.warn(""+srccnt)
          } catch {
            case ex: Exception => log.info("--------")
          } */
    }
  }

  def getSpecOverride(entity: String, src: String, cfg: Map[String, String]) : (String, String) = {
    val key = cfg(EMR)+ "|" +entity.toLowerCase+"."+entity+src+"|"+cfg(CLIENT_DS_NAME)
    try {
      val overrideString = overrides.getString(key)
      log.warn("Override found for the " +entity.toLowerCase+"."+entity+src+ " - the new class is " + overrideString)
      val newEntity = overrideString.split("\\.")(0)
      val newSrcClass = overrideString.split("\\.")(1)
      val newSrc = newSrcClass.substring(newEntity.length)
      (newEntity, newSrc)
    }  catch {
      case ex: ConfigException => {
        (entity, src)
      }
    }
  }


  def datatypematch(df : DataFrame, entity: String)= {
    var dfwithtype = df
    for (  entry <- schema.getStringList(entity)) {
      val col = entry.split("-")(0)
      val datatype = entry.split("-")(1)
      if (datatype != null) {
        if (datatype.toUpperCase.contains("INT")) {
          dfwithtype = dfwithtype.withColumn(col, safe_to_number(dfwithtype(col)))
        }
        else if (datatype.toUpperCase.contains("DECIMAL")) {
          val precision = datatype.toUpperCase.split("[\\(\\)]")(1).split(",")(0)
          val scale = datatype.toUpperCase.split("[\\(\\)]")(1).split(",")(1)
          dfwithtype = dfwithtype.withColumn(col, safe_to_decimal(dfwithtype(col), precision, scale))
        }
        else dfwithtype = dfwithtype.withColumn(col, dfwithtype(col).cast(datatype))
      }
      else dfwithtype
    }
    dfwithtype
  }

}