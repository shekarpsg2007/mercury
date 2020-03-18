package com.humedica.mercury.etl.core.apps

import java.io._
import java.util.Properties
import com.humedica.mercury.etl.core.engine.{Engine, EntitySource}
import com.humedica.mercury.etl.core.engine.Constants._
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._

/**
  * Created by jrachlin on 5/18/17.
  */
object BuildEMR {

  type ConfigMap = Map[String, String]
  type OptionsSet = (String, String, String, ConfigMap, String, Int, Boolean)

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def getEntitySourceClass(name: String, config: Map[String, String]): EntitySource = {
    val pkgRoot = config.getOrElse(PACKAGE_ROOT, "")
    val className = if (pkgRoot == "") name else pkgRoot + "." + config.getOrElse(EMR,"")+"."+name
    Class.forName(className).getDeclaredConstructor(classOf[Map[String, String]]).newInstance(config).asInstanceOf[EntitySource]
  }

  def readOptions(args: Array[String]): OptionsSet = {

    def nextOption(options : OptionsSet, list: List[String]): OptionsSet = {
      def isSwitch(s : String) = s.startsWith("--")
      list match {
        case Nil => options
        case "--collection" :: col :: tail => nextOption((col, options._2, options._3, options._4, options._5, options._6, options._7),tail)
        case "--entity" :: ent :: tail => nextOption((options._1, ent.toLowerCase.capitalize, options._3, options._4, options._5, options._6, options._7),tail)
        case "--spec" :: spec :: tail => nextOption((options._1, options._2, spec, options._4, options._5, options._6, options._7), tail)
        case "--config" :: cfgName :: tail => {
          val props = new Properties()
          try {
            val f = new File(cfgName)
            log.info(s"Trying to load configuration file at [${f.getAbsolutePath}]")
            props.load(new FileInputStream(f))
            nextOption((options._1, options._2, options._3, options._4 ++ props.toMap, options._5, options._6, options._7), tail)
          } catch {
            case ex: FileNotFoundException =>
              log.error(s"Can't find configuration file: $cfgName")
              throw ex
          }
        }
        case "--cache" :: cache :: tail => nextOption((options._1, options._2, options._3, options._4, cache, options._6, options._7), tail)
        case "--cachetimeout" :: mins :: tail => nextOption((options._1, options._2, options._3, options._4, options._5, mins.toInt, options._7), tail)
        case "--resetcache" :: tail => nextOption((options._1, options._2, options._3, options._4, options._5, options._6, true), tail)
        case key :: value :: tail if isSwitch(key) => nextOption((options._1, options._2, options._3, options._4 ++ Map(key.substring(2).toUpperCase() -> value), options._5, options._6, options._7), tail)
        case option :: tail => nextOption(options, tail) // ignore non-switched options
      }
    }

    nextOption((null, null, null, Map(), null, -1, false), args.toList)
  }

  def makeSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("BuildEMR")
      .config("spark.sql.caseSensitive", "false")
      .enableHiveSupport()
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {

    // Check command line arguments
    if (args.length < 3) {
      println("USAGE: mbuild --collection <col> [--entity <ent> [,<ent2>,<ent3>...<entN>] [--spec <spec>]] [--config <cfg>] [--cache <path>] [--cachetimeout <minutes>] [--resetcache] [--KEY <VALUE>]*")
      System.exit(1)
    }

    // Extract command line arguments
    // Build the configuration
    val (collection, entity, spec, cfg, cache, cachetimeout, resetcache) = readOptions(args)

    if (collection == null && spec == null ) {
      println("No collection or spec was specified.  Nothing to build.")
      println("USAGE: mbuild --collection <col> [--entity <ent> [,<ent2>,<ent3>...<entN>] [--spec <spec>]] [--config <cfg>] [--cache <path>] [--cachetimeout <minutes>] [--resetcache] [--KEY <VALUE>]*")
      System.exit(1)
    }

    // Initialize spark and engine
    val spark = makeSession()
    Engine.setSession(spark)
    Engine.setTableCache(cache, cachetimeout, resetcache)

    // Invoke the build
    // Building a single spec or a collection?
    if (collection != null) {  // collection
      log.info(s"COLLECTION: $collection")
      if (entity != null) { // build specified entities
        log.info(s"ENTITIES: [$entity], SPEC: $spec")
        if (!entity.contains(",") && (spec != null)) { // build a single spec for a single entity
          val className = entity.toLowerCase+"."+entity.toLowerCase.capitalize+spec.toLowerCase.capitalize
          log.info(s"SPEC: [$className]")
          val es = getEntitySourceClass(className, cfg)
          val df = Engine.buildSpec(es, cfg)
          val emr = cfg.getOrElse(EMR, null)
          val outDir = cfg(OUT_DATA_ROOT) + "/" + (if (emr != null) emr.toUpperCase + "/") + entity.toUpperCase +"/" + spec.toUpperCase
          log.warn(s"WRITING SPEC [$className] TO: [$outDir]")
          df.write.parquet(outDir)
        }
        else { // build a set of entities
          log.info(s"BUILDING ENTITY SET: [$entity]")
          val entityList = entity.split(",").map(x => x.trim.toLowerCase.capitalize)
          var isCollectionOverride = false
          var collectionRoot = collection
          var collectionOverride = DEFAULT_COLLECTION

          if (collection.contains("@"))  {
            collectionRoot = collection.split("@")(0)
            collectionOverride = collection.split("@")(1)
            isCollectionOverride = true
          }
          entityList.foreach { entity =>
            val be = collectionRoot+"@"+collectionOverride+"@"+entity
            val df = Engine.buildEntity(be, cfg, !isCollectionOverride)
            val outDir = cfg(OUT_DATA_ROOT) + "/" + entity.toUpperCase
            log.warn(s"WRITING ENTITY [$entity] TO: [$outDir]")
            df.write.parquet(outDir)
          }
        }
      }
      else Engine.buildCollection(collection, cfg) // build all entities in collection
    }

    spark.stop()
    log.info("Done.")
  }
}
