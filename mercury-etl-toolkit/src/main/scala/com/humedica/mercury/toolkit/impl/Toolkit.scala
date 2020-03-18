package com.humedica.mercury.toolkit.impl

import java.io.{File, FileInputStream}
import java.net.URI
import java.util.Properties

import com.humedica.mercury.etl.core.engine.ShowDf._
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.{Engine, EntitySource}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object Toolkit {

  private var spark: SparkSession = null

  // Load Properties and overrides
  val log: Logger = LoggerFactory.getLogger(getClass)
  var cfg: Map[String, String] = null

  val overrides = ConfigFactory.load("overrides.conf")
  var dfConfig: DataFrame = null

  def data(filter: String = "", sort: Seq[String] = Seq()): DataFrame = {
    //dfConfig = spark.read.json("hdfs:///user/lphilbrook/mtk/data2.json")
    val filterConfig = if (filter == "") dfConfig else dfConfig.filter(filter)
    val sortConfig = if (sort.isEmpty) filterConfig else filterConfig.orderBy(sort.head, sort.tail: _*)
    sortConfig.select("NAME", "EMR", "GROUP", "CLIENT_DS_NAME", "EMR_DATA_ROOT").show(truncate = false)
    sortConfig
  }

  def use(configName: String) = {
    try {
      val columns = dfConfig.columns
      val lis = dfConfig.filter("NAME='" + configName.toUpperCase() + "'").rdd.collect()(0).mkString(";")
      val values = lis.substring(0, lis.length).split(";")
      cfg = (columns zip values).toMap
      log.info("Loaded: " + configName)
    } catch {
      case e: Exception => log.error("Cannot find dataset: " + configName)
    }
  }

  def current() = {
    if (cfg != null) {
      cfg foreach (x => log.info(s"${x._1} -> ${x._2}"))
    } else {
      log.info("No dataset loaded. Run use command.")
    }
  }

  /*
    def data(emr:String = "", group:String = ""): Unit ={
      ConfigReader.datasets(emr, group)
    }

    def use(configName: String) =
    {
      cfg = ConfigReader.usedataset(configName)+ ("ENTITY" -> "") + ("ENTITYSOURCE" -> "")
    }

    def current()= {
      if (cfg != null){
        ConfigReader.printConfig(cfg)
      } else(println("No dataset loaded. Run useDataset command."))

    }
  */
  def loadConfig(cfgPath: String = "etl.cfg") = {
    val props = new Properties()
    props.load(new FileInputStream(new File(cfgPath)))
    cfg = props.toMap + ("ENTITY" -> "") + ("ENTITYSOURCE" -> "")
  }

  def loadConfigHDFS(hdfsPath: String, cfgPath: String) = {
    val hdfs = FileSystem.get(new URI(hdfsPath), new Configuration())
    val path = new Path(cfgPath)
    val theStream = hdfs.open(path)
    val props = new Properties()
    props.load(theStream)
    theStream.close()
    cfg = props.toMap + ("ENTITY" -> "") + ("ENTITYSOURCE" -> "")
  }

  // Initialize context
  def setSession(spk: SparkSession, datasetIndex: String = "hdfs:///optum/mercury-stage/datasets/dataset_index.json") {
    spark = spk
    Engine.setSession(spk)
    version()
    // dfConfig = spark.read.json("hdfs:///user/lphilbrook/mtk/data2.json")
    dfConfig = spark.read.json(datasetIndex)
  }

  def getSpec(className: String): EntitySource = {
    val base = cfg("PACKAGE_ROOT") + "." + cfg("EMR") + "."
    val name =
      try {
        base + overrides.getString(cfg(EMR) + "|" + className + "|" + cfg(CLIENT_DS_NAME))
      } catch {
        case ex: Exception => base + className
      }
    Engine.getEntitySourceClass(name, cfg)
  }

  def getMapped(className: String): Seq[String] = getSpec(className).map.keys.toSeq

  def dropNull(df: DataFrame): DataFrame = {
    val nullColumns = df.columns.filter(c => df.select(c).filter(c + " is not null").count == 0)
    val keep = df.columns.filterNot(nullColumns.toSet)
    df.select(keep.head, keep.tail: _*)
  }

  def build(es: EntitySource, allColumns: Boolean = false, cols: Seq[String] = Seq(), addColumns: Seq[String] = Seq(), cache: Boolean = true): DataFrame = {

    val df = Engine.buildSpec(es, es.cfg)
    val sel =
      if (allColumns == true) df
      else if (cols.isEmpty) {
        val actual = df.columns
        val keep = es.map.keys.toList union addColumns // keep only mapped columns plus specific columns to add
        //val nullcol = actual.filter(c=>df.select(c).filter(c+" is not null").count==0)
        //val keep = actual.filterNot(nullcol.toSet) // keep only non-null columns

        val overlap = actual intersect keep
        val selected = if (overlap.isEmpty) actual else overlap
        df.select(selected.head, selected.tail: _*)
      }
      else df.select(cols.head, cols.tail: _*)
    if (cache) {
      sel.cache()
      sel.count
    }
    sel
  }

  def buildFromJar(className: String, allColumns: Boolean = false, cols: Seq[String] = Seq(), addColumns: Seq[String] = Seq(), cache: Boolean = true): DataFrame = {
    val es = getSpec(className)
    build(es, allColumns, cols, addColumns, cache)
  }

  // Possible location patterns
  // 1. path + "/" + tableName + /*.parquet
  // 2. path + "/" + tableName + /*/*.parquet
  // 3. path + /*/ + tableName + /*.parquet
  // 4. path + /*/ + tableName + /*/*.parquet
  def readParquet(tableName: String, path: String): DataFrame = {
    try {
      spark.read.parquet(path + "/" + tableName + "/*.parquet")
    } catch {
      case ex: Exception => try {
        spark.read.parquet(path + "/" + tableName + "/*/*.parquet")
      } catch {
        case ex: Exception => try {
          spark.read.parquet(path + "/*/" + tableName + "/*.parquet")
        } catch {
          case ex: Exception => try {
            spark.read.parquet(path + "/*/" + tableName + "/*/*.parquet")
          } catch {
            case ex: Exception =>
              log.error("Cannot find table " + tableName + " below path " + path)
              null
          }
        }
      }
    }
  }

  def readCDR(entity: String, source: String = "", cols: Seq[String] = Seq(), filter: Boolean = true): DataFrame = {
    postProcessCDR(readParquet(entity.toUpperCase, cfg("CDR_DIR")), source, cols, filter)
  }

  def postProcessCDR(df: DataFrame, source: String = "", cols: Seq[String] = Seq(), filter: Boolean = true): DataFrame = {
    if (df == null) return null
    val groupId = cfg.getOrElse(GROUPID, "")
    val cdsId = cfg.getOrElse(CLIENT_DS_ID, "")
    val fil = if (source == "" || !df.columns.contains("DATASRC")) df else df.filter("DATASRC LIKE '" + source + "'")
    val fil2 = if (filter == true && groupId != "" && df.columns.contains("GROUPID")) fil.filter("GROUPID LIKE '" + groupId + "'") else fil
    val fil3 = if (filter == true && cdsId != "" && df.columns.contains("CLIENT_DS_ID")) fil2.filter("CLIENT_DS_ID LIKE '" + cdsId + "'") else fil2
    if (cols.isEmpty) fil3 else fil3.select(cols.head, cols.tail: _*)
  }

  def readMapping(mapping: String): DataFrame = readParquet(mapping.toUpperCase, cfg("CDR_DATA_ROOT"))

  def datasrc(entity: String): Unit = {
    val df0 = freqCDR(entity, "DATASRC")
    log.info(show(df0, 9999, false))
  }

  def freqCDR(entity: String, col: String, filter: Boolean = true): DataFrame = {
    val df = readParquet(entity.toUpperCase, cfg("CDR_DIR"))
    if (!df.columns.contains(col)) null
    else {
      val groupId = cfg.getOrElse(GROUPID, "")
      val cdsId = cfg.getOrElse(CLIENT_DS_ID, "")
      val fil1 = if (filter == true && groupId != "" && df.columns.contains("GROUPID")) df.filter("GROUPID LIKE '" + groupId + "'") else df
      val fil2 = if (filter == true && cdsId != "" && df.columns.contains("CLIENT_DS_ID")) fil1.filter("CLIENT_DS_ID LIKE '" + cdsId + "'") else fil1
      fil2.select(col).groupBy(col).count.sort("count")
    }
  }

  def readStage(source: String): DataFrame = {
    if (source.contains(".")) {
      val path = cfg(source.split("\\.")(0).toUpperCase() + "_DATA_ROOT")
      readParquet(source.split("\\.")(1).toUpperCase, path)
    } else readParquet(source.toUpperCase, cfg("EMR_DATA_ROOT"))
  }

  def formatTime(df: DataFrame, column: String): DataFrame = {
    df.withColumn(column, regexp_replace(df(column), "\\.0$", ""))
  }

  def version(): Unit = log.info("\n\nMercury ETL Toolkit (MTK).")

  def commands(): Unit = {
    println(
      """
-------------------------------------------------------------------------------------------
data([filter="filterByColumn='value'"], [sort=Seq("sortByColumn")])    -- DISPLAY DATASETS

    Examples:
    data()
    data(filter="EMR='asent'")
    data(filter="GROUP='12345'")
    data(filter="EMR='asent' AND GROUP='H053731'")
    data(sort=Seq("NAME"))
    data(filter="GROUP='12345'", sort=Seq("NAME"))

-------------------------------------------------------------------------------------------
use("config_name") -- USE (MAKE CURRENT) A NAMED DATASET

    Example:   use("H1234_asent")

-------------------------------------------------------------------------------------------
current   -- PRINT THE CONFIGURATION FOR THE CURRENTLY SELECTED DATASET

-------------------------------------------------------------------------------------------
datasrc("entity") -- GET DISTINCT VALUES FOR THE DATASRC COLUMN IN A SPECIFIED CDR TABLE

    Example: datasrc("temp_allergies")

-------------------------------------------------------------------------------------------
build(entity_source, [allColumns=true], [cols=Seq("column_name_1", "column_name_2")], [addColumns=Seq("column_name_to_add")], [cache=false])

    To build an entity source, use :paste to paste a Mercury ETL Specification (Scala class) into the toolkit shell.
    Then allocate a new instance of the specification object:

        val es = new EntityDatasrc(cfg)

    Finally, build the specification to create your table:

        val df = build(es)

    Examples:

        val sdf = build(es, allColumns=true)
        val sdf = build(es, cols=Seq("Name", "Age"))
        val sdf = build(es, addColumns=Seq("ColumnFromEntity"))
        val sdf = build(nes, cache=false)

-------------------------------------------------------------------------------------------
buildFromJar(entity_source, [allColumns=true], [cols=Seq("column_name_1", "column_name_2")], [addColumns=Seq("column_name_to_add")], [cache=false]

    Build a table from a Mercury ETL specification in an imported library.  For example:

        val sdf = buildFromJar("allergies.AllergiesAlert")

-------------------------------------------------------------------------------------------
readCDR("entity", "datasource", [cols=Seq("column_name")], [filter=false]) -- LOAD A CDR (COMPARISON) TABLE

    Examples:

        val cdf = readCDR("entity_name", "datasource_name")
        val cdf = readCDR("entity_name", "datasource_name", sdf.columns)  -- Include only the same columns as the sdf table  (see "build" command)
        val cdf = readCDR("entity_name", "datasource_name", filter=false) -- Disable GROUP/CLIENT_DS_ID filtering

-------------------------------------------------------------------------------------------
readStage("stagetablename")  -- LOAD THE STAGE TABLE

    Examples:
        val alert = readStage("alert")
        val mpv = readStage("cdr.map_predicate_values")

-------------------------------------------------------------------------------------------
diff(dataframe1, dataframe2, [filter="ColumnName=value"], [sort=Seq("column_name_1")], [include=Seq("column_name_1", "column_name_2")], [exclude=Seq("column_name_2")], [nrows=number], truncate=false)

    Examples:
        diff(cdf, sdf)
        diff(cdf, sdf, filter="PatientName=Bob")
        diff(cdf, sdf, sort=Seq("Date", "Name"))
        diff(cdf, sdf, include=Seq("Date", "Name")
        diff(cdf, sdf, exclude=Seq("Date")
        diff(cdf, sdf, nrows=50)
        diff(cdf, sdf, truncate=false)
        diff(cdf, sdf, filter="PatientName=Bob", truncate=false)

-------------------------------------------------------------------------------------------
formatTime(dataframe, column) -- REMOVE A TRAILING .0 FROM A TIME FOR THE PURPOSES OF DIFF
  Examples:
    formatTime(scc, "APPOINTMENTTIME")

-------------------------------------------------------------------------------------------
compare(dataframe1, dataframe2, [filter="ColumnName=value"], [sort=Seq("column_name_1")], [include=Seq("column_name_1", "column_name_2")], [exclude=Seq("column_name_2")], [nrows=number], truncate=false)

      Compare displays some rows from two dataframes for quick visual comparison.

      Examples: compare(cdf, sdf)

-------------------------------------------------------------------------------------------
freqCDR("entity", "columnName", [filter=false])     -- RETURN THE VALUE FREQUENCY OF A SPECIFIED CDR TABLE AND COLUMN

      freqCDR("temp_diagnosis", "MAPPEDDIAGNOSIS")
      freqCDR("temp_diagnosis", "MAPPEDDIAGNOSIS", filter=false)   -- Don't filter for GROUP / CLIENT_DS_ID

-------------------------------------------------------------------------------------------
dropNull(dataframe)    -- REMOVE NULL COLUMNS FROM A DATAFRAME

      dropNull(sdf)

-------------------------------------------------------------------------------------------
version          -- DISPLAY THE CURRENT VERSION OF THE TOOLKIT

 """)
  }
}