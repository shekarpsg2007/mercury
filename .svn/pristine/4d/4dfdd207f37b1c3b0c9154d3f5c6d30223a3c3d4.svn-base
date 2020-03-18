package com.humedica.mercury.etl.core.engine

import com.humedica.mercury.etl.core.engine.Types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by jrachlin on 12/8/16.
  */
object Functions {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  // Possible location patterns
  // 1. path + "/" + tableName + /*.parquet
  // 2. path + "/" + tableName + /*/*.parquet
  // 3. path + /*/ + tableName + /*.parquet
  // 4. path + /*/ + tableName + /*/*.parquet

  def readParquet(tableName: String, path: String): DataFrame = {

    //println("tableName: \""+tableName+"\"")
    //println("path     : \""+path+"\"")
    //println("Engine   : "+(if (Engine == null) "NULL" else "OK"))
    //println("spark    : "+(if (Engine != null && Engine.spark == null) "NULL" else "OK"))

    try {
      Engine.spark.read.parquet(path + "/" + tableName + "/*.parquet")
    } catch {
      case ex: Exception => try {
        Engine.spark.read.parquet(path + "/" + tableName + "/*/*.parquet")
      } catch {
        case ex: Exception => try {
          Engine.spark.read.parquet(path + "/*/" + tableName + "/*.parquet")
        } catch {
          case ex: Exception => try {
            Engine.spark.read.parquet(path + "/*/" + tableName + "/*/*.parquet")
          } catch {
            case ex: Exception => logger.error("Cannot find table " + tableName + " below path " + path); null
          }
        }
      }
    }
  }

  def readParquetList(tableName: String, paths: String): DataFrame = {
    var df: DataFrame = null;
    for (path <- paths.split(",")) {
      val df1 = readParquet(tableName, path)
      if (df1 != null) {
        return df1
      }
    }
    df
  }

  def readTable(source: String, cfg: Map[String, String]): DataFrame = {
    //println("In readTable: source="+source+"EMR_DATA_ROOT="+cfg("EMR_DATA_ROOT")+" cfg="+cfg.toString())

    if (source.contains(".")) {
      val path = cfg(source.split("\\.")(0).toUpperCase() + "_DATA_ROOT")
      readParquetList(source.split("\\.")(1).toUpperCase, path)
    } else {
      readParquetList(source.toUpperCase, cfg("EMR_DATA_ROOT"))
    }
  }

  /*
  def readTable(source: String, cfg: Map[String, String]): DataFrame = {
    //println("In readTable: source="+source+"EMR_DATA_ROOT="+cfg("EMR_DATA_ROOT")+" cfg="+cfg.toString())
    if (source.contains(".")) {
      val path = cfg(source.split("\\.")(0).toUpperCase() + "_DATA_ROOT")
      readParquet(source.split("\\.")(1).toUpperCase, path)
    } else readParquet(source.toUpperCase, cfg("EMR_DATA_ROOT"))
  }
  */

  // Misc. Functions
  //  def readTable(t: String, config: Map[String,String]): DataFrame = {
  //
  //    val tableName = if (t.indexOf('.') != -1) t.split("\\.")(1) else t
  //
  //    val tablePath =
  //      if (t.indexOf('@') != -1) t.split("@")(1)
  //      else {
  //        val base = if (t.indexOf('.') != -1) config(t.split("\\.")(0).toUpperCase + "_DATA_ROOT") else config(EMR_DATA_ROOT)
  //        base + "/" + tableName.toUpperCase() + "/*.parquet"
  //      }
  //
  //    Engine.sqlContext.read.parquet(tablePath).as(tableName)
  //
  //  }

  // Functions for standard joins

  def noJoin(): JoinFunction =
    (dfs: Map[String, DataFrame]) => dfs.head._2

  def innerJoin(col: String): JoinFunction =
    (dfs: Map[String, DataFrame]) => {
      var df = dfs.head._2
      for ((table, d) <- dfs)
        if (table != dfs.head._1)
          df = df.join(d, Seq(col), "inner")
      df
    }

  def leftOuterJoin(col: String): JoinFunction =
    (dfs: Map[String, DataFrame]) => {
      var df = dfs.head._2
      for ((table, d) <- dfs)
        if (table != dfs.head._1)
          df = df.join(d, Seq(col), "left_outer")
      df
    }

  // Functions for standard filtering (inclusion / exclusion) patterns

  def renameColumn(col: String, newcol: String): DataFunction =
    (df: DataFrame) => df.withColumnRenamed(col, newcol)

  def renameColumns(pairs: List[(String, String)]): DataFunction =
    (df: DataFrame) => {
      var tmp = df
      for ((col, newcol) <- pairs)
        tmp = tmp.withColumnRenamed(col, newcol)
      tmp
    }

  /*
    // Retrieve the specified column of the specified table.
    // Optional Parameters
    // takeNulls (default false). When false will not take null values.
    // takeSet (default true). When true will take only distinct values.
    def retrieveColumnAsList(df : DataFrame, colName : String, takeNulls : Boolean = false, takeSet : Boolean = true) : List[String] = {
      val table = df.select(colName)
      if (!takeNulls)
        if (takeSet)
          table.distinct.filter(table(colName).isNotNull).map(row => row(0)).collect.toList
        else
          table.filter(table(colName).isNotNull).map(row => row(0)).collect.toList
      else
      if(takeSet)
        table.distinct.map(row => row(0)).collect.toList
      else
        table.map(row => row(0)).collect.toList
    }

    //Returns a function that filters a DataFrame df based on whether the values in a particular column occur in a column of another table
    // Optional Parameters
    //   INTERSECT (default true). When true, will filter on when values in DF are in the other table.
    //   When false will take results that are present only in df, but not in the other table's column
    def filterAgainstOtherTable( col : String, otherDF: DataFrame, otherColumn : String, intersect : Boolean = true) : DataFunction =
    (df : DataFrame) => {
      val columnList = retrieveColumnAsList(otherDF, otherColumn)
      if (intersect)
        df.filter(df(col).isin(columnList:_*))
      else
        df.filter(!df(col).isin(columnList:_*))
    }
  */

  def noFilter(): DataFunction =
    (df: DataFrame) => df

  def includeIf(expr: String): DataFunction =
    (df: DataFrame) => df.filter(expr)

  def excludeIf(expr: String): DataFunction =
    (df: DataFrame) => df.filter("not(" + expr + ")")

  def valueRevisionFilter(col: String, revcol: String): DataFunction = {
    (df: DataFrame) => {
      val columns = df.columns
      val df2 = df
      df.join(df2, df(col) === df2(revcol), "left_outer")
        .filter(isnull(df2(revcol))).select(columns.map { c => df(c) }: _*)
    }
  }

  /**
    * Groups data frame by specified columns, then for each group, selects out a single row with the maximum value for the rnkcol
    *
    * @param grpcol
    * @param rnkcol
    * @return
    */
  def bestRowPerGroup(grpcol: List[String], rnkcol: String): DataFunction = {
    (df: DataFrame) =>
      df.join(df.groupBy(grpcol.head, grpcol.tail: _*).agg(max(rnkcol).alias(rnkcol)), rnkcol :: grpcol).dropDuplicates(rnkcol :: grpcol)
  }

  /**
    * Assigns a standard value to all rows within a group - the standard value being associated with the row whose
    * rnkcol value is maximum
    *
    * @param grpcol
    * @param rnkcol
    * @param stdcol
    * @return
    */
  def standardizeColumn(grpcol: List[String], rnkcol: String, stdcol: String): DataFunction = {
    (df: DataFrame) => {
      val best = bestRowPerGroup(grpcol, rnkcol)(df)
      val select = best.select(stdcol, grpcol: _*)
      val std = select.withColumnRenamed(stdcol, "STD_VALUE_COLUMN")
      df.join(std, grpcol).drop(stdcol).withColumnRenamed("STD_VALUE_COLUMN", stdcol)
    }
  }

  def standardizeColumn(grpcol: List[String], rnkcol: String, stdcols: List[String]): DataFunction = {
    (df: DataFrame) => {
      val best = bestRowPerGroup(grpcol, rnkcol)(df)
      var dftmp = df
      for (stdcol <- stdcols) {
        val select = best.select(stdcol, grpcol: _*)
        val std = select.withColumnRenamed(stdcol, "STD_VALUE_COLUMN")
        dftmp = dftmp.join(std, grpcol).drop(stdcol).withColumnRenamed("STD_VALUE_COLUMN", stdcol)
      }
      dftmp
    }
  }

  //conv.withColumn("PARSED", from_unixtime(unix_timestamp($"CONVERTED", "yyyyMM"),"MMM-yyyy")).show

  /**
    * Map from a data column.  Optionally specify both input and output format
    *
    * @param column       Source date column
    * @param inputFormat  Source column date format
    * @param outputFormat Output date format
    * @return Date from column with modified format.  null if column value doesn't conform to inputFormat
    */
  def mapFromDate(column: String, inputFormat: String = "yyyy-MM-dd HH:mm:ss", outputFormat: String = "yyyy-MM-dd HH:mm:ss"): ProvenanceFunction = {
    (col: String, df: DataFrame) => df.withColumn(col, from_unixtime(unix_timestamp(df(column), inputFormat), outputFormat))
  }

  def mapFromUnixtime(column: String, outputFormat: String = "yyyy-MM-dd HH:mm:ss"): ProvenanceFunction = {
    (col: String, df: DataFrame) => df.withColumn(col, from_unixtime(df(column), outputFormat))
  }

  def mapFrom(column: String, prefix: String = "", nullIf: Seq[String] = Seq(), nullIfNot: Seq[String] = Seq()): ProvenanceFunction =
    if (nullIf.isEmpty && nullIfNot.isEmpty) { // no blacklist or whitelist
      if (prefix == "")
        (col: String, df: DataFrame) => df.withColumn(col, df(column))
      else
        (col: String, df: DataFrame) => df.withColumn(col, concat(lit(prefix), df(column)))
    } // blacklist only
    else if (nullIf.nonEmpty && nullIfNot.isEmpty) {
      if (prefix == "")
        (col: String, df: DataFrame) => df.withColumn(col,
          when(df(column).isin(nullIf: _*), nullColumn).otherwise(df(column)))
      else
        (col: String, df: DataFrame) => df.withColumn(col,
          when(df(column).isin(nullIf: _*), nullColumn).otherwise(concat(lit(prefix), df(column))))
    } // whitelist only
    else if (nullIf.isEmpty && nullIfNot.nonEmpty) {
      (col: String, df: DataFrame) =>
        df.withColumn(col,
          when(df(column).isin(nullIfNot: _*), df(column)).otherwise(concat(lit(prefix), nullColumn)))
    } // both blacklist and whitelist specified
    else { //TODO: Remove blacklist from whitelist and apply whitelist
      if (prefix == "")
        (col: String, df: DataFrame) => df.withColumn(col, df(column))
      else
        (col: String, df: DataFrame) => df.withColumn(col, concat(lit(prefix), df(column)))
    }

  def cascadeFrom(columns: Seq[String], prefix: String = "", nullIf: Seq[String] = Seq(), nullIfNot: Seq[String] = Seq()): ProvenanceFunction =
    if (nullIf.isEmpty && nullIfNot.isEmpty) { // no blacklist or whitelist
      if (prefix == "")
        (col: String, df: DataFrame) => df.withColumn(col, coalesce(columns.map(c => df(c)): _*))
      else
        (col: String, df: DataFrame) => df.withColumn(col, concat(lit(prefix), coalesce(columns.map(c => df(c)): _*)))
    } // blacklist only
    else if (nullIf.nonEmpty && nullIfNot.isEmpty) {
      if (prefix == "")
        (col: String, df: DataFrame) => df.withColumn(col,
          when(coalesce(columns.map(c => df(c)): _*).isin(nullIf: _*), nullColumn)
            .otherwise(coalesce(columns.map(c => df(c)): _*)))
      else
        (col: String, df: DataFrame) => df.withColumn(col,
          when(coalesce(columns.map(c => df(c)): _*).isin(nullIf: _*), nullColumn)
            .otherwise(concat(lit(prefix), coalesce(columns.map(c => df(c)): _*))))
    } // whitelist only
    else if (nullIf.isEmpty && nullIfNot.nonEmpty) {
      if (prefix == "")
        (col: String, df: DataFrame) => df.withColumn(col,
          when(coalesce(columns.map(c => df(c)): _*).isin(nullIfNot: _*), coalesce(columns.map(c => df(c)): _*))
            .otherwise(nullColumn))
      else
        (col: String, df: DataFrame) => df.withColumn(col,
          when(coalesce(columns.map(c => df(c)): _*).isin(nullIfNot: _*), concat(lit(prefix), coalesce(columns.map(c => df(c)): _*)))
            .otherwise(nullColumn))
    }
    else { //TODO: Remove blacklist from whitelist and apply whitelist
      if (prefix == "")
        (col: String, df: DataFrame) => df.withColumn(col, coalesce(columns.map(c => df(c)): _*))
      else
        (col: String, df: DataFrame) => df.withColumn(col, concat(lit(prefix), coalesce(columns.map(c => df(c)): _*)))
    }

  def concatFrom(columns: Seq[String], delim: String = "", prefix: String = ""): ProvenanceFunction =
    (col: String, df: DataFrame) => df.withColumn(col, if (prefix == "") concat_ws(delim, columns.map(c => df(c)): _*) else
      concat_ws(delim, lit(prefix), concat_ws(delim, columns.map(c => df(c)): _*)))

  // Value mapped from another field so long as the value of a second column is NOT one of a specified set
  def mapFromSecondColumnValueIsNotIn(src: String, second: String, values: Seq[String]): ProvenanceFunction =
    (col: String, df: DataFrame) => df.withColumn(col,
      when(df(second).isin(values: _*), nullColumn).otherwise(df(src)))

  // Value mapped from another field so long as the value of a second column IS one of a specified set
  def mapFromSecondColumnValueIsIn(src: String, second: String, values: Seq[String]): ProvenanceFunction =
    (col: String, df: DataFrame) => df.withColumn(col,
      when(df(second).isin(values: _*), df(src)).otherwise(nullColumn))

  // Value mapped from another field so long as the value is of a specific length
  def mapFromRequiredLength(src: String, len: Int): ProvenanceFunction =
    (col: String, df: DataFrame) => df.withColumn(col,
      when(length(df(src)) === len, df(src)).otherwise(nullColumn))

  // Functions for standard provenance patterns
  def literal(s: String): ProvenanceFunction =
    (col: String, df: DataFrame) => df.withColumn(col, lit(s))

  def noop(): ProvenanceFunction =
    (col: String, df: DataFrame) => df

  // Assign null to the column
  def nullValue(): ProvenanceFunction =
    (col: String, df: DataFrame) => df.withColumn(col, nullColumn)

  // TODO - placeholder for generated fields
  def todo(cols: String*): ProvenanceFunction =
    (col: String, df: DataFrame) => df.withColumn(col, nullColumn)

  //  def applyColumnFunction(f: (Column*) => Column, colnames: Seq[String]): ProvenanceFunction =
  //  {
  //    (col: String, df: DataFrame) =>
  //      df.withColumn(col, f(colnames.map(df(_)):_*))
  //  }

  def validate(col: String, matchPattern: String): ProvenanceFunction = {
    (newcol: String, df: DataFrame) =>
      df.withColumn(newcol, when(df(col) rlike matchPattern, df(col)).otherwise(null))
  }

  def standardize(col: String, removePattern: String, matchPattern: String, mismatchPattern: String): ProvenanceFunction = {
    (newcol: String, df: DataFrame) => {
      val replaced = regexp_replace(trim(df(col)), removePattern, "")
      df.withColumn(newcol, when(replaced rlike matchPattern, when((replaced rlike mismatchPattern) && (!mismatchPattern.equals("")), null).otherwise(replaced)).otherwise(null))
    }
  }

  def standardizeSSN(col: String): ProvenanceFunction = {
    standardize(col, "[[:punct:]a-zA-Z]",
      "^[0-9]{3}\\-[0-9]{2}\\-[0-9]{4}$|^[0-9]{9}$",
      "^000|^9|^666|0000$|[0-9]{3}00[0-9]{4}|[0-9]{3}\\-00\\-[0-9]{4}|000-00-0000|111-11-1111|222-22-2222|333-33-3333|444-44-4444|555-55-5555|666-66-6666|777-77-7777|888-88-8888|999-99-9999|000000000|111111111|222222222|333333333|444444444|555555555|666666666|777777777|888888888|999999999")
  }

  // THIS IS TEMPORARY FOR MAVEN REPOSITORY TESTING
  def standardizeSSN2(col: String): ProvenanceFunction = {
    standardize(col, "[[:punct:]a-zA-Z]",
      "^[0-9]{3}\\-[0-9]{2}\\-[0-9]{4}$|^[0-9]{9}$",
      "^000|^9|^666|0000$|[0-9]{3}00[0-9]{4}|[0-9]{3}\\-00\\-[0-9]{4}|000-00-0000|111-11-1111|222-22-2222|333-33-3333|444-44-4444|555-55-5555|666-66-6666|777-77-7777|888-88-8888|999-99-9999|000000000|111111111|222222222|333333333|444444444|555555555|666666666|777777777|888888888|999999999")
  }

  def standardizeZip(col: String, zip5: Boolean = false): ProvenanceFunction = {
    val f = standardize(col, "[[:punct:]a-zA-Z]", "^[0-9]{5}\\-[0-9]{4}$|^[0-9]{5}$|^[0-9]{9}$", "")
    if (zip5) {
      (newcol: String, df: DataFrame) => {
        val df2 = f(newcol + "_TMP", df)
        df2.withColumn(newcol, substring(df2(newcol + "_TMP"), 0, 5))
          .drop(newcol + "_TMP")
      }
    } else f
  }

  def regexpLike(field: String, regexp: String): ProvenanceFunction =
    (col: String, df: DataFrame) => {
      val exp = "case when " + field + "' rlike " + regexp + " then " + field + " else null end"
      df.withColumn(col, expr(exp))
    }

  //matches regular expressions against values in a specified column.  The value assigned to col is a literal
  // based on the regexValueMap

  def regexpToLiteral(column: String, regexValueMap: Map[String, String], default: String = null): ProvenanceFunction =
    (col: String, df: DataFrame) => {
      val caseStatement =
        "case " +
          (for ((regex, value) <- regexValueMap)
            yield "when length(regexp_extract(" + column + ",\'" + regex + "\',0))>0 then \'" + value + "\' ").mkString("") +
          " else " + (if (default == null) "null" else "'" + default + "'") + " end"

      df.withColumn(col, expr(caseStatement))
    }

  // TODO - placeholder for functions to be coded
  def regexpSubstr(field: String, regexp: String, cols: Int): ProvenanceFunction =
    (col: String, df: DataFrame) => {
      val pattern = regexp.r
      val matches = pattern.findAllIn(df(field).toString()).toList
      if (matches.length < cols)
        df.withColumn(col, nullColumn)
      else df.withColumn(col, lit(matches(cols - 1)))
    }

  // TODO - placeholder for functions to be coded
  //def regexpSubstr(field: String, regexp: String, cols: String): ProvenanceFunction =
  //(col: String, df: DataFrame) => df.withColumn(col, nullColumn)

  // TODO - placeholder for functions to be coded
  def pvtColumn(columns: Seq[String], prefix: String = "", nullIf: Seq[String] = Seq(), nullIfNot: Seq[String] = Seq()): ProvenanceFunction =
    (col: String, df: DataFrame) => df.withColumn(col, nullColumn)

  def unpivot(cols: Seq[String], types: Seq[String] = Seq(), typeColumnName: String = "UNPIVOT_COLUMN", includeNulls: Boolean = false): ProvenanceFunction =
    (col: String, df: DataFrame) => {
      val ccCol = "UNPIVOT_CONCAT"
      val exCol = "UNPIVOT_EXPLODE"
      if (types.length == cols.length) {
        val df1 = df.withColumn(ccCol,
          concat_ws(";", (types zip cols).map { c => concat_ws("~", lit(c._1), df(c._2)) }: _*))
          .explode(ccCol, exCol) { s: String => s.split(";") }
        val df2 = df1.withColumn(typeColumnName, split(df1(exCol), "~")(0))
          .withColumn(col, split(df1(exCol), "~")(1))
        df2.filter(!isnull(df2(col)))
          .drop(ccCol)
          .drop(exCol)
      } else {
        val df1 = df.withColumn(ccCol, concat_ws(";", cols.map { c => df(c) }: _*))
          .explode(ccCol, col) { s: String => s.split(";") }
        val filtered = if (includeNulls) df1 else df1.filter(!isnull(df1(col)))
        filtered.drop(ccCol)
      }
    }

  //TODO placeholder for map_predicate_values.
  //def map_predicate_values(groupid: String, dataSrc:String, entity:String, tableName: String, columnName:String, cdsid:String): Seq[String] = {
  //  Seq("0","1","2") //2 funcitons, 1 returns list, 1 returns column
  // }

  // Return a table containing matching map predicate values
  def mpv(mpvTable: DataFrame, groupID: String, cdsid: String, dataSrc: String, entity: String, tableName: String, columnName: String): DataFrame = {
    mpvTable.filter("GROUPID='" + groupID + "' AND DATA_SRC='" + dataSrc + "' AND ENTITY='" + entity + "' AND TABLE_NAME='" + tableName + "' AND COLUMN_NAME='" + columnName + "' AND CLIENT_DS_ID='" + cdsid + "'")
      .select("COLUMN_VALUE")
  }

  // Return a List of map predicate values
  def mpvList(mpvTable: DataFrame, groupID: String, cdsid: String, dataSrc: String, entity: String, tableName: String, columnName: String): List[String] = {
    val lis = mpv(mpvTable, groupID, cdsid, dataSrc, entity, tableName, columnName).rdd.map(r => r(0).toString).collect().toList.map(x => "'" + x + "'")
    if (lis.isEmpty) List("'NO_MPV_MATCHES'")
    else lis
  }


  def mpvList1(mpvTable: DataFrame, groupID: String, cdsid: String, dataSrc: String, entity: String, tableName: String, columnName: String): List[String] = {
    val lis = mpv(mpvTable, groupID, cdsid, dataSrc, entity, tableName, columnName).rdd.map(r => r(0).toString).collect().toList.map(x => "" + x + "")
    if (lis.isEmpty) List("'NO_MPV_MATCHES'")
    else lis
  }


  def mpvClause(mpvTable: DataFrame, groupID: String, cdsid: String, dataSrc: String, entity: String, tableName: String, columnName: String): String =
    "" + mpvList(mpvTable, groupID, cdsid, dataSrc, entity, tableName, columnName).mkString(",") + ""

  def lookup(table: DataFrame, filterExpr: String, valueColumn: String): DataFrame = table.filter(filterExpr).select(valueColumn)

  def lookupList(table: DataFrame, filterExpr: String, valueColumn: String): List[String] =
    lookup(table, filterExpr, valueColumn).rdd.map(r => r(0).toString).collect().toList

  def lookupValue(table: DataFrame, filterExpr: String, valueColumn: String): String = {
    val lis = lookup(table, filterExpr, valueColumn).rdd.map(r => r(0).toString).collect().toList
    if (lis.length > 0) lis(0) else null
  }

  def labresults_extract_relativeindicator(): ProvenanceFunction =
    (col: String, df: DataFrame) => {
      df.withColumn(col,
        when(df("LOCALRESULT_25").rlike("(^|[^<>])[<>]($|[^<>])"), regexp_extract(df("LOCALRESULT_25"), "[<>=]{1}[=]{0,1}", 0))
          .when(lower(df("LOCALRESULT_25")).rlike("( less | under | lesser )"), lit("<"))
          .when(lower(df("LOCALRESULT_25")).rlike("( above | over | great | greater | \\+)"), lit(">")).otherwise(nullColumn))
    }

  def labresults_extract_resulttype(fieldcheck: String, coltopass: String): ProvenanceFunction =
    (col: String, df: DataFrame) => {
      var lowerCol = lower(df("LOCALRESULT_25"))
      df.withColumn(col, when(df("LOCALRESULT_NUMERIC").isNotNull, substring(df(coltopass), 1, 16)).otherwise(
        when(lowerCol.isNull, lit("CH000654"))
          .when(lowerCol.rlike("(posi| pos |^pos )"), lit("CH000651"))
          .when(lowerCol.rlike("neg"), lit("CH000652"))
          .when(lowerCol.rlike("moderat"), lit("CH000MOD"))
          .when(lowerCol.rlike("(wnl|^nl|^[ :;]?normal)"), lit("CH001203"))
          .otherwise(nullColumn)))
    }

  def labresults_extract_uom(): ProvenanceFunction =
    (col: String, df: DataFrame) => {
      df.withColumn(col,
        regexp_replace(regexp_extract(regexp_replace(lower(trim(df("LOCALRESULT_25"))), " per ", "/"),
          "(iu/l|iu/cc" +
            "|k/ul|kcals/kg|kcal/kg/day" +
            "|m/ul|mg/kg/mmol/l|mmol/l|mos/kg|mosm/l|mosm/kg" +
            "|u/kg/hr|bau|cal/kg|eu/dl|u/l|um3/cell" +
            "|cc/day|cc/kg|cc/hr" +
            "|cell[s]?/mcl|cell[s]?/ul|cfu/ml" +
            "|col/ml" +
            "|g/kg|g/dl/gm/kg|gm/dl" +
            "|iu/ml" +
            "|meq/dl|meq/kg|meq/l" +
            "|ml/kg|ml/hr|ml/min" +
            "|mcg/kg|mcg/kg/hr" +
            "|mil/mcl" +
            "|mg/cc|mg/dl|mg/g|mg/kg/day|mg/ml|mg/l|mg/wk" +
            "|mm/hr" +
            "|ng/ml|ng/dl|nmol/l" +
            "|pg/ml" +
            "|ug/mg|ug/ml|ug/dl|u/dl|uiu/ml|u/ml" +
            "|[x]?[ ]?10[-e*^][36]/ul" +
            "|g/dl" +
            "|#/mcl|#/ul" +
            "|mmol|ml|mg|mm|cc|cm|meg|mcg|iu| g$| g |%|ratio|percent|hpf|/hpf|pg|ph|fl|sec|hr|#)", 0), " ", ""))
    }

    
//START OF FUNCTIONS AND LISTS SUPPORTING LABRESULT EXTRACT_VALUE FUNCTION, SEE CDRSCALE-803 AND TERM-5329
import org.apache.spark.sql.functions.udf

//If a labresult includes one of these strings, exclude all together
val g_exclude_txt = Seq(
    " not performed ", "below linearity", "canceled", "cancelled", "cannot calc", "contaminated",
    "could not be calc", "disregard", "err code", "error", "incorrect", "invalid", "n/a", "not calc", "not done",
    "not performed", "not valid", "notified", "out of linearity", "reference interval", "tnp", "too high",
    "unable to calculate", "unable to report", "wrong patient", "chylomicrons")

//If a labresult includes one of these strings, truncate at the earliest character of any of the found strings
val g_trunc_txt = Seq(
    "call", "fax", "not sufficient", "performed at", "phone", "qns", "reference range", "trig >",
    "trig. >", "triglycerides >", "triglycerides are >", "triglycerides exceed", "triglyderides are >")

val months = Seq(
    "january", "february", "march", "april", "may", "june", "july", "august",
    "september", "october", "novermber", "december", "jan", "feb", "mar", "apr",
    "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec", "jan.", "feb.", "mar.", "apr.",
    "jun.", "jul.", "aug.", "sep.", "sept.", "oct.", "nov.", "dec.")

//Function to truncate labresults based on the strings in g_trunc_txt
def trunc_str(src:String): String =  {
    val str = Option(src).getOrElse(return null)
    var mindex = str.length
        for (i <- g_trunc_txt) {
             if (str.contains(i) & str.indexOf(i) < mindex) {
                mindex = str.indexOf(i)
                }
            }
    str.substring(0, mindex)
    }
val trunc_str_udf = udf[String, String](trunc_str)

//Monster string to pull out common units. Should eventually convert this to pull from a dictionary, but those have not yet been ported to hadoop
//so we will continue to use this historical solution from the old oracle sql function
val mtchstr = (
    "(iu/l|iu/cc" +
    "|k/ul| kcals/kg|kcal/kg/day" +
    "|m/ul|mg/kg/mmol/l|mmol/l|mos/kg|mosm/l|mosm/kg" +
    "|u/kg/hr|bau|cal/kg|eu/dl|u/l|um3/cell" +
    "|cc/day|cc/kg|cc/hr" +
    "|cell[s]?/mcl|cell[s]?/ul|cfu/ml" +
    "|col/ml" +
    "|g/kg|g/dl/gm/kg|gm/dl" +
    "|iu/ml" +
    "|meq/dl|meq/kg|meq/l" +
    "|ml/kg|ml/hr|ml/min" +
    "|mcg/kg|mcg/kg/hr" +
    "|mil/mcl" +
    "|mg/cc|mg/dl|mg/g|mg/kg/day|mg/ml|mg/l|mg/wk" +
    "|mm/hr" +
    "|ng/ml|ng/dl|nmol/l" +
    "|pg/ml" +
    "|ug/mg|ug/ml|ug/dl|u/dl|uiu/ml|u/ml" +
    "|[x]?[ ]?10[-e*^][36]/ul" +
    "|g/dl" +
    "|#/mcl|#/ul" +
    "|mmol|ml|mg|mm|cc|cm|meg|mcg|iu| g$| g |%|ratio|percent|hpf|/hpf|pg|ph|fl|sec|hr|mos|yrs|months|years)")

//Function to actually extract numeric results from tokens after all of the other pre-processing and filtering in extract_Value
def get_nbr(testc:String): String =  {
    val str = Option(testc).getOrElse(return null)
    //First regex is used to define the tokens as any continuous group of non-whitespace characters
    val r = "[^\\s]{1,}".r
    //Second regex to decide whether a token is a valid number or not
    val r2 = "^[\\-]?[.]?[0-9]{1,}$|^[\\-]?[0-9]{1,}\\.[0-9]*$".r
    val tokens = r.findAllIn(str)
    //Create a copy of the set of tokens, the iteration through the two sets will be staggered to support logic that
    //looks at what the previous or next token was/is
    val (tok1, tok2) = tokens.duplicate
    var t_count = 1
    var found = ""
    var token = ""
    var prev_token = ""
    var next_token = ""
    val month_string = "('" + months.mkString("','") + "')"
    if (tok2.hasNext) {
        next_token = tok2.next() }
    else {
        next_token = "" }
    //Look for numbers until we hit one, or go through 5 tokens, whichever comes first        
    while ((t_count <= 5) & (found == "")) {
        if (tok1.hasNext) {
            prev_token = token
            token = tok1.next()
            if (tok2.hasNext) {
                next_token = tok2.next() }
            else {
                next_token = "" }
            val nbr = r2.findFirstIn(token).getOrElse("") 
            val next_nbr = r2.findFirstIn(next_token).getOrElse("")
            //Check for a 1-4 length numeric token preceded by a month string, call those dates and exclude them
            if (token.length > 0 & token.length < 5 & months.contains(prev_token) & (nbr != "")) {
                found = "Date" }
            //Check for a   1-2 length numeric token that is follwed by either a month or a 4 digit numeric token (a year), call those dates and exclude them      
            else if (token.length > 0 & token.length < 3 & (nbr != "") & (months.contains(next_token) | (next_nbr.length == 4 & (next_nbr != "")))) {
                found = "Date" }
            //Exclude numbers that follow 'at' or 'day' as these indicate when something was done and not the actual result        
            else if ((token == "at" | token == "day") & (next_nbr != "")) {
                found = "Time" }
            else {
                found = nbr } 
                }
        else {
            found = "None" }
        t_count+=1 }
    if (found == "" | found == "Date" | found == "Time") {
        found = "None" }
    found }
val get_nbr_udf = udf[String, String](get_nbr)

//Function to format numeric results and get around the scientific notation return by casting to a float and then back to a string
def format_num(src:Column): Column =  {
    //Add leading 0 to decimal numbers
    val tmp1 = regexp_replace(src,"(?<=^[-]?)\\.(?=[0-9]{1,})", "0.")
    //Remove leading 0s
    val tmp2 = regexp_replace(tmp1, "(?<=^[-]?)[0]{1,}(?=[0-9])", "")
    //Remove trailing 0s after a decimal
    val tmp3 = regexp_replace(tmp2, "(?<=[0-9])\\.[0]*$", "")
    //Remove trailing 0s at the end of a decimal number
    val tmp4 = regexp_replace(tmp3, "(?<=\\.[0-9]{0,10}[1-9]{1})[0]{1,}$", "")
    //Don't let 0 be negative
    val tmp5 = regexp_replace(tmp4, "^\\-0$", "0")
    //If decimal number with length >25, truncate, otherwise if length >25 null (CDRSCALE-1176)
    when((instr(tmp5,".") =!= lit("0") && length(tmp5) > 25), substring(tmp5,1,25))
      .when(length(tmp5) > 25, null)
      .otherwise(tmp5)
    }
def extract_value(): ProvenanceFunction = (col: String, df: DataFrame) => {
    //Force results to lowercase, trim leading and trailing whitespace
    val v_txt = df.withColumn("v_txt", trim(lower(df("LOCALRESULT_25"))))
    //If there is a space after 25 chars, truncate the result there
    val v_txt1 = v_txt.withColumn("v_txt1", when(locate(" ", v_txt("v_txt"), 25) === 0, v_txt("v_txt")).otherwise(expr("substr(v_txt,1,locate(' ', v_txt, 25))"))) 
    val exclstr = "" + g_exclude_txt.mkString("|") + ""
    val truncstr = "" + g_trunc_txt.mkString("|") + ""
    //Exclude any results containing strings in the exclusion list, truncate any results containing strings in the truncate list
    var v_txt_fil = v_txt1.withColumn("v_txt_fil", when(expr("v_txt1 rlike '" + exclstr + "'"), "None").otherwise(when(expr("v_txt1 rlike '" + truncstr + "'"), trunc_str_udf(v_txt1("v_txt1"))).otherwise(v_txt1("v_txt1"))))
    //replace 'per' with '/' to make it easier to match units
    val v_txt_per = v_txt_fil.withColumn("v_txt_per", regexp_replace(v_txt_fil("v_txt_fil"), " per ", "/"))
    //remove any 2 whitespace separated character sequences (words) that are not flanked by a number, i.e. ("This is a result 1" -> "result 1"), ("This 2 is a result 1" -> no change)
    val v_txt_words_removed = v_txt_per.withColumn("v_match", regexp_replace(v_txt_per("v_txt_per"), "(?<![0-9]\\s{0,10})(?<=[\\s]{1,}|^)[^0-9\\s]{1,}(?=[\\s]{1,}|$)(?!\\s{0,10}[0-9])", " "))
    //remove units prior to looking for thigns like ranges
    val v_txt_no_units = v_txt_words_removed.withColumn("v_match_fil", regexp_replace(v_txt_words_removed("v_match"), mtchstr, ""))
    //remove certain punctuation and a few special cases including: numbers prefixed by just one + or with a trailing +, any = signs or numbers preceding an = sign, parentheses, @,*,;,| mutliple + in a row
    //for rationale, see examples on TERM-5329. this is likely the place where new targeted cases should be added 
    val v_no_punct = v_txt_no_units.withColumn("no_punct", regexp_replace(v_txt_no_units("v_match_fil"),"((?<=([^+]{1,}|^)[+]{1})[0-9.]{1,})|([0-9.]{1,}(?=[+]{1,}))|=|([0-9.]{1,}[=]{1,})|[\\(\\)]|@|\\||\\*|([+]{2,})|;|(^[0-9]\\.(?=[^\\d]{1,}))"," "))
    //Strip out commas following a number, or commas w/in valid numbers i.e. x,xxx but not x,xxxx
    val v_no_comma =  v_no_punct.withColumn("no_comma", regexp_replace(v_no_punct("no_punct"), "((?<=([^0-9]{1}|^)[0-9]{1,3}),(?=([0-9]{3})))|,(?=([^0-9]{1,}|$))", ""))
    //remove dates or phone numbers formatted as xx-xx-xxxx or xxx-xxx-xxxx
    val v_no_date = v_no_comma.withColumn("v_no_date", regexp_replace(v_no_comma("no_comma"), "(?<=[\\s]|^)[0-9]{1,3}[\\-\\ ]{1}[0-9]{1,3}[\\-\\ ]{1}[0-9]{2,4}(?=[\\s]|$)", ""))
    //remove ranges separated by '-' or 'at'
    val v_no_range = v_no_date.withColumn("no_range", regexp_replace(v_no_date("v_no_date"),"((?<=[\\s]|[+\\-]|^)[0-9.,]?[0-9.]{1,}[\\s]{1,}(-|to)[\\s]{1,}[+\\-]?[0-9.]{1,})|((?<=[\\s]|[+\\-]|^)[0-9.,]?[0-9.]{1,}(-|to)[+\\-]?[0-9.]{1,})"," "))
    //remove relative indicators
    val v_no_rel = v_no_range.withColumn("no_rel", regexp_replace(v_no_range("no_range"),"<|>",""))
    //now parse into tokens, and loop through these looking for numbers
    val v_out = v_no_rel.withColumn("nbr",  when(v_no_rel("no_rel").isNotNull, get_nbr_udf(v_no_rel("no_rel"))).otherwise(null))
    //see if we found anything and if so, clean up the formating before returning
    v_out.withColumn(col, when(v_out("LOCALRESULT_NUMERIC").isNotNull, substring(v_out("LOCALRESULT_NUMERIC"),1,25)).otherwise(when(v_out("nbr") === "None", null).otherwise(format_num(v_out("nbr")))))
}
//END OF FUNCTIONS AND LISTS SUPPORTING LABRESULT EXTRACT_VALUE FUNCTION, SEE CDRSCALE-803 AND TERM-5329

  def apply_unit_conv_function(v_numeric: Column, p_function_applied: Column) = {
    val value = when(isnull(v_numeric) || (v_numeric === 0), v_numeric)
      .otherwise(
        when(p_function_applied === "LINEAR", v_numeric)
          .when(p_function_applied === "LOG10", log(10, v_numeric))
          .when(p_function_applied === "LOG2", log(2, v_numeric))
          .when(p_function_applied === "TEMPTOC", (v_numeric - 32) * 5 / 9)
          .when(p_function_applied === "TEMP_60_LOGIC",
            when(v_numeric < 60, v_numeric)
              .otherwise((v_numeric - 32) * 5 / 9)
          )
          .otherwise(v_numeric))
    round(value, 2)
  }

  def convert_multiple_units(p_obstype: Column, p_localresult: Column, p_local_unit_cui: Column, p_std_unit_cui: Column
                             , p_dest_unit: Column, p_conv_fact1: Column, p_conv_fact2: Column, p_conv_fact3: Column, p_conv_fact4: Column
                             , p_conv_fact5: Column) = {
    var v_localresult = upper(substring(p_localresult, 1, 100))
    var v_conv_fact1 = when(p_dest_unit === p_std_unit_cui, p_conv_fact1)
    var v_conv_fact2 = when(p_dest_unit === p_std_unit_cui, p_conv_fact2)
    var v_conv_fact3 = when(p_dest_unit === p_std_unit_cui, p_conv_fact3)
    var v_conv_fact4 = when(p_dest_unit === p_std_unit_cui, p_conv_fact4)
    var v_conv_fact5 = when(p_dest_unit === p_std_unit_cui, p_conv_fact5)
    convert_height_units(p_obstype, p_localresult, p_local_unit_cui, p_std_unit_cui, v_conv_fact1, v_conv_fact2)
    convert_weight_units(p_obstype, p_localresult, p_local_unit_cui, p_std_unit_cui, v_conv_fact3, v_conv_fact4)
    convert_weeks_units(p_obstype, p_localresult, p_local_unit_cui, p_std_unit_cui, v_conv_fact5)
  }

  def convert_height_units(p_obstype: Column, p_localresult: Column, p_local_unit_cui: Column, p_std_unit_cui: Column
                           , v_conv_fact1: Column, v_conv_fact2: Column) = {
    var v_localresult = upper(substring(p_localresult, 1, 100))
    var ht_in_cm: Column = null
    var ht_ft_cm: Column = null
    var v_ht_ft: Column = null
    var ht_ft: Column = null
    var v_ht_in: Column = null
    var ht_in: Column = null
    when((p_obstype === lit("HT")) && (p_local_unit_cui === lit("CH002722")),
      {
        v_ht_ft = regexp_extract(v_localresult, "^[^F''f]*", 0)
        ht_ft_cm = when(v_ht_ft === v_localresult, ht_ft_cm)
          .otherwise(round(when(v_ht_ft.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), v_ht_ft).multiply(v_conv_fact1), 5))
        v_ht_in = ltrim(rtrim(regexp_extract(regexp_extract(v_localresult, "^[^I\"''''i]*", 0), "[^Tt]*$", 0)))
        v_ht_in = when(v_ht_in !== v_localresult, substring(v_ht_in, 1, 2))
        ht_in_cm = when(v_ht_in.isNotNull, round(when(v_ht_in.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), v_ht_in).multiply(v_conv_fact2), 5))
        ht_ft_cm + ht_in_cm
      })
  }

  def convert_weight_units(p_obstype: Column, p_localresult: Column, p_local_unit_cui: Column, p_std_unit_cui: Column
                           , v_conv_fact3: Column, v_conv_fact4: Column) = {
    var v_localresult = upper(substring(p_localresult, 1, 100))
    var wt_lbs_kg: Column = null
    var wt_oz_kg: Column = null
    when((p_obstype === lit("WT")) && (p_local_unit_cui === lit("CH002723")), {
      v_localresult = regexp_replace(regexp_replace(v_localresult, "_", ""), ",", "")
      v_localresult = when(locate("LBS. ", v_localresult) > 0, regexp_replace(v_localresult, "LBS.", "LBS"))
      var v_wt_lbs = regexp_extract(v_localresult, "^[^L''l#Pp]*", 0)
      wt_lbs_kg = when(v_wt_lbs.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), round(v_wt_lbs.multiply(v_conv_fact3), 5))
      var v_wt_oz = ltrim(rtrim(regexp_extract(regexp_extract(v_localresult, "^[^O\"''''o]*", 0), "[^Ss#bBdD]*$", 0)))
      v_wt_oz = when(v_wt_oz !== v_localresult, substring(v_wt_oz, 1, 2))
      wt_oz_kg = when((v_wt_oz !== v_localresult) && v_wt_oz.isNotNull, round(when(v_wt_oz.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), v_wt_oz).multiply(v_conv_fact4), 5))
      wt_lbs_kg + wt_oz_kg
    })
  }

  def convert_weeks_units(p_obstype: Column, p_localresult: Column, p_local_unit_cui: Column, p_std_unit_cui: Column
                          , v_conv_fact5: Column) = {
    var v_localresult = upper(substring(p_localresult, 1, 100))
    var ga_week_num: Column = null
    var ga_days_num: Column = null
    var ga_days_var: Column = null
    var ga_spl_chr_format: Column = null
    var ga_days_num_week: Column = null
    var ga_week_var: Column = null
    when(p_obstype.isin("CH002752", "CH002753", "CH002754") && (p_local_unit_cui === "CH002723"),
      {
        v_localresult = regexp_replace(regexp_replace(v_localresult, " ", ""), "S", "")
        ga_week_var = regexp_extract(v_localresult, "^[^W''w]*", 0)
        ga_spl_chr_format = when((locate("/", v_localresult) > 0) && (locate("7", regexp_extract(v_localresult, "[^/]*$", 0)) > 0), true).when(locate("\\+", ga_week_var) > 0, true)
        ga_days_var = when((locate("/", v_localresult) > 0) && (locate("7", regexp_extract(v_localresult, "[^/]*$", 0)) > 0), substring(ga_week_var, 3, 1))
          .when(locate("\\+", ga_week_var) > 0, ltrim(rtrim(substring(substring_index(ga_week_var, "\\+", -1), 1, 1))))
        ga_week_var = when((locate("\\/", v_localresult) > 0) && (locate("7", regexp_extract(v_localresult, "[^/]*$", 0)) > 0), substring(ga_week_var, 3, 1))
          .when(locate("\\+", ga_week_var) > 0, ltrim(rtrim(substring_index(ga_week_var, "\\+", 1))))
        ga_week_num = when(!ga_week_var.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), null).otherwise(ga_week_var)
        ga_days_var = when(isnull(ga_spl_chr_format) || (ga_spl_chr_format !== true), ltrim(rtrim(regexp_extract(regexp_extract(ga_days_var, "[^WwKk]*$", 0), "^[^D''d]*", 0))))
        ga_days_var = when(ga_days_var === v_localresult, null)
        ga_days_num_week = when(ga_days_var.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), round(ga_days_var.multiply(v_conv_fact5), 5)).otherwise(lit("0"))
        ga_week_num + ga_days_num_week
      })
  }

  def std_obsresult(p_obstype: Column, p_obsconvfactor: Column, p_obsregex: Column, p_datatype: Column, p_begin_range: Column, p_end_range: Column, p_round_prec: Column,
                    p_local_unit: Column, p_obstype_std_unit: Column, p_localresult: Column, p_local_unit_cui: Column, p_conv_factor: Column, p_function_applied: Column, p_obs_subs: Column) = {
    var v_obs_result: Column = null
    var v_conversion_factor: Column = null
    var v_low_rng: Column = null
    var v_hi_rng: Column = null
    v_obs_result = substring(p_localresult, 1, 250)
    v_obs_result = when(p_obsregex.isNotNull, p_obs_subs)
    v_obs_result = when((lower(p_datatype) === lit("n")) || (lower(p_datatype) === lit("f")),
      {
        v_obs_result = when(p_local_unit_cui !== lit("CH002773"), regexp_replace(regexp_replace(v_obs_result, "\\+", ""), "\\/", ""))
        var v_dash = locate("-", v_obs_result)
        v_low_rng = when((v_dash > 1) && (v_dash < length(v_obs_result)) && p_obstype.isin("CH001846", "CH001999", "PAIN"),
          when(substring(expr("substr(v_obs_result, 1, v_dash - 1"), 1, 8).rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), substring(expr("substr(v_obs_result, 1, v_dash - 1"), 1, 8)))
        v_hi_rng = when((v_dash > 1) && (v_dash < length(v_obs_result)) && p_obstype.isin("CH001846", "CH001999", "PAIN"),
          {
            v_hi_rng = substring(expr("substr(v_obs_result, v_dash + 1"), 1, 8)
            when(v_hi_rng < v_low_rng, concat_ws("", expr("substr(v_low_rng, 1, (length(v_low_rng) - length(v_hi_rng)))"), v_hi_rng))
          })
        var v_numeric = coalesce(v_hi_rng, v_low_rng, when(v_obs_result.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), v_obs_result))
        v_numeric = when((p_local_unit_cui !== "CH002722") && (p_local_unit_cui !== "CH002723") && (p_local_unit_cui !== "CH002773"),
          {
            v_numeric = when(when(isnull(p_obstype_std_unit), lit("0").otherwise(p_obstype_std_unit)) !== when(isnull(p_local_unit_cui), lit("0").otherwise(p_local_unit_cui)),
              {
                v_conversion_factor = coalesce(p_conv_factor, p_obsconvfactor)
                v_numeric = v_numeric * v_conversion_factor
                v_numeric = apply_unit_conv_function(v_numeric, when(isnull(p_function_applied), "LINEAR").otherwise(p_function_applied))
              })
          })
        v_numeric = when(p_obstype === lit("WT") && !v_numeric.between(when(isnull(p_begin_range), lit("1")).otherwise(p_begin_range), when(isnull(p_end_range), lit("300")).otherwise(p_end_range)), null)
          .when(p_obstype === lit("HT") && !v_numeric.between(when(isnull(p_begin_range), lit("1")).otherwise(p_begin_range), when(isnull(p_end_range), lit("300")).otherwise(p_end_range)), null)
          .when(p_begin_range.isNotNull && p_end_range.isNotNull && !v_numeric.between(p_begin_range, p_end_range), null)
          .otherwise(v_numeric)
        v_numeric = expr("round(v_numeric, when(isnull(p_round_prec), 2).otherwise(p_round_prec))")
        v_numeric
      }).when(lower(p_datatype) === "p",
      {
        v_obs_result = when(when(v_obs_result.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), v_obs_result) leq (lit("1")), v_obs_result.multiply(100))
        v_obs_result
      }).otherwise(v_obs_result)
  }

  def std_obsresult1(p_obstype: Column, p_obsconvfactor: Column, p_obsregex: Column, p_datatype: Column, p_begin_range: Column, p_end_range: Column, p_round_prec: Column,
                     p_local_unit: Column, p_obstype_std_unit: Column, p_localresult: Column, p_local_unit_cui: Column, p_conv_factor: Column, p_function_applied: Column, p_obs_subs: Column) = {
    var v_obs_result: Column = null
    var v_conversion_factor: Column = null
    var v_low_rng: Column = null
    var v_hi_rng: Column = null
    v_obs_result = substring(p_localresult, 1, 250)
    v_obs_result = when(p_obsregex.isNotNull, p_obs_subs)
    v_obs_result = when((lower(p_datatype) === lit("n")) || (lower(p_datatype) === lit("f")), {
      v_obs_result = when(p_local_unit_cui !== lit("CH002773"), regexp_replace(regexp_replace(v_obs_result, "\\+", ""), "\\/", "")).otherwise(v_obs_result)
      v_low_rng = when(p_obstype.isin("CH001846", "CH001999", "PAIN"),
        when(substring(substring_index(v_obs_result, "-", 1), 1, 8).rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), substring(substring_index(v_obs_result, "-", 1), 1, 8)))
      v_hi_rng = when(p_obstype.isin("CH001846", "CH001999", "PAIN"),
        {
          v_hi_rng = substring(reverse(substring_index(reverse(v_obs_result), "-", 1)), 1, 8)
          // when(v_hi_rng < v_low_rng, concat_ws("", expr("substr(" + v_low_rng + ", 1, (length(" + v_low_rng + ") - length(" + v_hi_rng + ")))"),  v_hi_rng ))
          v_hi_rng
        })
      /*

      var v_numeric = coalesce(v_hi_rng, v_low_rng, when(v_obs_result.rlike("[+-]?(\\.\\d+|\\d+\\.?\\d*)$"), v_obs_result))
      v_numeric = when((p_local_unit_cui !== "CH002722") && (p_local_unit_cui !== "CH002723") && (p_local_unit_cui !== "CH002773"),
        {
          v_numeric =  when(p_obstype_std_unit !== p_local_unit_cui,
        //  v_numeric = when(when(isnull(p_obstype_std_unit), lit("0")).otherwise(p_obstype_std_unit) !== when(isnull(p_local_unit_cui), lit("0")).otherwise(p_local_unit_cui),
            {
              v_conversion_factor = coalesce(p_conv_factor, p_obsconvfactor)
              v_numeric = v_numeric * v_conversion_factor
              apply_unit_conv_function(v_numeric, when(isnull(p_function_applied), "LINEAR").otherwise(p_function_applied))
            })
          v_numeric
        })
*/
      v_hi_rng
    })
    v_obs_result
  }

  def std_obsresult2(): ProvenanceFunction = {
    (col: String, df: DataFrame) => {
      val df1 = df.withColumn("v_obs_result", substring(df("LOCALRESULT"), 1, 250))
      val df2 = df1.withColumn("v_obs_result1", when(df1("OBSREGEX").isNotNull, expr("regexp_extract(v_obs_result, OBSREGEX, 0)")))
      val df3 = df2.withColumn("v_obs_result2", when(lower(df2("DATATYPE")).isin("n", "f"),
        when(df2("LOCAL_UNIT_CUI") !== lit("CH002773"), regexp_replace(regexp_replace(df2("v_obs_result1"), "\\+", ""), "\\/", ""))))
      df3.withColumn(col, df2("v_obs_result2"))
    }
  }

  def safe_to_date(df: DataFrame, date_column: String, out_column: String, p_format: String, p_length: Int = 99) = {
    val df1 = df.withColumn("v_dt", df(date_column))
      .withColumn("v_len", lit(p_length))
    val df2 = df1.withColumn("v_len1", when(df1("v_len") === lit(99), length(regexp_replace(upper(lit(p_format)), "HH24", "HH")))
      .otherwise(df1("v_len")))
    val df3 = df2.withColumn("v_dt1",
      when(df2("v_len1") === lit("0"), df2("v_dt"))
        .when(length(df2("v_dt")).lt(df2("v_len1")), expr("rpad(v_dt, v_len1, '0')"))
        .when(length(df2("v_dt")).gt(df2("v_len1")), expr("substr(v_dt, 0, length(v_len1))"))
        .otherwise(df2("v_dt")))

    df3.withColumn(out_column, to_date(from_unixtime(unix_timestamp(df3("v_dt1"), p_format))).cast("timestamp"))
      .drop("v_dt", "v_len", "v_dt1", "v_len1")

  }

  def datelengthcheck(date_column: Column, p_format: Column, p_length: Column = lit(null)) = {
    val v_len: Column = p_length
    val v_len1 = length(date_column)
    val v_len2 = when(isnull(v_len), length(regexp_replace(upper(p_format), "HH24", "HH"))).otherwise(v_len)
    v_len2
  }

  def coalesceDate(df: DataFrame, date_column: String, out_column: String) = {
    val df1 = df.withColumn(out_column,
      when(expr("instr(" + date_column + ", ' ') > 0"), expr("substring(" + date_column + ", 1, instr(" + date_column + ", ' ') - 1)"))
        .otherwise(df(date_column)))

    val date1 = safe_to_date(df1, out_column, "date1", "yyyy/MM/dd", 0)
    val date2 = safe_to_date(date1, out_column, "date2", "MM/dd/yy", 0)
    val date3 = safe_to_date(date2, out_column, "date3", "dd/MMM/yy", 0)

    date3.withColumn(out_column,
        when(date3(out_column).rlike("^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}"), date3("date1"))
        .when(date3(out_column).rlike("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}"), date3("date2"))
        .when(date3(out_column).rlike("^[0-9]{1,2}/[a-zA-Z]+/[0-9]{2,4}"), date3("date3"))
        .otherwise(null)
      )
      .drop("date1", "date2", "date3")
  }

  def substringCols(df: DataFrame, cols: Seq[String], pos: Int = 19): DataFrame = {
    cols.foldLeft(df)((df, c) => df.withColumn(c, substring(df(c), 1, pos)))
  }


  def safe_to_number(input_column: Column) = {
    if (input_column.cast("Integer") == null)
      nullColumn
    else
      input_column.cast("Integer")
  }


  def safe_to_decimal(input_column: Column, precision: String, scale: String) = {
    if (input_column.cast("Decimal") == null)
      nullColumn
    else
      input_column.cast("Decimal("+precision+","+scale+")")
  }

  def client_exceptions(current_cds_name: String, cds_names: String*)  : Boolean = {

    for (cds_name <- cds_names) {
      if (cds_name.equals(current_cds_name)){
        return true
      }
    }
    false;
  }


}

