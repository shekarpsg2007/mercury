package com.humedica.mercury.etl.core.engine

import org.apache.spark.sql.DataFrame
import com.humedica.mercury.etl.core.engine.Types._
import com.humedica.mercury.etl.core.engine.Constants._
import com.humedica.mercury.etl.core.engine.Functions._

/**
  * Created by jrachlin on 12/5/16.
  */
abstract class EntitySource(config: Map[String, String]) {

  val cfg = config
  val clientDataSourceName = config(CLIENT_DS_NAME)

  /** Elements of the specification */
  var tables: List[String] = List()
  var columnSelect: Map[String, List[String]] = Map()

  var beforeJoin: Map[String, DataFunction] = Map()
  var join: JoinFunction = (dfs: Map[String, DataFrame]) => dfs.head._2 // Default: Get first df in list (no join)
  var afterJoin: DataFunction = (df: DataFrame) => df //nop

  var map: Map[String, ProvenanceFunction] = Map()
  var afterMap: DataFunction = (df: DataFrame) => df //nop

  // cds = CLIENT DATA SOURCE NAME
  var beforeJoinExceptions: Map[String, Map[String, DataFunction]] = Map() // (cds -> (table -> Df))
  var joinExceptions: Map[String, JoinFunction] = Map() // cds -> Jf
  var afterJoinExceptions: Map[String, DataFunction] = Map() // cds -> Df
  var mapExceptions: Map[(String, String), ProvenanceFunction] = Map() // (cds, column) -> Pf
  var afterMapExceptions: Map[String, DataFunction] = Map() // cds -> Df

  /**
    * Table storage (name -> dataframe) for tables that are read or
    * built from specs or function calls or specified paths
    */
  var table: Map[String,DataFrame] = Map()

  /**
    * Column names (if not specified, the engine will try to look up
    * in engine.Schema class) If neither are provided, the entire dataframe produced
    * by the spec is saved and no mapping operations will occur!
    */
  var columns: List[String] = List()

  /**
    * Whether the entity source should be cached on startup.  The key will be the
    * source name derived from class name.
    */
  var cacheMe: Boolean = false

  def getTable(name: String): DataFrame = {
    val cached = Engine.cache.get(name)
    if (cached != null) cached else table.getOrElse(name, null)
  }

  // Integrate exceptions

  def initialize(): Unit = {

    // before join
    for (cds <- beforeJoinExceptions.keys)
      if (cds == clientDataSourceName)
        beforeJoin = beforeJoinExceptions(cds)

    // join
    for (cds <- joinExceptions.keys)
      if (cds == clientDataSourceName)
        join = joinExceptions(cds)

    // after join
    for (cds <- afterJoinExceptions.keys)
      if (cds == clientDataSourceName)
        afterJoin = afterJoinExceptions(cds)

    // map
    for ((cds, col) <- mapExceptions.keys)
      if (cds == clientDataSourceName)
        map += (col -> mapExceptions(cds, col))

    // Add default columns to map
    if (config(CLIENT_DS_ID) == "-1")
      map += (
        "GROUPID" -> literal(config(GROUP)),
        "CLIENT_DS_ID" -> mapFrom("CLIENT_DS_ID")
        )
    else if(config(CLIENT_DS_ID) != "-1")
      map += (
        "GROUPID" -> literal(config(GROUP)),
        "CLIENT_DS_ID" -> literal(config(CLIENT_DS_ID))
        )

    // Convert all mapping keys to uppercase
    map.foreach { case(key,value) =>
      map -= key
      map += key.toUpperCase -> value
    }

    // aftermap
    for (cds <- afterMapExceptions.keys)
      if (cds == clientDataSourceName)
        afterMap = afterMapExceptions(cds)
  }
}
