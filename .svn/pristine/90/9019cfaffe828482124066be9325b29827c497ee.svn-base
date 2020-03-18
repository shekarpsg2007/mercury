package com.humedica.mercury.toolkit.impl

import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import com.humedica.mercury.etl.core.engine.ShowDf._

object Diff {

  val log: Logger = LoggerFactory.getLogger(getClass)

  def compare(df1: DataFrame, df2: DataFrame, filter: String = "", sort: Seq[String] = Seq(),
              include: Seq[String] = Seq(), exclude: Seq[String] = Seq(), nrows: Int = 20,
              truncate: Boolean = true): (DataFrame, DataFrame) = {
    log.info("Dataframe comparison\n")
    val fil1 = if (filter == "") df1 else df1.filter(filter)
    val ord1 = if (sort.isEmpty) fil1 else fil1.orderBy(sort.head, sort.tail: _*)
    var sel1 = if (include.isEmpty) ord1 else ord1.select(include.head, include.tail: _*)
    for (col <- exclude)
      sel1 = if (sel1.columns.contains(col)) sel1.drop(col) else sel1
    log.info(show(sel1, nrows, truncate))

    val fil2 = if (filter == "") df2 else df2.filter(filter)
    val ord2 = if (sort.isEmpty) fil2 else fil2.orderBy(sort.head, sort.tail: _*)
    var sel2 = if (include.isEmpty) ord2 else ord2.select(include.head, include.tail: _*)
    for (col <- exclude)
      sel2 = if (sel2.columns.contains(col)) sel2.drop(col) else sel2
    log.info(show(sel2, nrows, truncate))
    (sel1, sel2)
  }

  def diff(df1: DataFrame, df2: DataFrame, filter: String = "", sort: Seq[String] = Seq(),
           include: Seq[String] = Seq(), exclude: Seq[String] = Seq(), nrows: Int = 20,
           truncate: Boolean = true): (DataFrame, DataFrame) = {
    val fil1 = if (filter == "") df1 else df1.filter(filter)
    val ord1 = if (sort.isEmpty) fil1 else fil1.orderBy(sort.head, sort.tail: _*)
    var sel1 = if (include.isEmpty) ord1 else ord1.select(include.head, include.tail: _*)
    for (col <- exclude)
      sel1 = if (sel1.columns.contains(col.toUpperCase)) sel1.drop(col) else sel1

    val fil2 = if (filter == "") df2 else df2.filter(filter)
    val ord2 = if (sort.isEmpty) fil2 else fil2.orderBy(sort.head, sort.tail: _*)
    var sel2 = if (include.isEmpty) ord2 else ord2.select(include.head, include.tail: _*)
    for (col <- exclude)
      sel2 = if (sel2.columns.contains(col.toUpperCase)) sel2.drop(col) else sel2

    val ex12 = (sel1 except sel2).distinct
    val ex21 = (sel2 except sel1).distinct

    // ex12.cache
    // ex21.cache
    log.info("Dataframe diff\n")
    log.info("Rows in first but not in second: " + ex12.count)
    log.info(show(ex12, nrows, truncate))
    log.info("Rows in second but not in first: " + ex21.count)
    log.info(show(ex21, nrows, truncate))
    (ex12, ex21)
  }

  def autodiff(df1: DataFrame, df2: DataFrame, filter: String = "",
               sort: Seq[String] = Seq(), silent: Boolean = true,
               nrows: Int = 20, truncate: Boolean = true): (DataFrame, DataFrame) = {

    val fil1 = if (filter == "") df1 else df1.filter(filter)
    val ord1 = if (sort.isEmpty) fil1 else fil1.orderBy(sort.head, sort.tail: _*)
    var sel1 = ord1

    val fil2 = if (filter == "") df2 else df2.filter(filter)
    val ord2 = if (sort.isEmpty) fil2 else fil2.orderBy(sort.head, sort.tail: _*)
    var sel2 = ord2

    var cols1 = df1.columns
    var cols2 = df2.columns

    for (col <- cols1) if (!sel2.columns.contains(col.toUpperCase)) sel1 = sel1.drop(col)
    for (col <- cols2) if (!sel1.columns.contains(col.toUpperCase)) sel2 = sel2.drop(col)

    val ex12 = (sel1 except sel2).distinct
    val ex21 = (sel2 except sel1).distinct

    if (!silent) {
      log.info("Auto-diff report\n")
      log.info("Rows in first but not in second: " + ex12.count)
      log.info(show(ex12, nrows, truncate))
      log.info("Rows in second but not in first: " + ex21.count)
      log.info(show(ex21, nrows, truncate))
    }

    (ex12, ex21)
  }
}
